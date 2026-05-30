package evolution

import (
	"fmt"
	"sort"
	"strings"

	"github.com/stablekernel/crucible/state"
)

// ChangeKind names the category of a single structural difference between two
// machine definitions.
type ChangeKind string

// The kinds of change the differ recognizes. Each maps to a fixed
// breaking/additive classification (see the Evolution Guide), except
// KindUnknown, which is always treated as breaking and flagged.
const (
	// Additive (backward-compatible) kinds.
	KindStateAdded      ChangeKind = "state_added"
	KindTransitionAdded ChangeKind = "transition_added"
	KindGuardAdded      ChangeKind = "guard_added"
	KindGuardRemoved    ChangeKind = "guard_removed"
	KindEffectAdded     ChangeKind = "effect_added"
	KindEffectRemoved   ChangeKind = "effect_removed"
	KindMetadataChanged ChangeKind = "metadata_changed"
	KindWaitModeChanged ChangeKind = "waitmode_changed"

	// Breaking kinds.
	KindStateRemoved         ChangeKind = "state_removed"
	KindTransitionRemoved    ChangeKind = "transition_removed"
	KindTransitionRetargeted ChangeKind = "transition_retargeted"
	KindInitialChanged       ChangeKind = "initial_changed"
	KindMachineRenamed       ChangeKind = "machine_renamed"
	KindFinalChanged         ChangeKind = "final_changed"

	// KindUnknown marks a delta the differ has no explicit rule for. It is always
	// breaking and is flagged for human review, per the Evolution Guide's
	// "unknown -> breaking" default.
	KindUnknown ChangeKind = "unknown"
)

// Change is a single classified difference between two machine definitions.
type Change struct {
	Kind ChangeKind
	// Path locates the change in the machine graph (e.g. a state name, or
	// "state/On" for a transition, with a dotted prefix for nested states).
	Path string
	// Description is a human-readable explanation. For flagged cases it is
	// prefixed with "[FLAGGED: ...]".
	Description string
	Breaking    bool
}

// Report is the full set of classified changes between two machine definitions.
// The zero Report (no changes) means the definitions are equivalent.
type Report struct {
	Changes []Change
}

// Bump is a semantic-version increment recommendation.
type Bump string

// Semantic-version bump recommendations.
const (
	// Patch: no schema changes (only changes the differ never surfaces, e.g.
	// source positions, which are stripped before diffing).
	Patch Bump = "patch"
	// Minor: additive, backward-compatible changes only.
	Minor Bump = "minor"
	// Major: at least one breaking change.
	Major Bump = "major"
)

// Empty reports whether the two definitions were equivalent.
func (r Report) Empty() bool { return len(r.Changes) == 0 }

// Breaking reports whether any change is breaking. A breaking change requires a
// major version bump and the full deprecation lifecycle from the Evolution
// Guide before the old definition can be removed.
func (r Report) Breaking() bool {
	for _, c := range r.Changes {
		if c.Breaking {
			return true
		}
	}
	return false
}

// SemverBump maps the report onto a recommended version bump: Major if any
// change is breaking, Minor if there are additive changes only, Patch if the
// definitions are equivalent.
func (r Report) SemverBump() Bump {
	switch {
	case r.Breaking():
		return Major
	case len(r.Changes) > 0:
		return Minor
	default:
		return Patch
	}
}

// String renders the report as one line per change, breaking changes first.
func (r Report) String() string {
	if r.Empty() {
		return "no changes"
	}
	var b strings.Builder
	for _, c := range r.Changes {
		sev := "additive"
		if c.Breaking {
			sev = "BREAKING"
		}
		fmt.Fprintf(&b, "%-8s %-22s %s: %s\n", sev, c.Kind, c.Path, c.Description)
	}
	return strings.TrimRight(b.String(), "\n")
}

// DiffMachines classifies the difference between two Quenched machines. Both are
// serialized to their position-independent IR (source positions are stripped, so
// file/line churn never registers as a change) and then diffed.
func DiffMachines[S comparable, E comparable, C any](old, updated *state.Machine[S, E, C]) (Report, error) {
	oldBytes, err := old.ToJSON(state.WithoutSrcPos())
	if err != nil {
		return Report{}, &SerializeError{Side: "old", Err: err}
	}
	newBytes, err := updated.ToJSON(state.WithoutSrcPos())
	if err != nil {
		return Report{}, &SerializeError{Side: "new", Err: err}
	}
	return DiffJSON[S, E, C](oldBytes, newBytes)
}

// DiffJSON classifies the difference between two serialized machine IRs. This is
// the form a CI gate uses: diff a committed golden machine.json against the
// current machine's serialized IR.
func DiffJSON[S comparable, E comparable, C any](old, updated []byte) (Report, error) {
	oldIR, err := state.LoadFromJSON[S, E, C](old)
	if err != nil {
		return Report{}, &DecodeError{Side: "old", Err: err}
	}
	newIR, err := state.LoadFromJSON[S, E, C](updated)
	if err != nil {
		return Report{}, &DecodeError{Side: "new", Err: err}
	}
	return Diff(oldIR, newIR), nil
}

// Diff classifies the difference between two machine IRs as additive or
// breaking, following the Evolution Guide. The result is deterministic: changes
// are ordered breaking-first, then by path.
func Diff[S comparable, E comparable, C any](old, updated *state.IR[S, E, C]) Report {
	var r Report
	d := &differ[S, E, C]{r: &r}

	if str(old.Name) != str(updated.Name) {
		r.add(Change{
			Kind:        KindMachineRenamed,
			Path:        str(old.Name),
			Description: fmt.Sprintf("machine renamed %q -> %q; downstream identity breaks", str(old.Name), str(updated.Name)),
			Breaking:    true,
		})
	}

	oldInit := initialStr(old)
	newInit := initialStr(updated)
	if oldInit != newInit {
		r.add(Change{
			Kind:        KindInitialChanged,
			Path:        "<initial>",
			Description: fmt.Sprintf("initial state changed %s -> %s; new instances start elsewhere", oldInit, newInit),
			Breaking:    true,
		})
	}

	d.diffStates("", old.States, updated.States)

	sort.SliceStable(r.Changes, func(i, j int) bool {
		if r.Changes[i].Breaking != r.Changes[j].Breaking {
			return r.Changes[i].Breaking // breaking first
		}
		if r.Changes[i].Path != r.Changes[j].Path {
			return r.Changes[i].Path < r.Changes[j].Path
		}
		return r.Changes[i].Kind < r.Changes[j].Kind
	})
	return r
}

type differ[S comparable, E comparable, C any] struct {
	r *Report
}

func (r *Report) add(c Change) { r.Changes = append(r.Changes, c) }

// diffStates compares two sibling state slices under the given dotted path
// prefix, recursing into children and regions.
func (d *differ[S, E, C]) diffStates(prefix string, oldStates, newStates []state.State[S, E, C]) {
	oldByName := indexStates(oldStates)
	newByName := indexStates(newStates)

	for name, os := range oldByName {
		path := join(prefix, name)
		ns, ok := newByName[name]
		if !ok {
			d.r.add(Change{
				Kind:        KindStateRemoved,
				Path:        path,
				Description: fmt.Sprintf("state %q removed; entities persisted in it hit ErrInvalidTransition (a rename appears as remove+add)", name),
				Breaking:    true,
			})
			continue
		}
		d.diffState(path, os, ns)
	}

	for name := range newByName {
		if _, ok := oldByName[name]; !ok {
			d.r.add(Change{
				Kind:        KindStateAdded,
				Path:        join(prefix, name),
				Description: fmt.Sprintf("new state %q; no existing entity is in it", name),
				Breaking:    false,
			})
		}
	}
}

// diffState compares one state present in both definitions.
func (d *differ[S, E, C]) diffState(path string, os, ns state.State[S, E, C]) {
	if os.OwnedBy != ns.OwnedBy {
		d.r.add(Change{
			Kind:        KindMetadataChanged,
			Path:        path,
			Description: fmt.Sprintf("OwnedBy %q -> %q; metadata only, no behavior change", os.OwnedBy, ns.OwnedBy),
			Breaking:    false,
		})
	}
	if os.IsFinal != ns.IsFinal {
		d.r.add(Change{
			Kind:        KindFinalChanged,
			Path:        path,
			Description: fmt.Sprintf("IsFinal %v -> %v; changes terminality of a persisted state", os.IsFinal, ns.IsFinal),
			Breaking:    true,
		})
	}

	d.diffTransitions(path, os.Transitions, ns.Transitions)

	// Recurse into the hierarchy.
	d.diffStates(path, os.Children, ns.Children)
	d.diffRegions(path, os.Regions, ns.Regions)
}

// diffRegions compares the orthogonal regions of two parallel states by region
// name.
func (d *differ[S, E, C]) diffRegions(prefix string, oldRegions, newRegions []state.Region[S, E, C]) {
	oldByName := make(map[string]state.Region[S, E, C], len(oldRegions))
	for _, rg := range oldRegions {
		oldByName[rg.Name] = rg
	}
	newByName := make(map[string]state.Region[S, E, C], len(newRegions))
	for _, rg := range newRegions {
		newByName[rg.Name] = rg
	}
	for name, or := range oldByName {
		path := join(prefix, "region:"+name)
		nr, ok := newByName[name]
		if !ok {
			d.r.add(Change{
				Kind:        KindStateRemoved,
				Path:        path,
				Description: fmt.Sprintf("region %q removed from parallel state", name),
				Breaking:    true,
			})
			continue
		}
		d.diffStates(path, or.States, nr.States)
	}
	for name, nr := range newByName {
		if _, ok := oldByName[name]; !ok {
			path := join(prefix, "region:"+name)
			d.r.add(Change{
				Kind:        KindStateAdded,
				Path:        path,
				Description: fmt.Sprintf("new region %q added", name),
				Breaking:    false,
			})
			// Surface the region's states as additive too.
			d.diffStates(path, nil, nr.States)
		}
	}
}

// transitionKey identifies a transition by its source state and triggering
// event. The Evolution Guide frames a transition as "a new event on an existing
// state", so (From, On) is the stable identity; a change to To for the same key
// is a retarget, not an add+remove.
type transitionKey struct {
	from string
	on   string
}

func (d *differ[S, E, C]) diffTransitions(statePath string, oldTr, newTr []state.Transition[S, E, C]) {
	oldByKey := indexTransitions(oldTr)
	newByKey := indexTransitions(newTr)

	for key, ot := range oldByKey {
		tpath := statePath + "/" + key.on
		nt, ok := newByKey[key]
		if !ok {
			d.r.add(Change{
				Kind:        KindTransitionRemoved,
				Path:        tpath,
				Description: fmt.Sprintf("transition on %q removed; paths relying on it become ErrNoPath", key.on),
				Breaking:    true,
			})
			continue
		}
		d.diffTransition(tpath, ot, nt)
	}

	for key := range newByKey {
		if _, ok := oldByKey[key]; !ok {
			tpath := statePath + "/" + key.on
			d.r.add(Change{
				Kind:        KindTransitionAdded,
				Path:        tpath,
				Description: fmt.Sprintf("new transition on %q from an existing state; existing Fire calls unaffected", key.on),
				Breaking:    false,
			})
		}
	}
}

// diffTransition compares one transition present in both definitions (same
// From/On key).
func (d *differ[S, E, C]) diffTransition(tpath string, ot, nt state.Transition[S, E, C]) {
	if str(ot.To) != str(nt.To) {
		d.r.add(Change{
			Kind:        KindTransitionRetargeted,
			Path:        tpath,
			Description: fmt.Sprintf("transition target changed %s -> %s; the graph's reachability changes", str(ot.To), str(nt.To)),
			Breaking:    true,
		})
	}
	if ot.WaitMode != nt.WaitMode {
		d.r.add(Change{
			Kind:        KindWaitModeChanged,
			Path:        tpath,
			Description: fmt.Sprintf("WaitMode %v -> %v; safe if consumers handle both modes", ot.WaitMode, nt.WaitMode),
			Breaking:    false,
		})
	}

	// A composite guard expression's named-ref and stateIn leaves count as guard
	// requirements for evolution classification: adding one tightens the
	// transition (flagged-additive), removing one loosens it.
	d.diffRefs(tpath, "guard", guardRefs(ot), guardRefs(nt), KindGuardAdded, KindGuardRemoved)
	d.diffRefs(tpath, "effect", ot.Effects, nt.Effects, KindEffectAdded, KindEffectRemoved)
}

// diffRefs compares two sets of named Refs (guards or effects) on a transition.
// Adding a guard is additive but flagged: it is only safe if every entity
// currently in that state satisfies the new guard (Evolution Guide caveat).
// Adding an effect is plainly additive; removing either is a loosening and
// additive.
func (d *differ[S, E, C]) diffRefs(tpath, kind string, oldRefs, newRefs []state.Ref, addKind, remKind ChangeKind) {
	oldSet := refNameSet(oldRefs)
	newSet := refNameSet(newRefs)
	for name := range newSet {
		if _, ok := oldSet[name]; ok {
			continue
		}
		desc := fmt.Sprintf("%s %q added", kind, name)
		if kind == "guard" {
			desc = fmt.Sprintf("[FLAGGED: audit data first] %s %q added; additive only if every entity currently in this state already satisfies it (Evolution Guide caveat)", kind, name)
		}
		d.r.add(Change{Kind: addKind, Path: tpath, Description: desc, Breaking: false})
	}
	for name := range oldSet {
		if _, ok := newSet[name]; !ok {
			d.r.add(Change{
				Kind:        remKind,
				Path:        tpath,
				Description: fmt.Sprintf("%s %q removed; a loosening of the transition's requirements", kind, name),
				Breaking:    false,
			})
		}
	}
}

// --- helpers -------------------------------------------------------------

func indexStates[S comparable, E comparable, C any](states []state.State[S, E, C]) map[string]state.State[S, E, C] {
	m := make(map[string]state.State[S, E, C], len(states))
	for _, s := range states {
		m[str(s.Name)] = s
	}
	return m
}

// guardRefs returns every guard requirement of a transition: its plain guard
// refs plus the named-ref and stateIn leaves of any composite guard expression.
// stateIn leaves are rendered as synthetic "stateIn(<state>)" ref names so they
// participate in add/remove classification like any other guard.
func guardRefs[S comparable, E comparable, C any](t state.Transition[S, E, C]) []state.Ref {
	refs := append([]state.Ref(nil), t.Guards...)
	if t.GuardExpr != nil {
		refs = append(refs, t.GuardExpr.LeafRefs()...)
		for _, in := range t.GuardExpr.StateInTargets() {
			refs = append(refs, state.Ref{Name: fmt.Sprintf("stateIn(%v)", in)})
		}
	}
	return refs
}

func indexTransitions[S comparable, E comparable, C any](trs []state.Transition[S, E, C]) map[transitionKey]state.Transition[S, E, C] {
	m := make(map[transitionKey]state.Transition[S, E, C], len(trs))
	for _, t := range trs {
		m[transitionKey{from: str(t.From), on: str(t.On)}] = t
	}
	return m
}

func refNameSet(refs []state.Ref) map[string]struct{} {
	m := make(map[string]struct{}, len(refs))
	for _, r := range refs {
		m[r.Name] = struct{}{}
	}
	return m
}

func initialStr[S comparable, E comparable, C any](ir *state.IR[S, E, C]) string {
	if !ir.HasInitial {
		return "<none>"
	}
	return str(ir.Initial)
}

// str renders any comparable state/event value as a stable string key.
func str(v any) string { return fmt.Sprint(v) }

func join(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}
