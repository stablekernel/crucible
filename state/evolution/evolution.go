package evolution

import (
	"encoding/json"
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

	// KindTransitionReordered marks a state whose transition list was reordered
	// without changing the multiset of branches. Order decides which branch fires
	// first, so a pure reorder is behavior-changing and breaking.
	KindTransitionReordered ChangeKind = "transition_reordered"
	// KindGuardStructureChanged marks a composite guard expression whose operator
	// structure changed (e.g. And -> Or, or an added Not) even when its leaf-ref
	// set is unchanged. The boolean shape decides enablement, so it is breaking.
	KindGuardStructureChanged ChangeKind = "guard_structure_changed"
	// KindInitialChildChanged marks a compound state or region whose InitialChild
	// (default descent target) changed, including nil<->set. It changes where an
	// instance lands on entry, so it is breaking.
	KindInitialChildChanged ChangeKind = "initial_child_changed"
	// KindHistoryChanged marks a state whose HistoryType or HistoryDefault changed.
	// History semantics decide re-entry behavior, so the flip is breaking.
	KindHistoryChanged ChangeKind = "history_changed"
	// KindContextSchemaChanged marks a context schema field added, removed, or
	// retyped. The data model is part of the machine contract, so it is breaking.
	KindContextSchemaChanged ChangeKind = "context_schema_changed"
	// KindEventlessChanged marks an added eventless (Always) edge, or a paired
	// transition whose EventLess flag flipped. Eventless edges change
	// run-to-completion behavior, so they are breaking rather than additive.
	KindEventlessChanged ChangeKind = "eventless_changed"

	// KindUnknown marks a delta the differ has no explicit rule for. It is always
	// breaking and is flagged for human review, per the Evolution Guide's
	// "unknown -> breaking" default.
	KindUnknown ChangeKind = "unknown"

	// KindUnknownStructuralDelta is the fail-safe backstop: it marks a residual
	// structural difference on a paired transition that none of the modeled rules
	// accounts for (e.g. Forbidden, Wildcard, Internal, Reenter, Raise, or a future
	// field). It forces a major bump so an unmodeled IR addition over-reports
	// rather than silently classifying as a patch.
	KindUnknownStructuralDelta ChangeKind = "unknown_structural_delta"
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

	d.diffContextSchema(old.Context, updated.Context)

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

// diffContextSchema compares the IR-level context schemas. A field added,
// removed, or retyped (recursively, by name) changes the machine's data
// contract and is breaking. Either side may be nil (no schema declared).
func (d *differ[S, E, C]) diffContextSchema(oldCtx, newCtx *state.ContextSchema) {
	oldFields := contextFields(oldCtx)
	newFields := contextFields(newCtx)
	if schemaFieldsEqual(oldFields, newFields) {
		return
	}
	d.r.add(Change{
		Kind:        KindContextSchemaChanged,
		Path:        "<context>",
		Description: "context schema changed (field added, removed, or retyped); the machine's data contract breaks",
		Breaking:    true,
	})
}

// contextFields returns the top-level fields of a (possibly nil) schema.
func contextFields(c *state.ContextSchema) []state.SchemaField {
	if c == nil {
		return nil
	}
	return c.Fields
}

// schemaFieldsEqual reports whether two field slices describe the same shape,
// matching by name and comparing kind, nullability, enum values, and nested
// shape recursively. Order is not significant.
func schemaFieldsEqual(a, b []state.SchemaField) bool {
	if len(a) != len(b) {
		return false
	}
	byName := make(map[string]state.SchemaField, len(b))
	for _, f := range b {
		byName[f.Name] = f
	}
	for _, fa := range a {
		fb, ok := byName[fa.Name]
		if !ok || !schemaFieldEqual(fa, fb) {
			return false
		}
	}
	return true
}

// schemaFieldEqual reports whether two named fields describe the same type.
func schemaFieldEqual(a, b state.SchemaField) bool {
	if a.Kind != b.Kind || a.Nullable != b.Nullable {
		return false
	}
	if !stringsEqual(a.Enum, b.Enum) {
		return false
	}
	if !schemaFieldsEqual(a.Fields, b.Fields) {
		return false
	}
	if !schemaPtrEqual(a.Elem, b.Elem) || !schemaPtrEqual(a.Key, b.Key) {
		return false
	}
	return true
}

// schemaPtrEqual compares two optional nested SchemaField pointers.
func schemaPtrEqual(a, b *state.SchemaField) bool {
	if a == nil || b == nil {
		return a == b
	}
	return schemaFieldEqual(*a, *b)
}

// stringsEqual reports whether two string slices are element-wise equal.
func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// diffStates compares two sibling state slices under the given dotted path
// prefix, recursing into children and regions.
func (d *differ[S, E, C]) diffStates(prefix string, oldStates, newStates []state.State[S, E, C]) {
	oldByName := statesByName(oldStates)
	newByName := statesByName(newStates)

	for name, os := range oldByName {
		path := join(prefix, name)
		ns, ok := newByName[name]
		if !ok {
			d.r.add(Change{
				Kind:        KindStateRemoved,
				Path:        path,
				Description: fmt.Sprintf("state %q removed; entities persisted in it hit InvalidTransitionError (a rename appears as remove+add)", name),
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
	if oldIC, newIC := ptrStr(os.InitialChild), ptrStr(ns.InitialChild); oldIC != newIC {
		d.r.add(Change{
			Kind:        KindInitialChildChanged,
			Path:        path,
			Description: fmt.Sprintf("InitialChild %s -> %s; the compound descends into a different default child", oldIC, newIC),
			Breaking:    true,
		})
	}
	if os.HistoryType != ns.HistoryType || ptrStr(os.HistoryDefault) != ptrStr(ns.HistoryDefault) {
		d.r.add(Change{
			Kind:        KindHistoryChanged,
			Path:        path,
			Description: fmt.Sprintf("history changed (type %v -> %v, default %s -> %s); re-entry behavior differs", os.HistoryType, ns.HistoryType, ptrStr(os.HistoryDefault), ptrStr(ns.HistoryDefault)),
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
		if oldIC, newIC := ptrStr(or.InitialChild), ptrStr(nr.InitialChild); oldIC != newIC {
			d.r.add(Change{
				Kind:        KindInitialChildChanged,
				Path:        path,
				Description: fmt.Sprintf("region InitialChild %s -> %s; the region enters a different default child", oldIC, newIC),
				Breaking:    true,
			})
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

// diffTransitions compares the transitions of one state across two definitions.
// Transitions are grouped by (From, On); within a group the old and new branches
// are matched so that guarded sibling branches on the same event are diffed
// independently rather than collapsed into a single map slot.
//
// When a group has exactly one branch on each side, the two are paired directly
// regardless of guard signature, so adding or removing a guard on a lone
// transition reads as a guard add/remove (a loosening/flagged-tightening) rather
// than a transition add+remove. When either side has multiple branches, branches
// are matched by guard signature: a branch whose guard set has no counterpart is
// reported as added or removed, surfacing a breaking change that the old (From,
// On)-only key silently hid.
func (d *differ[S, E, C]) diffTransitions(statePath string, oldTr, newTr []state.Transition[S, E, C]) {
	d.diffTransitionOrder(statePath, oldTr, newTr)

	oldGroups := groupTransitions(oldTr)
	newGroups := groupTransitions(newTr)

	for key, oldBranches := range oldGroups {
		tpath := statePath + "/" + key.on
		newBranches := newGroups[key]
		d.diffTransitionGroup(tpath, key, oldBranches, newBranches)
	}
	for key, newBranches := range newGroups {
		if _, ok := oldGroups[key]; ok {
			continue
		}
		tpath := statePath + "/" + key.on
		d.diffTransitionGroup(tpath, key, nil, newBranches)
	}
}

// diffTransitionOrder reports a breaking reorder when a state's transition list
// holds the same multiset of branches but in a different order. Order decides
// which branch fires first within a state, so a pure reorder is behavior-
// changing. It compares the ordered sequence of per-branch identity tokens
// (From, On, guard signature); when the sorted multisets match but the in-order
// sequences differ, the only change is order. Genuine adds/removes yield
// differing multisets and are skipped here (they are reported by the group diff).
func (d *differ[S, E, C]) diffTransitionOrder(statePath string, oldTr, newTr []state.Transition[S, E, C]) {
	oldSeq := transitionTokens(oldTr)
	newSeq := transitionTokens(newTr)
	if len(oldSeq) != len(newSeq) {
		return // add/remove changes the multiset; handled by the group diff.
	}
	if !multisetEqual(oldSeq, newSeq) {
		return // not the same set of branches; not a pure reorder.
	}
	if sequenceEqual(oldSeq, newSeq) {
		return // identical order.
	}
	d.r.add(Change{
		Kind:        KindTransitionReordered,
		Path:        statePath,
		Description: "transition order changed with the same branch set; a different branch now fires first",
		Breaking:    true,
	})
}

// diffTransitionGroup diffs the branches sharing one (From, On) key.
func (d *differ[S, E, C]) diffTransitionGroup(
	tpath string, key transitionKey, oldBranches, newBranches []state.Transition[S, E, C],
) {
	// Single branch on each side: pair directly so a guard added/removed on a lone
	// transition is classified as a guard change, not an add+remove.
	if len(oldBranches) == 1 && len(newBranches) == 1 {
		d.diffTransition(tpath, oldBranches[0], newBranches[0])
		return
	}

	// Multiple branches: match by guard signature so guarded siblings stay distinct.
	newBySig := make(map[string]state.Transition[S, E, C], len(newBranches))
	for _, nt := range newBranches {
		newBySig[guardSignature(nt)] = nt
	}
	matched := make(map[string]bool, len(newBranches))
	for _, ot := range oldBranches {
		sig := guardSignature(ot)
		nt, ok := newBySig[sig]
		if !ok {
			d.r.add(Change{
				Kind:        KindTransitionRemoved,
				Path:        tpath,
				Description: fmt.Sprintf("transition on %q removed; paths relying on it become NoPathError", key.on),
				Breaking:    true,
			})
			continue
		}
		matched[sig] = true
		d.diffTransition(tpath, ot, nt)
	}
	for _, nt := range newBranches {
		if matched[guardSignature(nt)] {
			continue
		}
		if nt.EventLess {
			// An added eventless (Always) edge changes run-to-completion behavior,
			// so it is breaking rather than an additive transition_added.
			d.r.add(Change{
				Kind:        KindEventlessChanged,
				Path:        tpath,
				Description: fmt.Sprintf("new eventless (Always) edge on %q; it fires automatically and changes run-to-completion behavior", key.on),
				Breaking:    true,
			})
			continue
		}
		d.r.add(Change{
			Kind:        KindTransitionAdded,
			Path:        tpath,
			Description: fmt.Sprintf("new transition on %q from an existing state; existing Fire calls unaffected", key.on),
			Breaking:    false,
		})
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

	if ot.EventLess != nt.EventLess {
		d.r.add(Change{
			Kind:        KindEventlessChanged,
			Path:        tpath,
			Description: fmt.Sprintf("EventLess %v -> %v; changes whether the edge fires automatically (run-to-completion)", ot.EventLess, nt.EventLess),
			Breaking:    true,
		})
	}

	// A composite guard expression's named-ref and stateIn leaves count as guard
	// requirements for evolution classification: adding one tightens the
	// transition (flagged-additive), removing one loosens it.
	d.diffRefs(tpath, "guard", guardRefs(ot), guardRefs(nt), KindGuardAdded, KindGuardRemoved)
	d.diffRefs(tpath, "effect", ot.Effects, nt.Effects, KindEffectAdded, KindEffectRemoved)

	// The leaf-ref diff above is operator-blind: And(a,b) and Or(a,b) share a leaf
	// set yet behave differently. Compare a canonical structural serialization of
	// the guard expression so a reshaped combinator tree (And<->Or, an added Not)
	// is caught even when the leaf set is unchanged. A pure leaf add/remove already
	// reported by diffRefs is suppressed by canonicalizing leaves to a sorted set.
	if guardStructure(ot.GuardExpr) != guardStructure(nt.GuardExpr) {
		d.r.add(Change{
			Kind:        KindGuardStructureChanged,
			Path:        tpath,
			Description: "guard expression structure changed (combinator reshaped, e.g. And<->Or or an added Not); enablement differs",
			Breaking:    true,
		})
	}

	d.diffUnknownStructuralDelta(tpath, ot, nt)
}

// diffUnknownStructuralDelta is the fail-safe backstop. Every structural field
// the differ models explicitly (From, To, On, Guards, Effects, WaitMode,
// GuardExpr, EventLess), plus the diagnostic-only source-position fields
// (SrcFile, SrcLine), is zeroed on both transitions, then the residuals are
// JSON-marshaled and compared. Any remaining difference — Forbidden, Wildcard,
// Internal, Reenter, Raise, or a field a future IR adds — trips this breaking
// change so unmodeled deltas over-report rather than silently classifying as a
// patch. Zeroing the understood fields keeps the backstop from double-firing on a
// difference a specific rule already reported; zeroing the source-position fields
// keeps it from flagging a benign, non-semantic call-site shift (e.g. an unchanged
// transition forged at a different line in the new revision). Marshaling failures
// are ignored; they cannot happen for the IR's JSON-safe transition shape, and the
// package's no-panic contract forbids surfacing them here.
func (d *differ[S, E, C]) diffUnknownStructuralDelta(tpath string, ot, nt state.Transition[S, E, C]) {
	if residualEqual(ot, nt) {
		return
	}
	d.r.add(Change{
		Kind:        KindUnknownStructuralDelta,
		Path:        tpath,
		Description: "[FLAGGED: unmodeled structural difference] a transition field the differ has no specific rule for changed; treated as breaking so future IR additions fail safe",
		Breaking:    true,
	})
}

// residualEqual reports whether two transitions are equal once the fields the
// differ understands are zeroed out, leaving only the residual structural shape.
func residualEqual[S comparable, E comparable, C any](ot, nt state.Transition[S, E, C]) bool {
	oj, err1 := json.Marshal(zeroUnderstood(ot))
	nj, err2 := json.Marshal(zeroUnderstood(nt))
	if err1 != nil || err2 != nil {
		// Marshaling cannot fail for the IR's JSON-safe transition; if it somehow
		// did we cannot prove equality, so fall through to "differ" (over-report).
		return false
	}
	return string(oj) == string(nj)
}

// zeroUnderstood returns a copy of the transition with every field the differ
// models explicitly reset to its zero value, so only residual (unmodeled) fields
// remain to be compared by the backstop. It also zeroes the diagnostic-only
// source-position fields (SrcFile, SrcLine): these are non-semantic call-site
// markers captured by the DSL builder (and strippable via WithoutSrcPos), so a
// position-only delta — e.g. an unchanged transition forged at a different line in
// the new revision — must not be mistaken for a structural change.
func zeroUnderstood[S comparable, E comparable, C any](t state.Transition[S, E, C]) state.Transition[S, E, C] {
	var zeroS S
	var zeroE E
	t.From = zeroS
	t.To = zeroS
	t.On = zeroE
	t.Guards = nil
	t.Effects = nil
	t.WaitMode = 0
	t.GuardExpr = nil
	t.EventLess = false
	// Diagnostic-only source position: never a behavior change.
	t.SrcFile = ""
	t.SrcLine = 0
	return t
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

func statesByName[S comparable, E comparable, C any](states []state.State[S, E, C]) map[string]state.State[S, E, C] {
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

// groupTransitions buckets transitions by their (From, On) key, preserving every
// guarded sibling branch in declaration order rather than collapsing them.
func groupTransitions[S comparable, E comparable, C any](trs []state.Transition[S, E, C]) map[transitionKey][]state.Transition[S, E, C] {
	m := make(map[transitionKey][]state.Transition[S, E, C], len(trs))
	for _, t := range trs {
		key := transitionKey{from: str(t.From), on: str(t.On)}
		m[key] = append(m[key], t)
	}
	return m
}

// guardSignature renders a transition's guard requirements as a stable string so
// guarded sibling branches on the same (from, on) get distinct transition keys.
// The signature folds the plain guard refs together with the composite guard's
// named-ref and stateIn leaves, sorted, so it is order-independent and
// deterministic across runs.
func guardSignature[S comparable, E comparable, C any](t state.Transition[S, E, C]) string {
	refs := guardRefs(t)
	if len(refs) == 0 {
		return ""
	}
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		names = append(names, r.Name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func refNameSet(refs []state.Ref) map[string]struct{} {
	m := make(map[string]struct{}, len(refs))
	for _, r := range refs {
		m[r.Name] = struct{}{}
	}
	return m
}

// ptrStr renders an optional state pointer as a stable string, "<nil>" when unset,
// so nil<->set transitions register as a difference.
func ptrStr[S comparable](p *S) string {
	if p == nil {
		return "<nil>"
	}
	return str(*p)
}

// transitionTokens renders each transition to its ordered identity token
// (From | On | guard signature), preserving declaration order, so the sequence
// can be compared for a pure reorder.
func transitionTokens[S comparable, E comparable, C any](trs []state.Transition[S, E, C]) []string {
	tokens := make([]string, 0, len(trs))
	for _, t := range trs {
		tokens = append(tokens, str(t.From)+"|"+str(t.On)+"|"+guardSignature(t))
	}
	return tokens
}

// multisetEqual reports whether two token slices contain the same elements with
// the same multiplicities, ignoring order.
func multisetEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		counts[s]--
		if counts[s] < 0 {
			return false
		}
	}
	return true
}

// sequenceEqual reports whether two token slices are equal element-wise in order.
func sequenceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// guardStructure renders a guard expression tree to a canonical, order-significant
// string capturing Op, Kind, Ref name, stateIn target, field Path, literal, and
// children recursively. It is operator-aware (unlike guardSignature, which folds
// only the leaf set), so And(a,b) and Or(a,b) produce different structures while
// reordering siblings of the same combinator is intentionally significant because
// it can change short-circuit behavior. A nil expression renders empty.
func guardStructure[S comparable](g *state.GuardNode[S]) string {
	if g == nil {
		return ""
	}
	var b strings.Builder
	writeGuardStructure(&b, *g)
	return b.String()
}

// writeGuardStructure appends one node's canonical structure to b, recursing into
// children. The form is "(op:kind ref=.. in=.. path=.. lit=.. set=[..] [children])".
func writeGuardStructure[S comparable](b *strings.Builder, g state.GuardNode[S]) {
	b.WriteByte('(')
	b.WriteString(string(g.Op))
	b.WriteByte(':')
	b.WriteString(string(g.Kind))
	if g.Ref != nil {
		b.WriteString(" ref=")
		b.WriteString(g.Ref.Name)
	}
	if g.In != nil {
		b.WriteString(" in=")
		b.WriteString(str(*g.In))
	}
	if g.Path != "" {
		b.WriteString(" path=")
		b.WriteString(g.Path)
	}
	if g.Lit != nil {
		fmt.Fprintf(b, " lit=%v:%v", g.Lit.Type, g.Lit.Value)
	}
	for i, l := range g.Set {
		if i == 0 {
			b.WriteString(" set=[")
		}
		fmt.Fprintf(b, "%v:%v,", l.Type, l.Value)
		if i == len(g.Set)-1 {
			b.WriteByte(']')
		}
	}
	for _, c := range g.Children {
		writeGuardStructure(b, c)
	}
	b.WriteByte(')')
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
