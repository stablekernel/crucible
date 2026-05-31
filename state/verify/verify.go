package verify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// Witness is the concrete evidence for a [Finding]: the route from the
// machine's initial state to the finding's state. It is the same path type the
// analysis package enumerates, so a witness is a first-class artifact a driver
// can replay — Witness.Events is exactly the event sequence to fire. An empty
// witness (no steps) is the initial state reaching itself, or the absence of
// evidence for a property that does not hold.
type Witness = analysis.Path

// FindingKind names the property a [Finding] decides. New kinds are added as
// new property checks land; a consumer should treat an unrecognized kind as
// informational rather than failing on it.
type FindingKind string

// The property kinds verify can decide. V1 ships reachability; later additions
// extend this set without changing existing values.
const (
	// KindReachability is the verdict on whether a declared state can be entered
	// in some run of the machine, starting from the initial state. It is exact:
	// reachability is computed guard-agnostically, and a guard can only ever prune
	// an edge at run time, never add one, so a state reachable here is reachable in
	// some run and an unreachable verdict holds in every run.
	KindReachability FindingKind = "reachability"

	// KindConditionalReachability is the verdict on whether a target state can be
	// reached along some run that never passes through any state in a declared
	// avoid-set — the "reach X without entering Y" safety/reachability property. It
	// is exact in the same sense as KindReachability: the constrained search is
	// guard-agnostic, and a guard can only ever prune an edge at run time, never
	// add one, so a clean route found here exists in some run and an unsatisfiable
	// verdict (every route crosses the avoid-set) holds in every run. A satisfiable
	// finding carries the witnessing event sequence; an unsatisfiable one carries
	// the zero Witness.
	KindConditionalReachability FindingKind = "conditional_reachability"
)

// Finding is one decided property about one state. Kind names the property,
// State names the subject, and Reachable carries the reachability verdict. When
// the property holds with supporting evidence, Witness is the route that proves
// it; an unreachable state carries the zero Witness.
type Finding struct {
	// Kind is the property this finding decides.
	Kind FindingKind
	// State is the state the finding concerns.
	State string
	// Reachable is the reachability verdict for the state.
	Reachable bool
	// Witness is the proving route when one exists: for a reachable state, the
	// shortest event sequence from the initial state to it. The zero Witness means
	// no supporting route (an unreachable state, or the initial state itself).
	Witness Witness
}

// Result is the outcome of a [Verify] pass: one [Finding] per decided property,
// in a deterministic order. The zero Result carries no findings.
type Result struct {
	// Findings are the decided properties, ordered by kind then by state name so
	// the report is reproducible.
	Findings []Finding
	// initial is the machine's initial state name, used to render the states a
	// witness visits via [Witness.States].
	initial string
}

// Initial returns the machine's initial state name, the anchor a witness's
// visited-state list begins at. It is "" when the machine declares no initial
// state.
func (r *Result) Initial() string { return r.initial }

// For returns the reachability finding for a single state and whether one
// exists. A state that is not declared in the machine has no finding.
func (r *Result) For(stateName string) (Finding, bool) {
	for _, f := range r.Findings {
		if f.Kind == KindReachability && f.State == stateName {
			return f, true
		}
	}
	return Finding{}, false
}

// CanReach reports whether the named state is reachable. An undeclared state is
// not reachable.
func (r *Result) CanReach(stateName string) bool {
	f, ok := r.For(stateName)
	return ok && f.Reachable
}

// ConditionalReach returns the conditional-reachability finding for a target and
// whether one exists. A finding exists only for a target a [ReachAvoiding] option
// requested that is also a declared state; its Reachable field is the
// satisfiability verdict (true when a route avoiding the forbidden set exists),
// and its Witness is the proving event sequence when satisfiable.
func (r *Result) ConditionalReach(target string) (Finding, bool) {
	for _, f := range r.Findings {
		if f.Kind == KindConditionalReachability && f.State == target {
			return f, true
		}
	}
	return Finding{}, false
}

// Unreachable returns the names of every declared state that cannot be entered,
// in sorted order. An empty result means every checked state is reachable.
func (r *Result) Unreachable() []string {
	var out []string
	for _, f := range r.Findings {
		if f.Kind == KindReachability && !f.Reachable {
			out = append(out, f.State)
		}
	}
	sort.Strings(out)
	return out
}

// OK reports whether the result contains no defect: no checked state is
// unreachable.
func (r *Result) OK() bool { return len(r.Unreachable()) == 0 }

// String renders the result as one line per finding, in finding order, so a
// report is human-readable and diffable. A reachability finding shows its witness
// event sequence when the state is reachable, or is marked unreachable; a
// conditional-reachability finding reads as satisfiable (with its avoid-free
// witness) or unsatisfiable.
func (r *Result) String() string {
	if len(r.Findings) == 0 {
		return "no findings"
	}
	var b strings.Builder
	for i, f := range r.Findings {
		if i > 0 {
			b.WriteByte('\n')
		}
		hit, miss := findingVerbs(f.Kind)
		if f.Reachable {
			fmt.Fprintf(&b, "%-24s %s: %s %v", f.Kind, f.State, hit, f.Witness.Events())
		} else {
			fmt.Fprintf(&b, "%-24s %s: %s", f.Kind, f.State, miss)
		}
	}
	return b.String()
}

// findingVerbs returns the satisfied/unsatisfied phrasing for a finding kind, so
// each property reads naturally: reachability is "reachable via"/"unreachable",
// conditional reachability is "satisfiable via"/"unsatisfiable".
func findingVerbs(k FindingKind) (hit, miss string) {
	if k == KindConditionalReachability {
		return "satisfiable via", "unsatisfiable"
	}
	return "reachable via", "unreachable"
}

// Verify checks behavioral properties of a Quenched machine and returns a
// [Result] carrying one [Finding] per decided property, each with a witness
// where one exists. The machine's IR is read via its serialized form — no
// instance is cast and no guard or action is evaluated — so a machine built by
// the Forge DSL and one loaded from JSON verify identically.
//
// With no options, Verify checks reachability of every declared state. Pass
// [Reachable] to restrict the pass to named target states. Options are additive:
// later property checks arrive as new option constructors without changing this
// signature.
//
// Verify never returns nil and never panics: a machine whose IR cannot be read
// yields an empty result rather than an error, honoring the kernel's no-panic
// contract for read-only inspection.
func Verify[S comparable, E comparable, C any](m *state.Machine[S, E, C], opts ...Option) *Result {
	cfg := config{}
	for _, o := range opts {
		o(&cfg)
	}

	res := &Result{initial: initialName(m)}
	top := readTopology(m)

	// Authoritative reachability comes from the analysis package's static pass:
	// its KindUnreachableState finding is the proven verdict, and it correctly
	// accounts for hierarchical entry (a composite or region substate entered by
	// initial descent is reachable even though no event names it directly). verify
	// is the property-checking layer that consumes this verdict rather than
	// re-deriving the reachability set.
	report := analysis.Analyze(m, analysis.Only(analysis.KindUnreachableState))
	unreachable := map[string]bool{}
	for _, f := range report.OfKind(analysis.KindUnreachableState) {
		unreachable[f.State] = true
	}

	// Witnesses come from the analysis shortest-path enumeration: for a state
	// entered by a firing event it is the minimal event sequence; for a substate
	// entered by initial descent (no firing event of its own) the witness is the
	// path to the nearest enclosing ancestor whose activation transitively enters
	// the substate. Firing that ancestor's witness drives an instance into a
	// configuration that includes the substate, so the witness is replayable.
	paths, err := analysis.ShortestPaths(m)
	if err != nil {
		// An IR that cannot serialize is a kernel bug, not a user defect; surface it
		// as an empty result rather than panicking.
		return res
	}

	targets := top.order
	if len(cfg.targets) > 0 {
		targets = filterTargets(top.order, cfg.targets)
	}

	for _, name := range targets {
		reachable := !unreachable[name]
		var w Witness
		if reachable {
			w = witnessFor(name, paths, top)
		}
		res.Findings = append(res.Findings, Finding{
			Kind:      KindReachability,
			State:     name,
			Reachable: reachable,
			Witness:   w,
		})
	}

	// Conditional reachability ("reach X without passing through Y") runs its own
	// avoid-pruning search over the structural graph, since the reachable-space
	// pass above and analysis.ShortestPaths cannot exclude an avoid-set.
	if len(cfg.reachAvoiding) > 0 {
		g := buildSearchGraph(m)
		for _, q := range cfg.reachAvoiding {
			if !g.nodes[q.target] {
				continue // report on declared states only, matching Reachable
			}
			w, ok := g.reachAvoiding(q.target, q.avoid)
			res.Findings = append(res.Findings, Finding{
				Kind:      KindConditionalReachability,
				State:     q.target,
				Reachable: ok,
				Witness:   w,
			})
		}
	}

	sortFindings(res.Findings)
	return res
}

// witnessFor returns the proving route for a reachable state. When the state is
// directly named by a path it is returned as-is; otherwise the state is entered
// by initial descent and the witness is the path to its nearest enclosing
// ancestor that a path reaches, retargeted to the state so a caller reading
// Witness.Target sees the state it asked about. An ancestor-derived witness has
// the same Events as the ancestor's: firing them activates the composite, which
// transitively enters this substate.
func witnessFor(name string, paths map[string]analysis.Path, top topology) Witness {
	if p, ok := paths[name]; ok {
		return p
	}
	for anc := top.parent[name]; anc != ""; anc = top.parent[anc] {
		if p, ok := paths[anc]; ok {
			return analysis.Path{Target: name, Steps: p.Steps}
		}
	}
	return analysis.Path{Target: name}
}

// sortFindings orders findings deterministically: by kind, then by state name,
// so a report is reproducible across runs.
func sortFindings(fs []Finding) {
	sort.SliceStable(fs, func(i, j int) bool {
		if fs[i].Kind != fs[j].Kind {
			return fs[i].Kind < fs[j].Kind
		}
		return fs[i].State < fs[j].State
	})
}

// filterTargets returns the declared states that appear in want, preserving
// declaration order and dropping any requested name that is not a declared
// state.
func filterTargets(declared []string, want map[string]bool) []string {
	var out []string
	for _, n := range declared {
		if want[n] {
			out = append(out, n)
		}
	}
	return out
}
