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

	// KindLiveness is the verdict on whether a target state is always eventually
	// reachable: from every reachable configuration, can the target still be
	// reached? It is the CTL eventuality AG EF target. It is exact in the same
	// sense as KindReachability: the reverse-reachability computation is
	// guard-agnostic, and a guard can only ever prune an edge at run time, never
	// add one, so a configuration from which the structural graph offers no route
	// to the target has no run to it in any instance, and a holding verdict holds
	// in every run. A holding finding (Reachable true) carries the zero Witness; a
	// failing one carries a counterexample witness: the route to a reachable
	// configuration — a target-free terminal or cycle — from which the target can
	// never be reached.
	KindLiveness FindingKind = "liveness"

	// KindInvariant is the verdict on whether a configuration invariant — a
	// predicate over the active-state configuration, such as mutual exclusion,
	// implication, or never-active — holds in every reachable configuration. It is
	// exact in the same guard-agnostic sense as KindReachability: the
	// configuration-product exploration is guard-agnostic, and a guard can only ever
	// prune an edge at run time, never add one, so a configuration reachable
	// structurally is reachable in some run and a holding verdict holds in every run.
	// A holding finding (Reachable true) carries the zero Witness; a failing one
	// carries a counterexample witness: the shortest route to a reachable
	// configuration that violates the predicate, whose Target names that
	// configuration.
	KindInvariant FindingKind = "invariant"

	// KindBoundedViolation is the verdict of a bounded exhaustive simulation: a
	// caller-supplied oracle was evaluated at every configuration reachable within a
	// depth bound, and either held throughout (Reachable true, zero Witness) or was
	// rejected by some reachable configuration (Reachable false), in which case the
	// Witness is the shortest trace — the event sequence that drives the machine
	// into the rejected configuration, whose Target names it. Unlike the other
	// kinds it is NOT exact: a holding verdict means only that the oracle held
	// across the configurations reachable within the bound, never that it holds in
	// every run. A violation, by contrast, is real: the reported trace is replayable
	// and the oracle genuinely fails at the reached configuration.
	KindBoundedViolation FindingKind = "bounded_violation"

	// KindCoverage is the verdict of a structural-coverage analysis: a set of
	// scenarios (event sequences) was replayed over the configuration-product
	// explorer and the states and transitions they exercised were measured against
	// the reachable universe. Its Reachable field is true when the scenario set
	// leaves no reachable state and no reachable transition uncovered (full
	// coverage), false otherwise. The full breakdown — covered and uncovered states
	// and transitions with their fractions — is read with [Result.Coverage] rather
	// than from a Witness, since coverage concerns a whole universe rather than a
	// single configuration. It is guard-agnostic like the other kinds: the universe
	// is the structural reachable space, so an uncovered element is a real gap a
	// scenario set leaves unexercised.
	KindCoverage FindingKind = "coverage"
)

// coverageLabel is the stable Finding.State a coverage finding is keyed by, so a
// single [Coverage] pass yields one [Result.Coverage]-addressable finding.
const coverageLabel = "coverage"

// Finding is one decided property about one state. Kind names the property,
// State names the subject, and Reachable carries the reachability verdict. When
// the property holds with supporting evidence, Witness is the route that proves
// it; an unreachable state carries the zero Witness.
type Finding struct {
	// Kind is the property this finding decides.
	Kind FindingKind
	// State is the state the finding concerns.
	State string
	// Reachable is the property verdict for the state, but its polarity is
	// overloaded per Kind — the field name reads naturally only for the
	// reachability kinds. To read it unambiguously, prefer the kind-specific
	// accessors [Finding.IsReachable], [Finding.Holds], [Finding.Violated], and
	// [Finding.Covered], which interpret the bool for the finding's actual Kind.
	// The per-kind meaning of a true value is:
	//
	//   - [KindReachability], [KindConditionalReachability]: the (possibly
	//     constrained) target is reachable.
	//   - [KindLiveness]: the target is always eventually reachable from every
	//     reachable configuration (the property holds).
	//   - [KindInvariant]: the predicate holds in every reachable configuration
	//     (no violation).
	//   - [KindBoundedViolation]: the oracle held across every configuration
	//     reachable within the depth bound (no violation within the bound).
	//   - [KindCoverage]: the scenarios leave nothing in the reachable universe
	//     uncovered.
	//
	// So for the reachability kinds a true verdict is merely informational, while
	// for the liveness/invariant/bounded/coverage kinds a true verdict is the
	// desirable one and a false verdict carries a counterexample or coverage gap.
	//
	// Note: the single overloaded bool is an advisory-tier shape, not part of the
	// frozen v1.0 contract (see the package stability banner); a future minor
	// release may split it into kind-specific fields.
	Reachable bool
	// Witness is the supporting route. For reachability and conditional
	// reachability it is the proving event sequence when the property holds, and
	// the zero Witness when it does not. For [KindLiveness] the witness is inverted:
	// a holding verdict carries the zero Witness, while a failing verdict carries
	// the route to the stuck configuration from which the target can never be
	// reached, whose Target names that configuration.
	Witness Witness
	// coverage carries the structural-coverage breakdown for a [KindCoverage]
	// finding, exposed via [Result.Coverage]. It is nil for every other kind.
	coverage *CoverageReport
}

// IsReachable reports whether this finding's target is reachable. It is meaningful
// only for the reachability kinds ([KindReachability] and
// [KindConditionalReachability]), where it returns the Reachable verdict directly;
// for every other kind it returns false, since "reachable" is not the property
// those kinds decide. Use it to read the overloaded Reachable bool without
// hard-coding its per-kind polarity.
func (f Finding) IsReachable() bool {
	switch f.Kind {
	case KindReachability, KindConditionalReachability:
		return f.Reachable
	default:
		return false
	}
}

// Holds reports whether the decided property holds. It is meaningful for the kinds
// whose true verdict is the desirable one — [KindLiveness] (always-eventually),
// [KindInvariant] (no violation), and [KindBoundedViolation] (no violation within
// the bound) — returning the Reachable verdict for those. For [KindReachability]
// and [KindConditionalReachability] there is no holds/fails property, so it returns
// false; read those with [Finding.IsReachable] instead. A bounded-violation Holds is
// a bounded guarantee only, not a proof of absence.
func (f Finding) Holds() bool {
	switch f.Kind {
	case KindLiveness, KindInvariant, KindBoundedViolation:
		return f.Reachable
	default:
		return false
	}
}

// Violated reports whether the decided property is violated — the negation of
// [Finding.Holds] for the liveness, invariant, and bounded-violation kinds, where a
// true result means the finding carries a counterexample Witness. For every other
// kind it returns false (no holds/fails property to violate).
func (f Finding) Violated() bool {
	switch f.Kind {
	case KindLiveness, KindInvariant, KindBoundedViolation:
		return !f.Reachable
	default:
		return false
	}
}

// Covered reports whether a [KindCoverage] finding leaves nothing in the reachable
// universe uncovered. It is meaningful only for [KindCoverage] and returns false for
// every other kind. The full covered/uncovered breakdown is read with
// [Result.Coverage].
func (f Finding) Covered() bool {
	if f.Kind == KindCoverage {
		return f.Reachable
	}
	return false
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

// Liveness returns the liveness finding for a target and whether one exists. A
// finding exists only for a target an [AlwaysEventually] option requested that is
// also a declared state. Its Reachable field is the liveness verdict — true when
// the target is always eventually reachable from every reachable configuration —
// and its Witness, when the verdict is false, is the route to a reachable
// configuration from which the target can never be reached.
func (r *Result) Liveness(target string) (Finding, bool) {
	for _, f := range r.Findings {
		if f.Kind == KindLiveness && f.State == target {
			return f, true
		}
	}
	return Finding{}, false
}

// Invariant returns the invariant finding for an invariant label and whether one
// exists. A finding exists only for an invariant a [CheckInvariant] option
// requested; look up the label with [Invariant.Label]. Its Reachable field is the
// invariant verdict — true when the predicate holds in every reachable
// configuration — and its Witness, when the verdict is false, is the route to the
// nearest reachable configuration that violates the predicate, whose Target names
// that configuration.
func (r *Result) Invariant(label string) (Finding, bool) {
	for _, f := range r.Findings {
		if f.Kind == KindInvariant && f.State == label {
			return f, true
		}
	}
	return Finding{}, false
}

// BoundedSim returns the bounded-simulation finding for a label and whether one
// exists. A finding exists only for a label a [SimulateBounded] option requested.
// Its Reachable field is the bounded verdict — true when the oracle held across
// every configuration reachable within the depth bound — and its Witness, when the
// verdict is false, is the shortest trace to the rejected configuration, whose
// Target names that configuration. A holding verdict is a bounded guarantee only,
// not a proof of absence.
func (r *Result) BoundedSim(label string) (Finding, bool) {
	for _, f := range r.Findings {
		if f.Kind == KindBoundedViolation && f.State == label {
			return f, true
		}
	}
	return Finding{}, false
}

// Coverage returns the structural-coverage breakdown and whether one exists. A
// report exists only when a [Coverage] option was passed. Its Reachable companion
// finding (see [KindCoverage]) is true exactly when the report leaves nothing
// uncovered; the report itself carries the covered and uncovered states and
// transitions and their fractions.
func (r *Result) Coverage() (CoverageReport, bool) {
	for _, f := range r.Findings {
		if f.Kind == KindCoverage && f.coverage != nil {
			return *f.coverage, true
		}
	}
	return CoverageReport{}, false
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
		b.WriteString(renderFinding(f))
	}
	return b.String()
}

// renderFinding renders one finding as a single line. Most properties read
// "satisfied via <witness>" or "unsatisfied"; liveness inverts the witness — a
// holding verdict has no counterexample, while a failing one names the stuck
// configuration the counterexample path reaches.
func renderFinding(f Finding) string {
	if f.Kind == KindCoverage {
		var rep CoverageReport
		if f.coverage != nil {
			rep = *f.coverage
		}
		return fmt.Sprintf("%-24s %s:\n%s", f.Kind, f.State, rep.String())
	}
	hit, miss := findingVerbs(f.Kind)
	if f.Kind == KindLiveness || f.Kind == KindInvariant || f.Kind == KindBoundedViolation {
		if f.Reachable {
			return fmt.Sprintf("%-24s %s: %s", f.Kind, f.State, hit)
		}
		return fmt.Sprintf("%-24s %s: %s %s via %v", f.Kind, f.State, miss, f.Witness.Target, f.Witness.Events())
	}
	if f.Reachable {
		return fmt.Sprintf("%-24s %s: %s %v", f.Kind, f.State, hit, f.Witness.Events())
	}
	return fmt.Sprintf("%-24s %s: %s", f.Kind, f.State, miss)
}

// findingVerbs returns the satisfied/unsatisfied phrasing for a finding kind, so
// each property reads naturally: reachability is "reachable via"/"unreachable",
// conditional reachability is "satisfiable via"/"unsatisfiable".
func findingVerbs(k FindingKind) (hit, miss string) {
	switch k {
	case KindConditionalReachability:
		return "satisfiable via", "unsatisfiable"
	case KindLiveness:
		return "always eventually reachable", "stuck at"
	case KindInvariant:
		return "holds", "violated at"
	case KindBoundedViolation:
		return "no violation within bound", "violation at"
	default:
		return "reachable via", "unreachable"
	}
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

	// Round-trip the machine to its public IR once and reuse it across every
	// structural builder below. Each builder previously re-serialized the machine,
	// so a single Verify paid for the JSON marshal/unmarshal several times over.
	ir, irOK := loadIR(m)

	var (
		res *Result
		top topology
	)
	if irOK {
		res = &Result{initial: initialNameFromIR(ir)}
		top = topologyFromIR(ir)
	} else {
		res = &Result{initial: ""}
		top = topology{parent: map[string]string{}}
	}

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

	// Conditional reachability ("reach X without passing through Y") and liveness
	// ("from every reachable config, Z is always eventually reachable") both run
	// their own structural search, so build the search graph once when either is
	// requested.
	if len(cfg.reachAvoiding) > 0 || len(cfg.alwaysEventually) > 0 {
		var g searchGraph
		if irOK {
			g = buildSearchGraphFromIR(ir)
		} else {
			g = emptySearchGraph()
		}

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

		for _, target := range cfg.alwaysEventually {
			f, ok := livenessFor(g, target)
			if !ok {
				continue // report on declared states only, matching Reachable
			}
			res.Findings = append(res.Findings, f)
		}
	}

	// Configuration invariants, bounded simulation, and coverage all reason about
	// whole configurations of co-active leaves over the configuration-product space,
	// so they share one configGraph, built once when any is requested.
	if len(cfg.invariants) > 0 || len(cfg.boundedSims) > 0 || cfg.coverageRequested {
		var cg configGraph
		if irOK {
			cg = buildConfigGraphFromIR(ir)
		} else {
			cg = emptyConfigGraph()
		}
		if len(cfg.invariants) > 0 {
			exp := cg.explore()
			for _, inv := range cfg.invariants {
				res.Findings = append(res.Findings, invariantFor(cg, exp, inv))
			}
		}
		for _, q := range cfg.boundedSims {
			res.Findings = append(res.Findings, boundedSimFor(cg, q))
		}
		if cfg.coverageRequested {
			res.Findings = append(res.Findings, coverageFor(cg, cfg.coverageScenarios))
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
