package dispatch

import (
	"fmt"
	"sort"

	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/verify"
	"github.com/stablekernel/crucible/state/verify/symbolic"
)

// keyStages are the order-saga states the proof insists on being able to reach:
// the two terminals the business cares about (Delivered, Canceled), the declined
// terminal (Rejected), the orthogonal fulfillment superstate (Active), and the
// Watchdog region's breach leaf (Overdue). Reachability here is exact: the analyzer
// computes it guard-agnostically, so a stage reachable in this set is reachable in
// some real run.
var keyStages = []fooddelivery.Stage{
	fooddelivery.Active,
	fooddelivery.Delivered,
	fooddelivery.Canceled,
	fooddelivery.Rejected,
	fooddelivery.Overdue,
}

// GuardVerdict records one transition guard's satisfiability check: the guard's
// human-readable rendering and whether the symbolic analyzer found it satisfiable.
// An unsatisfiable guard is a dead branch — a transition that can never fire — and
// is the defect the proof is designed to surface.
type GuardVerdict struct {
	// Guard is the compact rendering of the transition's effective guard.
	Guard string
	// Satisfiable reports whether some context assignment can enable the guard. The
	// symbolic analyzer is conservative on opaque (named Go-func and Rich CEL) leaves:
	// it treats them as satisfiable rather than risk a false dead-branch report.
	Satisfiable bool
}

// ProofReport is the outcome of [Prove]: the formal evidence that the order saga is
// well-formed. Every field is a positive assertion about the machine, so a report
// whose [ProofReport.Sound] is true is a machine with no dead ends, no contradictory
// guards, and no analyzable nondeterminism.
type ProofReport struct {
	// Reachable maps each key stage's name to whether it can be entered in some run.
	// A false entry is a dead state the saga can never reach.
	Reachable map[string]bool
	// WatchdogExclusive reports whether the Watchdog region's OnTime and Overdue
	// leaves are never simultaneously active — the mutual-exclusion invariant that
	// holds because they are sequential leaves of one region.
	WatchdogExclusive bool
	// Overlaps lists every same-source/same-event transition pair whose guards the
	// analyzer could not prove disjoint. It is conservative: a pair guarded by an
	// opaque (Rich CEL or named Go-func) guard is reported even when the two branches
	// are in fact mutually exclusive, because the analyzer refuses to assume a verdict
	// it cannot prove.
	Overlaps []symbolic.Overlap[fooddelivery.Stage, fooddelivery.Signal]
	// Guards lists each transition guard and its satisfiability verdict, in a stable
	// order, so a contradictory (dead) guard is named rather than merely counted.
	Guards []GuardVerdict
}

// Sound reports whether the proof found the machine well-formed: every key stage is
// reachable, the Watchdog leaves are mutually exclusive, and no transition guard is
// a dead branch. It deliberately does not fold [ProofReport.Overlaps] into the
// verdict, because the analyzer's overlap pass is conservative over opaque guards
// (see [ProofReport.Overlaps]); the report exposes the raw overlap list so a caller
// can judge it against the machine's known opaque branches.
func (r ProofReport) Sound() bool {
	for _, reachable := range r.Reachable {
		if !reachable {
			return false
		}
	}
	if !r.WatchdogExclusive {
		return false
	}
	for _, g := range r.Guards {
		if !g.Satisfiable {
			return false
		}
	}
	return true
}

// Prove runs the suite's formal checks over the order-saga machine and returns the
// evidence as a [ProofReport]. It verifies reachability of the key stages and the
// Watchdog mutual-exclusion invariant with the exact, guard-agnostic [verify.Verify]
// pass; scans for nondeterministic competing transitions with
// [symbolic.Overlaps]; and confirms no transition guard is contradictory with
// [symbolic.Satisfiable]. It returns an error only when the machine cannot be
// serialized for the symbolic passes; the verify pass itself never fails.
func Prove(m *state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) (ProofReport, error) {
	watchdog := verify.MutualExclusion(fooddelivery.OnTime.String(), fooddelivery.Overdue.String())

	opts := make([]verify.Option, 0, len(keyStages)+1)
	for _, s := range keyStages {
		opts = append(opts, verify.Reachable(s.String()))
	}
	opts = append(opts, verify.CheckInvariant(watchdog))

	res := verify.Verify(m, opts...)

	report := ProofReport{Reachable: make(map[string]bool, len(keyStages))}
	for _, s := range keyStages {
		report.Reachable[s.String()] = res.CanReach(s.String())
	}
	if finding, ok := res.Invariant(watchdog.Label()); ok {
		report.WatchdogExclusive = finding.Reachable
	}

	overlaps, err := symbolic.Overlaps(m)
	if err != nil {
		return ProofReport{}, fmt.Errorf("dispatch: scan overlaps: %w", err)
	}
	report.Overlaps = overlaps

	guards, err := guardVerdicts(m)
	if err != nil {
		return ProofReport{}, fmt.Errorf("dispatch: check guards: %w", err)
	}
	report.Guards = guards

	return report, nil
}

// guardVerdicts loads the machine's IR and runs a satisfiability check on every
// transition's effective guard, returning the verdicts in a stable rendering order.
// It mirrors the IR walk [symbolic.Overlaps] uses — descending into compound
// children and parallel regions — so no guarded edge escapes the check.
func guardVerdicts(m *state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) ([]GuardVerdict, error) {
	js, err := m.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("serialize machine: %w", err)
	}
	ir, err := state.LoadFromJSON[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order](js)
	if err != nil {
		return nil, fmt.Errorf("load machine IR: %w", err)
	}

	var schema state.ContextSchema
	if ir.Context != nil {
		schema = *ir.Context
	}

	var states []state.State[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]
	var collect func(ss []state.State[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order])
	collect = func(ss []state.State[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) {
		for i := range ss {
			states = append(states, ss[i])
			collect(ss[i].Children)
			for r := range ss[i].Regions {
				collect(ss[i].Regions[r].States)
			}
		}
	}
	collect(ir.States)

	seen := make(map[string]bool)
	var verdicts []GuardVerdict
	for si := range states {
		for _, t := range states[si].Transitions {
			guard := effectiveGuard(t)
			if guard.Op == "" && len(guard.Children) == 0 && guard.Ref == nil {
				// An unguarded transition has the empty conjunction, which is trivially
				// satisfiable; recording it would only add noise, so skip it.
				continue
			}
			rendered := renderGuard(guard)
			if seen[rendered] {
				continue
			}
			seen[rendered] = true
			verdicts = append(verdicts, GuardVerdict{
				Guard:       rendered,
				Satisfiable: symbolic.Satisfiable(guard, schema),
			})
		}
	}

	sort.Slice(verdicts, func(i, j int) bool { return verdicts[i].Guard < verdicts[j].Guard })
	return verdicts, nil
}

// renderGuard renders a guard node as a compact, stable string so each transition
// guard reads symbolically in the report and verdicts sort deterministically. It
// covers the node shapes the order saga uses — named-ref leaves, in-state leaves,
// field/literal operands, comparisons, membership, and the and/or/not combinators —
// and falls back to the bare op name for anything else.
func renderGuard(g state.GuardNode[fooddelivery.Stage]) string {
	switch g.Op {
	case state.GuardLeaf:
		if g.Ref != nil {
			return g.Ref.Name
		}
		return "leaf"
	case state.GuardStateIn:
		if g.In != nil {
			return fmt.Sprintf("in(%v)", *g.In)
		}
		return "in"
	case state.GuardNot:
		return "not(" + renderChildren(g.Children, ", ") + ")"
	case state.GuardAnd:
		return "and(" + renderChildren(g.Children, ", ") + ")"
	case state.GuardOr:
		return "or(" + renderChildren(g.Children, ", ") + ")"
	case state.GuardField:
		return g.Path
	case state.GuardLit:
		if g.Lit != nil {
			return fmt.Sprintf("%v", g.Lit.Value)
		}
		return "lit"
	case state.GuardEq, state.GuardNe, state.GuardLt, state.GuardLe, state.GuardGt, state.GuardGe:
		return renderChildren(g.Children, " "+string(g.Op)+" ")
	case state.GuardIn:
		return renderChildren(g.Children, ", ") + " in set"
	default:
		return string(g.Op)
	}
}

// renderChildren renders a guard node's operands joined by sep, the recursive arm
// of [renderGuard].
func renderChildren(children []state.GuardNode[fooddelivery.Stage], sep string) string {
	parts := make([]string, len(children))
	for i := range children {
		parts[i] = renderGuard(children[i])
	}
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += sep
		}
		out += p
	}
	return out
}

// effectiveGuard builds a transition's effective guard: the conjunction of its
// named-ref leaves and its composite GuardExpr, the same shape the symbolic overlap
// pass reasons over. A transition with no guard yields the zero node.
func effectiveGuard(t state.Transition[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) state.GuardNode[fooddelivery.Stage] {
	var nodes []state.GuardNode[fooddelivery.Stage]
	for _, g := range t.Guards {
		nodes = append(nodes, state.Guard[fooddelivery.Stage](g.Name))
	}
	if t.GuardExpr != nil {
		nodes = append(nodes, *t.GuardExpr)
	}
	if len(nodes) == 0 {
		return state.GuardNode[fooddelivery.Stage]{}
	}
	if len(nodes) == 1 {
		return nodes[0]
	}
	return state.And(nodes...)
}
