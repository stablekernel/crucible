package analysis_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// TestWildcard_MakesTargetReachable proves a wildcard (OnAny) transition is
// modeled as a real, traversable edge: a state reachable only via a catch-all
// edge must NOT be reported unreachable (F1).
func TestWildcard_MakesTargetReachable(t *testing.T) {
	// "open" reaches "closed" only through a wildcard edge; nothing names "closed"
	// via a specific On-keyed transition.
	m := forge("wildcard-reach").
		State("open").
		Transition("open").OnAny().GoTo("closed").
		State("closed").Final().
		Initial("open").
		Quench()

	r := analysis.Analyze(m)
	if states(r, analysis.KindUnreachableState)["closed"] {
		t.Fatalf("'closed' is reachable via a wildcard edge and must not be unreachable; report:\n%s", r)
	}
	// "open" leaves via the wildcard, so it is not a dead end.
	if states(r, analysis.KindDeadEnd)["open"] {
		t.Fatalf("'open' leaves via a wildcard edge and must not be a dead end; report:\n%s", r)
	}
}

// TestForbidden_IsNotTraversable proves a forbidden (Forbid/ForbidAny) transition
// is NOT modeled as a real edge: a state whose only "outgoing" transition is
// forbidden IS a dead end (F2), and a state targeted only by a forbidden edge's
// meaningless To field is NOT made reachable by it.
func TestForbidden_IsDeadEnd(t *testing.T) {
	// "stuck" is non-final and only forbids an event; a forbidden transition has no
	// target and never leaves the state, so "stuck" is a true dead end.
	m := forge("forbid-deadend").
		State("open").
		Transition("open").On("go").GoTo("stuck").
		Transition("open").On("done").GoTo("closed").
		State("stuck").
		Transition("stuck").Forbid("cancel").
		State("closed").Final().
		Initial("open").
		Quench()

	r := analysis.Analyze(m)
	if !states(r, analysis.KindDeadEnd)["stuck"] {
		t.Fatalf("'stuck' only forbids an event (no real exit) and must be a dead end; report:\n%s", r)
	}
}

// TestForbidden_DoesNotMakeTargetReachable proves the meaningless To of a
// forbidden transition does not create a false reachability edge.
func TestForbidden_DoesNotMakeTargetReachable(t *testing.T) {
	// A forbidden transition authored through JSON with a non-zero To that points at
	// an otherwise-unreachable "ghost". The forbidden edge must not make "ghost"
	// reachable.
	ir := &state.IR[string, string, any]{
		Name: "forbid-ghost", Initial: "open", HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "open", Transitions: []state.Transition[string, string, any]{
				{From: "open", On: "done", To: "closed"},
				{From: "open", On: "cancel", To: "ghost", Forbidden: true},
			}},
			{Name: "closed", IsFinal: true},
			{Name: "ghost"},
		},
	}
	m := mustProvide(t, ir)
	r := analysis.Analyze(m)
	if !states(r, analysis.KindUnreachableState)["ghost"] {
		t.Fatalf("'ghost' is only the meaningless To of a forbidden edge and must stay unreachable; report:\n%s", r)
	}
}

// TestWildcard_NondeterminismLabel proves a wildcard edge is labeled as a
// catch-all ("*") and counted as a wildcard, not as a specific zero-value event.
func TestWildcard_NotCountedAsSpecificEvent(t *testing.T) {
	// Two specific events plus one wildcard out of "open" — the wildcard is the
	// lowest-priority catch-all and is not a guardless overlap of the empty event.
	m := forge("wildcard-label").
		State("open").
		Transition("open").On("a").GoTo("closed").
		Transition("open").OnAny().GoTo("closed").
		State("closed").Final().
		Initial("open").
		Quench()

	r := analysis.Analyze(m)
	for _, f := range r.OfKind(analysis.KindNondeterministic) {
		if f.State == "open" {
			t.Fatalf("a single wildcard must not be reported nondeterministic; report:\n%s", r)
		}
	}
}

// Note: a transition to an undeclared state is a hard error at Quench, so such a
// machine never reaches Analyze through the public API. The KindUndefinedTarget
// check is defense-in-depth for any future or non-Quench IR path, and is pinned
// directly against the graph layer in undefinedtarget_internal_test.go.
