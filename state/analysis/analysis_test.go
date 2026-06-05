package analysis_test

import (
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// forge opens a string-typed builder for the example machines. The machines are
// neutral order/job lifecycles with no real-world coupling.
func forge(name string) *state.Builder[string, string, any] {
	return state.Forge[string, string, any](name)
}

// kinds returns the set of finding kinds present in a report.
func kinds(r analysis.Report) map[analysis.Kind]int {
	m := map[analysis.Kind]int{}
	for _, f := range r.Findings {
		m[f.Kind]++
	}
	return m
}

// states returns the set of state names flagged for a given kind.
func states(r analysis.Report, k analysis.Kind) map[string]bool {
	m := map[string]bool{}
	for _, f := range r.OfKind(k) {
		m[f.State] = true
	}
	return m
}

// cleanMachine is a fully-connected order lifecycle with a final state and no
// defects: every state is reachable, deterministic, leaves somewhere, and can
// reach the final "closed" state.
func cleanMachine(t *testing.T) *state.Machine[string, string, any] {
	t.Helper()
	return forge("clean").
		State("open").
		Transition("open").On("pay").GoTo("paid").
		State("paid").
		Transition("paid").On("ship").GoTo("shipped").
		State("shipped").
		Transition("shipped").On("deliver").GoTo("closed").
		State("closed").Final().
		Initial("open").
		Quench()
}

func TestCleanMachine_EmptyReport(t *testing.T) {
	r := analysis.Analyze(cleanMachine(t))
	if !r.Empty() {
		t.Fatalf("expected empty report for a clean machine, got:\n%s", r)
	}
	if r.HasErrors() {
		t.Fatalf("clean machine should have no errors")
	}
}

func TestUnreachableState(t *testing.T) {
	// "orphan" is declared (target of a transition out of itself) but nothing
	// reaches it from the initial state.
	m := forge("unreachable").
		State("start").
		Transition("start").On("go").GoTo("middle").
		State("middle").
		Transition("middle").On("done").GoTo("end").
		State("end").Final().
		State("orphan").
		Transition("orphan").On("loop").GoTo("orphan").
		Initial("start").
		Quench()

	r := analysis.Analyze(m)
	if !states(r, analysis.KindUnreachableState)["orphan"] {
		t.Fatalf("expected 'orphan' reported unreachable; report:\n%s", r)
	}
	if states(r, analysis.KindUnreachableState)["start"] {
		t.Fatalf("initial state must never be unreachable")
	}
	// The unreachable finding is an error (IR-proven).
	for _, f := range r.OfKind(analysis.KindUnreachableState) {
		if f.Severity != analysis.SeverityError {
			t.Fatalf("unreachable finding should be an error, got %s", f.Severity)
		}
	}
}

// collKey is a state type whose String renders only its Name, so two distinct
// keys with the same Name collide when the analysis graph flattens them by their
// rendered name. It models the real hazard: distinct typed states (here in
// separate branches) that are indistinguishable once stringified.
type collKey struct {
	ID   int
	Name string
}

func (c collKey) String() string { return c.Name }

// TestDuplicateState_CollisionIsReported proves that two distinct states whose
// rendered names collide are surfaced as a duplicate_state error rather than
// being silently merged into one analysis node.
func TestDuplicateState_CollisionIsReported(t *testing.T) {
	a := collKey{ID: 1, Name: "dup"}
	b := collKey{ID: 2, Name: "dup"}
	final := collKey{ID: 3, Name: "done"}

	m := state.Forge[collKey, string, any]("coll").
		State(a).
		Transition(a).On("go").GoTo(final).
		State(b).
		Transition(b).On("go").GoTo(final).
		State(final).Final().
		Initial(a).
		Quench()

	r := analysis.Analyze(m)
	dups := r.OfKind(analysis.KindDuplicateState)
	if len(dups) == 0 {
		t.Fatalf("expected a duplicate_state finding for the colliding name; report:\n%s", r)
	}
	for _, f := range dups {
		if f.State != "dup" {
			t.Fatalf("duplicate_state finding state = %q, want dup", f.State)
		}
		if f.Severity != analysis.SeverityError {
			t.Fatalf("duplicate_state finding should be an error, got %s", f.Severity)
		}
	}

	// Without restricts the pass: excluding the kind suppresses the finding.
	rr := analysis.Analyze(m, analysis.Without(analysis.KindDuplicateState))
	if len(rr.OfKind(analysis.KindDuplicateState)) != 0 {
		t.Fatalf("Without(KindDuplicateState) should suppress the finding; report:\n%s", rr)
	}
}

func TestDeadTransition(t *testing.T) {
	// The self-loop on the unreachable "orphan" can never fire.
	m := forge("dead-transition").
		State("start").
		Transition("start").On("go").GoTo("end").
		State("end").Final().
		State("orphan").
		Transition("orphan").On("loop").GoTo("end").
		Initial("start").
		Quench()

	r := analysis.Analyze(m)
	found := false
	for _, f := range r.OfKind(analysis.KindDeadTransition) {
		if f.State == "orphan" && strings.Contains(f.Transition, "orphan") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a dead transition out of unreachable 'orphan'; report:\n%s", r)
	}
}

func TestNondeterminism_SameEvent(t *testing.T) {
	// Two guardless transitions from "open" on the same event "pay" — ambiguous.
	// Quench emits a warning for this but does not panic (it is not an error).
	m := forge("nondeterministic").
		State("open").
		Transition("open").On("pay").GoTo("a").
		Transition("open").On("pay").GoTo("b").
		State("a").Final().
		State("b").Final().
		Initial("open").
		Quench()

	r := analysis.Analyze(m)
	got := r.OfKind(analysis.KindNondeterministic)
	if len(got) == 0 {
		t.Fatalf("expected a nondeterminism finding for two guardless 'pay' transitions; report:\n%s", r)
	}
	if got[0].State != "open" {
		t.Fatalf("expected nondeterminism on 'open', got %q", got[0].State)
	}
	if !strings.Contains(got[0].Message, "pay") {
		t.Fatalf("message should name the ambiguous event; got %q", got[0].Message)
	}
}

func TestNondeterminism_GuardedOverlapNotReported(t *testing.T) {
	reg := forgeGuard(t)
	m := reg.
		State("open").
		Transition("open").On("pay").When("hasFunds").GoTo("a").
		Transition("open").On("pay").When("isVIP").GoTo("b").
		State("a").Final().
		State("b").Final().
		Initial("open").
		Quench()

	r := analysis.Analyze(m)
	if got := r.OfKind(analysis.KindNondeterministic); len(got) != 0 {
		t.Fatalf("guarded overlap must not be reported as nondeterministic; got:\n%s", r)
	}
}

// mustProvide binds a hand-authored IR against an empty registry and freezes it,
// modeling the JSON/UI front-end (LoadFromJSON+Provide). A missing CurrentStateFn
// is only a warning, so non-strict Quench succeeds — the analysis never casts an
// instance, so it needs neither a CurrentStateFn nor an entity.
func mustProvide(t *testing.T, ir *state.IR[string, string, any]) *state.Machine[string, string, any] {
	t.Helper()
	return ir.Provide(state.NewRegistry[any]()).Quench()
}

// forgeGuard builds a machine carrying two named guards so guarded-overlap tests
// can attach them.
func forgeGuard(t *testing.T) *state.Builder[string, string, any] {
	t.Helper()
	return forge("guarded").
		Guard("hasFunds", func(state.GuardCtx[any]) bool { return true }).
		Guard("isVIP", func(state.GuardCtx[any]) bool { return false })
}

func TestNondeterminism_AlwaysTransitions(t *testing.T) {
	// Two guardless eventless ("always") transitions out of "decide". The DSL has
	// no eventless builder verb, so this defect is authored through the JSON IR
	// path — exercising that JSON-loaded machines analyze identically.
	ir := &state.IR[string, string, any]{
		Name: "always", Initial: "decide", HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "decide", Transitions: []state.Transition[string, string, any]{
				{From: "decide", To: "a", EventLess: true},
				{From: "decide", To: "b", EventLess: true},
			}},
			{Name: "a", IsFinal: true},
			{Name: "b", IsFinal: true},
		},
	}
	m := mustProvide(t, ir)
	r := analysis.Analyze(m)
	got := r.OfKind(analysis.KindNondeterministic)
	if len(got) == 0 {
		t.Fatalf("expected nondeterminism for two always transitions; report:\n%s", r)
	}
	if !strings.Contains(got[0].Message, "always") && !strings.Contains(got[0].Message, "eventless") {
		t.Fatalf("message should mention always/eventless; got %q", got[0].Message)
	}
}

func TestDeadEnd(t *testing.T) {
	// "stuck" is non-final and has no outgoing transitions.
	m := forge("dead-end").
		State("open").
		Transition("open").On("a").GoTo("stuck").
		Transition("open").On("b").GoTo("closed").
		State("stuck").
		State("closed").Final().
		Initial("open").
		Quench()

	r := analysis.Analyze(m)
	if !states(r, analysis.KindDeadEnd)["stuck"] {
		t.Fatalf("expected 'stuck' reported as a dead end; report:\n%s", r)
	}
	// Final states must not be reported as dead ends.
	if states(r, analysis.KindDeadEnd)["closed"] {
		t.Fatalf("final state 'closed' must not be a dead end")
	}
	for _, f := range r.OfKind(analysis.KindDeadEnd) {
		if f.Severity != analysis.SeverityWarning {
			t.Fatalf("dead-end finding should be a warning, got %s", f.Severity)
		}
	}
}

func TestCannotReachFinal(t *testing.T) {
	// "trap" is reachable and leaves (self-loop) but can never reach the final
	// "closed" state.
	m := forge("liveness").
		State("open").
		Transition("open").On("trap").GoTo("trap").
		Transition("open").On("finish").GoTo("closed").
		State("trap").
		Transition("trap").On("spin").GoTo("trap").
		State("closed").Final().
		Initial("open").
		Quench()

	r := analysis.Analyze(m)
	if !states(r, analysis.KindCannotReachFinal)["trap"] {
		t.Fatalf("expected 'trap' reported as cannot-reach-final; report:\n%s", r)
	}
	// "open" reaches "closed", so it must not be flagged.
	if states(r, analysis.KindCannotReachFinal)["open"] {
		t.Fatalf("'open' can reach final and must not be flagged")
	}
}

func TestLiveness_SkippedWithoutFinalStates(t *testing.T) {
	// No final states: liveness is undefined and must be skipped gracefully.
	m := forge("no-final").
		State("a").
		Transition("a").On("x").GoTo("b").
		State("b").
		Transition("b").On("y").GoTo("a").
		Initial("a").
		Quench()

	r := analysis.Analyze(m)
	if got := r.OfKind(analysis.KindCannotReachFinal); len(got) != 0 {
		t.Fatalf("liveness must be skipped when no final state exists; got:\n%s", r)
	}
}

func TestOptions_Only(t *testing.T) {
	m := forge("opts").
		State("open").
		Transition("open").On("a").GoTo("stuck").
		Transition("open").On("b").GoTo("closed").
		State("stuck").
		State("closed").Final().
		State("orphan").
		Transition("orphan").On("loop").GoTo("closed").
		Initial("open").
		Quench()

	// Only dead-end: the orphan/unreachable findings must be suppressed.
	r := analysis.Analyze(m, analysis.Only(analysis.KindDeadEnd))
	if k := kinds(r); len(k) != 1 || k[analysis.KindDeadEnd] == 0 {
		t.Fatalf("Only(DeadEnd) should yield only dead-end findings; got %v", k)
	}
}

func TestOptions_Without(t *testing.T) {
	m := forge("opts2").
		State("open").
		Transition("open").On("a").GoTo("stuck").
		Transition("open").On("b").GoTo("closed").
		State("stuck").
		State("closed").Final().
		Initial("open").
		Quench()

	r := analysis.Analyze(m, analysis.Without(analysis.KindDeadEnd))
	if kinds(r)[analysis.KindDeadEnd] != 0 {
		t.Fatalf("Without(DeadEnd) should suppress dead-end findings; got:\n%s", r)
	}
}

func TestReport_StringAndHelpers(t *testing.T) {
	empty := analysis.Report{}
	if empty.String() != "no findings" {
		t.Fatalf("empty report should render 'no findings', got %q", empty.String())
	}
	if !empty.Empty() {
		t.Fatalf("zero report should be Empty()")
	}

	m := forge("strtest").
		State("open").
		Transition("open").On("a").GoTo("stuck").
		Transition("open").On("b").GoTo("closed").
		State("stuck").
		State("closed").Final().
		Initial("open").
		Quench()
	r := analysis.Analyze(m)
	if r.String() == "no findings" {
		t.Fatalf("expected findings rendered, got 'no findings'")
	}
	if !strings.Contains(r.String(), "dead_end") {
		t.Fatalf("rendered report should mention the kind; got:\n%s", r.String())
	}
}
