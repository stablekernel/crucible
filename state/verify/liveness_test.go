package verify_test

import (
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
	"github.com/stablekernel/crucible/state/verify"
)

// liveToGoal is a machine where every reachable state can still reach the goal:
// a loop (working <-> resting) whose every node retains an edge toward done.
//
//	start -begin-> working -finish-> done
//	working -rest-> resting -resume-> working
//
// From start, working, and resting, done is always eventually reachable, so
// "always eventually done" holds.
func liveToGoal() *state.Machine[string, string, any] {
	return forge("liveToGoal").
		State("start").
		Transition("start").On("begin").GoTo("working").
		State("working").
		Transition("working").On("finish").GoTo("done").
		Transition("working").On("rest").GoTo("resting").
		State("resting").
		Transition("resting").On("resume").GoTo("working").
		State("done").Final().
		Initial("start").
		Quench()
}

// trapBeforeGoal forks into a dead trap and a live arm. The trap is a non-goal
// terminal: once there, the goal can never be reached, so "always eventually
// goal" fails with the trap as the counterexample.
//
//	start -trap-> trapped (terminal, != goal)
//	start -go-> goal
func trapBeforeGoal() *state.Machine[string, string, any] {
	return forge("trapBeforeGoal").
		State("start").
		Transition("start").On("trap").GoTo("trapped").
		Transition("start").On("go").GoTo("goal").
		State("trapped").Final(). // a terminal that is not the goal
		State("goal").Final().
		Initial("start").
		Quench()
}

// zFreeCycle forks into a live arm and a sink cycle that can never escape to the
// goal. The cycle (spin <-> spinBack) has no edge toward goal, so every config in
// it violates "always eventually goal".
//
//	start -go-> goal
//	start -loop-> spin -back-> spinBack -fwd-> spin   (no exit toward goal)
func zFreeCycle() *state.Machine[string, string, any] {
	return forge("zFreeCycle").
		State("start").
		Transition("start").On("go").GoTo("goal").
		Transition("start").On("loop").GoTo("spin").
		State("spin").
		Transition("spin").On("back").GoTo("spinBack").
		State("spinBack").
		Transition("spinBack").On("fwd").GoTo("spin").
		State("goal").Final().
		Initial("start").
		Quench()
}

func TestAlwaysEventually_LiveMachine_Holds(t *testing.T) {
	res := verify.Verify(liveToGoal(), verify.AlwaysEventually("done"))
	f, ok := res.Liveness("done")
	if !ok {
		t.Fatal("expected a liveness finding for done")
	}
	if !f.Reachable {
		t.Errorf("done must be always-eventually reachable; got %s", res)
	}
	if len(f.Witness.Steps) != 0 {
		t.Errorf("a holding liveness finding carries no counterexample witness, got %v", f.Witness.Steps)
	}
}

func TestAlwaysEventually_TrapTerminal_Violation(t *testing.T) {
	res := verify.Verify(trapBeforeGoal(), verify.AlwaysEventually("goal"))
	f, ok := res.Liveness("goal")
	if !ok {
		t.Fatal("expected a liveness finding for goal")
	}
	if f.Reachable {
		t.Fatalf("the trapped terminal can never reach goal; liveness must fail; got %s", res)
	}
	// The counterexample witness points at the stuck config: trapped.
	if f.Witness.Target != "trapped" {
		t.Errorf("counterexample should point at the trap %q, got %q", "trapped", f.Witness.Target)
	}
	if got, want := f.Witness.Events(), []string{"trap"}; !reflect.DeepEqual(got, want) {
		t.Errorf("counterexample path = %v, want %v", got, want)
	}
}

func TestAlwaysEventually_ZFreeCycle_Violation(t *testing.T) {
	res := verify.Verify(zFreeCycle(), verify.AlwaysEventually("goal"))
	f, ok := res.Liveness("goal")
	if !ok {
		t.Fatal("expected a liveness finding for goal")
	}
	if f.Reachable {
		t.Fatalf("the spin cycle can never reach goal; liveness must fail; got %s", res)
	}
	// The nearest stuck config the cycle traps in is spin (one event from start).
	if f.Witness.Target != "spin" {
		t.Errorf("counterexample should point at the first stuck config %q, got %q", "spin", f.Witness.Target)
	}
	if got, want := f.Witness.Events(), []string{"loop"}; !reflect.DeepEqual(got, want) {
		t.Errorf("counterexample path = %v, want %v", got, want)
	}
}

func TestAlwaysEventually_TargetIsInitialAndAlwaysReturnable_Holds(t *testing.T) {
	// A toggle: off <-> on. From every state off is always eventually reachable.
	m := forge("toggle").
		State("off").
		Transition("off").On("flip").GoTo("on").
		State("on").
		Transition("on").On("flip").GoTo("off").
		Initial("off").
		Quench()
	res := verify.Verify(m, verify.AlwaysEventually("off"))
	f, ok := res.Liveness("off")
	if !ok {
		t.Fatal("expected a liveness finding for off")
	}
	if !f.Reachable {
		t.Errorf("off is always reachable in a toggle; liveness must hold; got %s", res)
	}
}

func TestAlwaysEventually_LinearChain_GoalIsFinal_Holds(t *testing.T) {
	// In a -> b -> c -> d(final), d is always eventually reachable from every state.
	res := verify.Verify(linearChain(), verify.AlwaysEventually("d"))
	f, ok := res.Liveness("d")
	if !ok {
		t.Fatal("expected a liveness finding for d")
	}
	if !f.Reachable {
		t.Errorf("d terminates every run; liveness must hold; got %s", res)
	}
}

func TestAlwaysEventually_UndeclaredTarget_NoFinding(t *testing.T) {
	res := verify.Verify(linearChain(), verify.AlwaysEventually("nope"))
	if _, ok := res.Liveness("nope"); ok {
		t.Error("an undeclared target must yield no liveness finding")
	}
}

func TestAlwaysEventually_UnreachableTarget_Violation(t *testing.T) {
	// orphan is unreachable; no reachable config can reach it, so "always
	// eventually orphan" fails, with the initial state as the counterexample.
	res := verify.Verify(withUnreachable(), verify.AlwaysEventually("orphan"))
	f, ok := res.Liveness("orphan")
	if !ok {
		t.Fatal("expected a liveness finding for orphan")
	}
	if f.Reachable {
		t.Error("an unreachable target can never be eventually reached; liveness must fail")
	}
}

func TestAlwaysEventually_Parallel_GoalInRegion_Holds(t *testing.T) {
	// In parallelMachine, the Exec region runs idle -work-> busy(final). busy is
	// entered by initial descent into active's Exec region (no firing edge of its
	// own), then reached via the work event. Liveness must honor that structural
	// entry: from offline, active, idle, busy is always eventually reachable, so
	// "always eventually busy" holds. A path-advancing-edge-only model would
	// wrongly call every config stuck; honoring initial descent makes it exact.
	res := verify.Verify(parallelMachine(), verify.AlwaysEventually("busy"))
	f, ok := res.Liveness("busy")
	if !ok {
		t.Fatal("expected a liveness finding for busy")
	}
	if !f.Reachable {
		t.Errorf("busy is always eventually reachable through the Exec region; liveness must hold; got %s", res)
	}
}

func TestAlwaysEventually_Parallel_OrthogonalRegionsProgressIndependently(t *testing.T) {
	// Regions are orthogonal: the Exec region parking in busy (its final) does NOT
	// block the Tele region from independently progressing silent -> loud, because
	// both leaves are active at once. Liveness honors this — being in busy keeps
	// the active superstate (and therefore the Tele region) alive, so loud is still
	// always eventually reachable. The verdict holds for both region finals.
	res := verify.Verify(parallelMachine(),
		verify.AlwaysEventually("loud"),
		verify.AlwaysEventually("busy"),
	)
	for _, target := range []string{"loud", "busy"} {
		f, ok := res.Liveness(target)
		if !ok {
			t.Fatalf("expected a liveness finding for %q", target)
		}
		if !f.Reachable {
			t.Errorf("orthogonal regions progress independently; %q must stay always eventually reachable; got %s", target, res)
		}
	}
}

func TestAlwaysEventually_Determinism(t *testing.T) {
	m := zFreeCycle()
	first := verify.Verify(m, verify.AlwaysEventually("goal")).String()
	for i := 0; i < 20; i++ {
		if got := verify.Verify(m, verify.AlwaysEventually("goal")).String(); got != first {
			t.Fatalf("run %d differs:\n%s\n---\n%s", i, got, first)
		}
	}
}

// TestAlwaysEventually_CrossCheck_Conformance proves the counterexample witness
// is a real run: replaying the path to the stuck config lands an instance in
// that config, confirming the trap is genuinely reachable.
func TestAlwaysEventually_CrossCheck_Conformance(t *testing.T) {
	codec := conformance.EventCodec[string]{
		Named:   func(e string) string { return e },
		Resolve: func(name string) (string, bool) { return name, true },
	}
	cases := []struct {
		name    string
		machine *state.Machine[string, string, any]
		initial string
		target  string
		stuck   string
	}{
		{"trap", trapBeforeGoal(), "start", "goal", "trapped"},
		{"cycle", zFreeCycle(), "start", "goal", "spin"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := verify.Verify(tc.machine, verify.AlwaysEventually(tc.target))
			f, ok := res.Liveness(tc.target)
			if !ok || f.Reachable {
				t.Fatalf("%q expected a liveness violation", tc.target)
			}
			sc := conformance.Scenario{
				MachineID:    tc.machine.Name(),
				InitialState: tc.initial,
				Events:       eventsToScenario(f.Witness.Events()),
			}
			got := conformance.RunAgainst(tc.machine, sc, nil, codec, tc.initial)
			if got.Err != nil {
				t.Fatalf("replay error: %v", got.Err)
			}
			if got.FinalState != tc.stuck {
				t.Errorf("counterexample replayed to %q, want stuck config %q", got.FinalState, tc.stuck)
			}
		})
	}
}
