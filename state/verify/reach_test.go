package verify_test

import (
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
	"github.com/stablekernel/crucible/state/verify"
)

// branchingAvoid forks the start into two arms that converge on a shared goal:
// the "left" arm passes through a danger state, the "right" arm avoids it. Both
// arms reach goal, so "reach goal avoiding danger" is satisfiable via the clean
// right arm only.
//
//	start -open-> hazard -step-> goal
//	start -safe-> calm   -step-> goal
func branchingAvoid() *state.Machine[string, string, any] {
	return forge("branchingAvoid").
		State("start").
		Transition("start").On("open").GoTo("hazard").
		Transition("start").On("safe").GoTo("calm").
		State("hazard").
		Transition("hazard").On("step").GoTo("goal").
		State("calm").
		Transition("calm").On("step").GoTo("goal").
		State("goal").Final().
		Initial("start").
		Quench()
}

// gatedGoal forces every path to the goal through a single chokepoint, so
// "reach goal avoiding chokepoint" is unsatisfiable.
//
//	start -go-> chokepoint -go-> goal
func gatedGoal() *state.Machine[string, string, any] {
	return forge("gatedGoal").
		State("start").
		Transition("start").On("go").GoTo("chokepoint").
		State("chokepoint").
		Transition("chokepoint").On("go").GoTo("goal").
		State("goal").Final().
		Initial("start").
		Quench()
}

func TestReachAvoiding_LinearChain_OffPathAvoidIsSat(t *testing.T) {
	// Avoiding a state not on the path to the target leaves the target reachable.
	res := verify.Verify(linearChain(), verify.ReachAvoiding("c", "orphanNotHere"))
	f, ok := res.ConditionalReach("c")
	if !ok {
		t.Fatal("expected a conditional-reachability finding for c")
	}
	if !f.Reachable {
		t.Fatalf("c must be reachable while avoiding an off-path state; got %s", res)
	}
	if got, want := f.Witness.Events(), []string{"next", "next"}; !reflect.DeepEqual(got, want) {
		t.Errorf("witness events = %v, want %v", got, want)
	}
}

func TestReachAvoiding_LinearChain_MidpointAvoidIsUnsat(t *testing.T) {
	// Every path to d in a->b->c->d must pass through c, so avoiding c is unsat.
	res := verify.Verify(linearChain(), verify.ReachAvoiding("d", "c"))
	f, ok := res.ConditionalReach("d")
	if !ok {
		t.Fatal("expected a conditional-reachability finding for d")
	}
	if f.Reachable {
		t.Errorf("d must be unreachable while avoiding c (every path crosses c); got %s", res)
	}
	if len(f.Witness.Steps) != 0 {
		t.Errorf("unsatisfiable conditional reach must carry no witness, got %v", f.Witness.Steps)
	}
}

func TestReachAvoiding_AvoidingTargetItselfIsUnsat(t *testing.T) {
	// Reaching X while avoiding X is a contradiction.
	res := verify.Verify(linearChain(), verify.ReachAvoiding("c", "c"))
	f, ok := res.ConditionalReach("c")
	if !ok {
		t.Fatal("expected a finding for c")
	}
	if f.Reachable {
		t.Error("reaching c while avoiding c is unsatisfiable")
	}
}

func TestReachAvoiding_Branching_CleanBranchWins(t *testing.T) {
	// hazard is on the left arm; avoiding it forces the right (calm) arm.
	res := verify.Verify(branchingAvoid(), verify.ReachAvoiding("goal", "hazard"))
	f, ok := res.ConditionalReach("goal")
	if !ok {
		t.Fatal("expected a finding for goal")
	}
	if !f.Reachable {
		t.Fatalf("goal must be reachable via the clean arm; got %s", res)
	}
	got := f.Witness.Events()
	want := []string{"safe", "step"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("witness = %v, want the clean-arm sequence %v", got, want)
	}
	// And the witness must not visit hazard.
	for _, s := range f.Witness.States(res.Initial()) {
		if s == "hazard" {
			t.Errorf("witness visited the avoided state hazard: %v", f.Witness.States(res.Initial()))
		}
	}
}

func TestReachAvoiding_GatedGoal_Unsat(t *testing.T) {
	res := verify.Verify(gatedGoal(), verify.ReachAvoiding("goal", "chokepoint"))
	f, ok := res.ConditionalReach("goal")
	if !ok {
		t.Fatal("expected a finding for goal")
	}
	if f.Reachable {
		t.Error("goal is gated behind chokepoint; avoiding it must be unsat")
	}
}

func TestReachAvoiding_MultipleAvoidStates(t *testing.T) {
	// Avoiding both branch midpoints leaves no path to goal.
	res := verify.Verify(branchingAvoid(), verify.ReachAvoiding("goal", "hazard", "calm"))
	f, ok := res.ConditionalReach("goal")
	if !ok {
		t.Fatal("expected a finding for goal")
	}
	if f.Reachable {
		t.Error("avoiding both arms' midpoints must leave goal unreachable")
	}
}

func TestReachAvoiding_UndeclaredTarget_NoFinding(t *testing.T) {
	res := verify.Verify(linearChain(), verify.ReachAvoiding("nope", "a"))
	if _, ok := res.ConditionalReach("nope"); ok {
		t.Error("an undeclared target must yield no conditional-reachability finding")
	}
}

func TestReachAvoiding_Parallel_AvoidRegionLeafPrunesConfig(t *testing.T) {
	// In parallelMachine, entering "active" descends into both regions' initial
	// children (idle, silent). Avoiding "silent" makes the active config — and
	// therefore everything reached through it — forbidden, because silent is an
	// active leaf of the configuration. So "loud" (reached only inside active)
	// is unreachable while avoiding silent.
	res := verify.Verify(parallelMachine(), verify.ReachAvoiding("loud", "silent"))
	f, ok := res.ConditionalReach("loud")
	if !ok {
		t.Fatal("expected a finding for loud")
	}
	if f.Reachable {
		t.Errorf("loud sits in a config whose active leaf silent is avoided; must be unsat; got %s", res)
	}
}

func TestReachAvoiding_Parallel_AvoidAncestorPrunesDescendants(t *testing.T) {
	// Avoiding the superstate "active" forbids every descendant: idle, busy,
	// silent, loud are all in a config that includes the active ancestor.
	res := verify.Verify(parallelMachine(), verify.ReachAvoiding("busy", "active"))
	f, ok := res.ConditionalReach("busy")
	if !ok {
		t.Fatal("expected a finding for busy")
	}
	if f.Reachable {
		t.Error("busy is a descendant of the avoided ancestor active; must be unsat")
	}
}

func TestReachAvoiding_Parallel_OfflineReachableAvoidingActive(t *testing.T) {
	// offline is the initial state and does not include active in its config, so
	// reaching offline while avoiding active is trivially satisfiable (empty
	// witness).
	res := verify.Verify(parallelMachine(), verify.ReachAvoiding("offline", "active"))
	f, ok := res.ConditionalReach("offline")
	if !ok {
		t.Fatal("expected a finding for offline")
	}
	if !f.Reachable {
		t.Errorf("offline must be reachable while avoiding active; got %s", res)
	}
	if len(f.Witness.Steps) != 0 {
		t.Errorf("offline is the initial state; witness must be empty, got %v", f.Witness.Steps)
	}
}

func TestReachAvoiding_NoAvoidSet_EquivalentToReachable(t *testing.T) {
	// ReachAvoiding with no forbidden states is plain reachability.
	res := verify.Verify(linearChain(), verify.ReachAvoiding("d"))
	f, ok := res.ConditionalReach("d")
	if !ok {
		t.Fatal("expected a finding for d")
	}
	if !f.Reachable {
		t.Fatalf("d must be reachable with an empty avoid set; got %s", res)
	}
	if got, want := f.Witness.Events(), []string{"next", "next", "next"}; !reflect.DeepEqual(got, want) {
		t.Errorf("witness = %v, want %v", got, want)
	}
}

func TestReachAvoiding_Determinism(t *testing.T) {
	m := branchingAvoid()
	first := verify.Verify(m, verify.ReachAvoiding("goal", "hazard")).String()
	for i := 0; i < 20; i++ {
		if got := verify.Verify(m, verify.ReachAvoiding("goal", "hazard")).String(); got != first {
			t.Fatalf("run %d differs:\n%s\n---\n%s", i, got, first)
		}
	}
}

// TestReachAvoiding_CrossCheck_Conformance is the critical gate: every
// satisfiable witness, replayed through the conformance harness, must reach the
// target AND never enter any avoided state along the way.
func TestReachAvoiding_CrossCheck_Conformance(t *testing.T) {
	codec := conformance.EventCodec[string]{
		Named:   func(e string) string { return e },
		Resolve: func(name string) (string, bool) { return name, true },
	}

	cases := []struct {
		name    string
		machine *state.Machine[string, string, any]
		initial string
		target  string
		avoid   []string
	}{
		{"chain-off-path", linearChain(), "a", "c", []string{"orphanNotHere"}},
		{"branch-clean-arm", branchingAvoid(), "start", "goal", []string{"hazard"}},
		{"chain-no-avoid", linearChain(), "a", "d", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := verify.Verify(tc.machine, verify.ReachAvoiding(tc.target, tc.avoid...))
			f, ok := res.ConditionalReach(tc.target)
			if !ok || !f.Reachable {
				t.Fatalf("%q expected satisfiable", tc.target)
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
			if got.FinalState != tc.target {
				t.Errorf("witness replayed to %q, want %q", got.FinalState, tc.target)
			}
			avoid := map[string]bool{}
			for _, a := range tc.avoid {
				avoid[a] = true
			}
			// The replay must never have entered an avoided state on any step.
			for _, step := range got.Trace.Steps {
				if avoid[step.ToState] {
					t.Errorf("replay entered avoided state %q (step to %q)", step.ToState, step.ToState)
				}
				for _, entered := range step.EnteredStates {
					if avoid[entered] {
						t.Errorf("replay entered avoided state %q via EnteredStates", entered)
					}
				}
			}
		})
	}
}
