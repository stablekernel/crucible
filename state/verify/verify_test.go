package verify_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
	"github.com/stablekernel/crucible/state/verify"
)

// forge returns a string-typed builder, matching the analysis package's test
// fixtures so the two packages exercise the same machine shapes.
func forge(name string) *state.Builder[string, string, any] {
	return state.Forge[string, string, any](name)
}

// linearChain is a 4-state pipeline: a -> b -> c -> d(final). Every state is
// reachable and the witnesses are unique shortest event sequences.
func linearChain() *state.Machine[string, string, any] {
	return forge("linear").
		State("a").
		Transition("a").On("next").GoTo("b").
		State("b").
		Transition("b").On("next").GoTo("c").
		State("c").
		Transition("c").On("next").GoTo("d").
		State("d").Final().
		Initial("a").
		Quench()
}

// branching forks from the start into two terminal arms.
func branching() *state.Machine[string, string, any] {
	return forge("branching").
		State("start").
		Transition("start").On("left").GoTo("leftEnd").
		Transition("start").On("right").GoTo("rightEnd").
		State("leftEnd").Final().
		State("rightEnd").Final().
		Initial("start").
		Quench()
}

// withUnreachable declares an island state nothing transitions to.
func withUnreachable() *state.Machine[string, string, any] {
	return forge("island").
		State("open").
		Transition("open").On("close").GoTo("closed").
		State("closed").Final().
		State("orphan"). // nothing points here
		Transition("orphan").On("reopen").GoTo("open").
		Initial("open").
		Quench()
}

// parallelMachine has an active superstate with two orthogonal regions; every
// region substate must be reachable.
func parallelMachine() *state.Machine[string, string, any] {
	return forge("parallel").
		State("offline").
		Transition("offline").On("activate").GoTo("active").
		SuperState("active").
		Region("Exec").
		Initial("idle").
		SubState("idle").On("work").GoTo("busy").
		SubState("busy").Final().
		EndRegion().
		Region("Tele").
		Initial("silent").
		SubState("silent").On("report").GoTo("loud").
		SubState("loud").Final().
		EndRegion().
		EndSuperState().
		Initial("offline").
		Quench()
}

func TestVerify_DefaultChecksEveryDeclaredState(t *testing.T) {
	res := verify.Verify(linearChain())
	for _, s := range []string{"a", "b", "c", "d"} {
		f, ok := res.For(s)
		if !ok {
			t.Fatalf("expected a finding for state %q", s)
		}
		if !f.Reachable {
			t.Errorf("state %q should be reachable", s)
		}
	}
}

func TestVerify_Reachable_WitnessPaths(t *testing.T) {
	tests := []struct {
		name    string
		machine *state.Machine[string, string, any]
		target  string
		want    []string // expected witness events
	}{
		{"chain-mid", linearChain(), "c", []string{"next", "next"}},
		{"chain-final", linearChain(), "d", []string{"next", "next", "next"}},
		{"chain-initial", linearChain(), "a", []string{}},
		{"branch-left", branching(), "leftEnd", []string{"left"}},
		{"branch-right", branching(), "rightEnd", []string{"right"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := verify.Verify(tt.machine, verify.Reachable(tt.target))
			f, ok := res.For(tt.target)
			if !ok {
				t.Fatalf("no finding for %q", tt.target)
			}
			if !f.Reachable {
				t.Fatalf("state %q should be reachable", tt.target)
			}
			if got := f.Witness.Events(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("witness events = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerify_Reachable_True(t *testing.T) {
	if !verify.Verify(linearChain(), verify.Reachable("d")).CanReach("d") {
		t.Fatal("d must be reachable")
	}
}

func TestVerify_Unreachable_NoWitness(t *testing.T) {
	res := verify.Verify(withUnreachable(), verify.Reachable("orphan"))
	f, ok := res.For("orphan")
	if !ok {
		t.Fatal("expected a finding for orphan")
	}
	if f.Reachable {
		t.Fatal("orphan must be unreachable")
	}
	if len(f.Witness.Steps) != 0 {
		t.Errorf("unreachable state must carry no witness, got %v", f.Witness.Steps)
	}
	if res.CanReach("orphan") {
		t.Error("CanReach must be false for orphan")
	}
}

func TestVerify_Unreachable_ListsAllUnreachable(t *testing.T) {
	res := verify.Verify(withUnreachable())
	un := res.Unreachable()
	if !reflect.DeepEqual(un, []string{"orphan"}) {
		t.Errorf("Unreachable() = %v, want [orphan]", un)
	}
}

func TestVerify_Parallel_AllRegionStatesReachable(t *testing.T) {
	res := verify.Verify(parallelMachine())
	for _, s := range []string{"active", "idle", "busy", "silent", "loud"} {
		if !res.CanReach(s) {
			t.Errorf("parallel region state %q should be reachable; findings:\n%s", s, res)
		}
	}
}

func TestVerify_For_UnknownState(t *testing.T) {
	res := verify.Verify(linearChain(), verify.Reachable("nope"))
	if _, ok := res.For("nope"); ok {
		t.Error("For must report not-ok for a state that is not declared")
	}
}

func TestVerify_OK(t *testing.T) {
	if !verify.Verify(linearChain()).OK() {
		t.Error("a machine with no unreachable states should be OK")
	}
	if verify.Verify(withUnreachable()).OK() {
		t.Error("a machine with an unreachable state should not be OK")
	}
}

// TestVerify_CrossCheck_Conformance proves the witness verify hands back is a
// real run: replaying the witness event sequence through the conformance harness
// actually lands the instance in the target state. This ties verify's static
// claim to the kernel's executable semantics.
func TestVerify_CrossCheck_Conformance(t *testing.T) {
	m := linearChain()
	codec := conformance.EventCodec[string]{
		Named:   func(e string) string { return e },
		Resolve: func(name string) (string, bool) { return name, true },
	}

	for _, target := range []string{"b", "c", "d"} {
		f, ok := verify.Verify(m, verify.Reachable(target)).For(target)
		if !ok || !f.Reachable {
			t.Fatalf("%q expected reachable", target)
		}
		sc := conformance.Scenario{
			MachineID:    m.Name(),
			InitialState: "a",
			Events:       eventsToScenario(f.Witness.Events()),
		}
		got := conformance.RunAgainst(m, sc, nil, codec, "a")
		if got.Err != nil {
			t.Fatalf("replay error for %q: %v", target, got.Err)
		}
		if got.FinalState != target {
			t.Errorf("witness for %q replayed to %q", target, got.FinalState)
		}
	}
}

// TestVerify_CrossCheck_AllReachableHaveDrivableWitness asserts the property the
// conformance scenarios agree with: every state verify calls reachable has a
// witness that, fired through the kernel, ends in that state.
func TestVerify_CrossCheck_AllReachableHaveDrivableWitness(t *testing.T) {
	m := branching()
	codec := conformance.EventCodec[string]{
		Named:   func(e string) string { return e },
		Resolve: func(name string) (string, bool) { return name, true },
	}
	res := verify.Verify(m)
	for _, f := range res.Findings {
		if !f.Reachable || len(f.Witness.Steps) == 0 {
			continue // initial state has the empty witness
		}
		sc := conformance.Scenario{
			MachineID:    m.Name(),
			InitialState: "start",
			Events:       eventsToScenario(f.Witness.Events()),
		}
		got := conformance.RunAgainst(m, sc, nil, codec, "start")
		if got.Err != nil || got.FinalState != f.State {
			t.Errorf("witness for %q replayed to %q (err=%v)", f.State, got.FinalState, got.Err)
		}
	}
}

func eventsToScenario(events []string) []conformance.Event {
	out := make([]conformance.Event, 0, len(events))
	for _, e := range events {
		out = append(out, conformance.Event{Event: e})
	}
	return out
}

// TestVerify_Determinism asserts that repeated runs over the same machine yield
// byte-identical reports — the order-stable property the golden test pins.
func TestVerify_Determinism(t *testing.T) {
	m := parallelMachine()
	first := verify.Verify(m).String()
	for i := 0; i < 20; i++ {
		if got := verify.Verify(m).String(); got != first {
			t.Fatalf("run %d differs from first:\n%s\n---\n%s", i, got, first)
		}
	}
}

// Ensure the package does not accidentally depend on a live context: Verify
// must be a pure read of the IR.
func TestVerify_NoContextNeeded(t *testing.T) {
	_ = context.Background()
	if verify.Verify(linearChain()) == nil {
		t.Fatal("Verify must never return nil")
	}
}
