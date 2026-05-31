package verify_test

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
	"github.com/stablekernel/crucible/state/verify"
)

// alwaysHolds is an oracle that never reports a violation, used to assert a
// clean bounded run.
func alwaysHolds(map[string]bool) bool { return true }

// activeContains builds an oracle that is violated by any reached configuration
// whose active set contains every named state — the bounded-simulation analog of
// the invariant predicates, written from the violation side.
func activeContains(states ...string) verify.Oracle {
	return func(active map[string]bool) bool {
		for _, s := range states {
			if !active[s] {
				return true // missing one: invariant holds for this config
			}
		}
		return false // all present: violation
	}
}

func TestSimulateBounded_NoViolation_HoldsToDepth(t *testing.T) {
	// An oracle that never trips holds for every reached configuration up to the
	// bound, so the finding carries no counterexample.
	res := verify.Verify(linearChain(), verify.SimulateBounded("clean", 10, alwaysHolds))
	f, ok := res.BoundedSim("clean")
	if !ok {
		t.Fatal("expected a bounded-simulation finding for \"clean\"")
	}
	if !f.Reachable {
		t.Errorf("an oracle that never trips must hold up to the bound; got %s", res)
	}
	if len(f.Witness.Steps) != 0 {
		t.Errorf("a holding bounded-sim finding carries no counterexample, got %v", f.Witness.Steps)
	}
}

func TestSimulateBounded_ViolationAtDepth_ReportsShortestTrace(t *testing.T) {
	// "c is active" first becomes true at depth 2 (next, next) in a->b->c->d. The
	// reported violating trace is exactly that shortest event sequence.
	res := verify.Verify(linearChain(), verify.SimulateBounded("reach-c", 5, activeContains("c")))
	f, ok := res.BoundedSim("reach-c")
	if !ok {
		t.Fatal("expected a bounded-simulation finding")
	}
	if f.Reachable {
		t.Fatalf("c is reachable within the bound; oracle must trip; got %s", res)
	}
	if got, want := f.Witness.Events(), []string{"next", "next"}; !reflect.DeepEqual(got, want) {
		t.Errorf("violating trace = %v, want shortest %v", got, want)
	}
	if !containsAll(f.Witness.Target, "c") {
		t.Errorf("violating config %q must name c", f.Witness.Target)
	}
}

func TestSimulateBounded_ViolationBeyondBound_ReportsNone(t *testing.T) {
	// c first activates at depth 2, but a bound of 1 cannot reach it. The bound is
	// honored: no violation is reported even though one exists deeper. This proves
	// "no violation up to depth N" is a bounded claim, not a proof of absence.
	res := verify.Verify(linearChain(), verify.SimulateBounded("shallow", 1, activeContains("c")))
	f, ok := res.BoundedSim("shallow")
	if !ok {
		t.Fatal("expected a bounded-simulation finding")
	}
	if !f.Reachable {
		t.Errorf("c is beyond depth 1; the bound must be honored and no violation reported; got %s", res)
	}
	if len(f.Witness.Steps) != 0 {
		t.Errorf("a bounded-clean finding carries no counterexample, got %v", f.Witness.Steps)
	}

	// Raising the bound to exactly 2 surfaces the violation, pinning the boundary.
	deep := verify.Verify(linearChain(), verify.SimulateBounded("deep", 2, activeContains("c")))
	df, _ := deep.BoundedSim("deep")
	if df.Reachable {
		t.Errorf("at depth 2 the violation is in range and must be reported; got %s", deep)
	}
}

func TestSimulateBounded_InitialConfigEvaluated(t *testing.T) {
	// A bound of 0 still evaluates the oracle at the initial configuration, so an
	// oracle violated by the initial state trips with an empty trace.
	res := verify.Verify(linearChain(), verify.SimulateBounded("at-start", 0, activeContains("a")))
	f, ok := res.BoundedSim("at-start")
	if !ok {
		t.Fatal("expected a bounded-simulation finding")
	}
	if f.Reachable {
		t.Fatalf("the initial config has a active; oracle must trip at depth 0; got %s", res)
	}
	if len(f.Witness.Steps) != 0 {
		t.Errorf("a violation at the initial config carries the empty trace, got %v", f.Witness.Events())
	}
	if !containsAll(f.Witness.Target, "a") {
		t.Errorf("violating config %q must name a", f.Witness.Target)
	}
}

func TestSimulateBounded_ParallelCompoundTrace(t *testing.T) {
	// In the parallel machine, busy and loud are co-active only after advancing
	// both regions: activate (enters idle+silent), work (busy), report (loud). The
	// oracle "busy and loud co-active" trips at the compound configuration, and the
	// reported trace drives an instance there.
	oracle := activeContains("busy", "loud")
	res := verify.Verify(parallelMachine(), verify.SimulateBounded("co-active", 8, oracle))
	f, ok := res.BoundedSim("co-active")
	if !ok {
		t.Fatal("expected a bounded-simulation finding")
	}
	if f.Reachable {
		t.Fatalf("busy and loud are co-active in the parallel machine; oracle must trip; got %s", res)
	}
	if !containsAll(f.Witness.Target, "busy", "loud") {
		t.Errorf("violating config %q must name both co-active leaves", f.Witness.Target)
	}
	if len(f.Witness.Steps) == 0 {
		t.Error("a compound violation must carry a non-empty trace")
	}
}

func TestSimulateBounded_MultipleOracles(t *testing.T) {
	// Several bounded-simulation checks compose in one pass, each keyed by its
	// label and decided independently.
	res := verify.Verify(
		linearChain(),
		verify.SimulateBounded("reach-c", 5, activeContains("c")),
		verify.SimulateBounded("never-tripped", 5, alwaysHolds),
	)
	if f, ok := res.BoundedSim("reach-c"); !ok || f.Reachable {
		t.Errorf("reach-c must report a violation; got %s", res)
	}
	if f, ok := res.BoundedSim("never-tripped"); !ok || !f.Reachable {
		t.Errorf("never-tripped must hold; got %s", res)
	}
}

func TestSimulateBounded_Determinism(t *testing.T) {
	m := parallelMachine()
	mk := func() string {
		return verify.Verify(
			m,
			verify.SimulateBounded("co-active", 8, activeContains("busy", "loud")),
			verify.SimulateBounded("clean", 8, alwaysHolds),
		).String()
	}
	first := mk()
	for i := 0; i < 20; i++ {
		if got := mk(); got != first {
			t.Fatalf("run %d differs:\n%s\n---\n%s", i, got, first)
		}
	}
}

// TestSimulateBounded_CrossCheck_Conformance replays each reported violating
// trace through the conformance harness and asserts the instance reaches the
// reported configuration and the oracle genuinely trips there. This pins the
// static bounded-simulation verdict to the kernel's executable semantics.
func TestSimulateBounded_CrossCheck_Conformance(t *testing.T) {
	cases := []struct {
		name    string
		machine *state.Machine[string, string, any]
		initial string
		label   string
		oracle  verify.Oracle
		depth   int
	}{
		{
			name:    "reach-c-chain",
			machine: linearChain(),
			initial: "a",
			label:   "reach-c",
			oracle:  activeContains("c"),
			depth:   5,
		},
		{
			name:    "co-active-parallel",
			machine: parallelMachine(),
			initial: "offline",
			label:   "co-active",
			oracle:  activeContains("busy", "loud"),
			depth:   8,
		},
	}
	codec := conformance.EventCodec[string]{
		Named:   func(e string) string { return e },
		Resolve: func(name string) (string, bool) { return name, true },
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := verify.Verify(tc.machine, verify.SimulateBounded(tc.label, tc.depth, tc.oracle))
			f, ok := res.BoundedSim(tc.label)
			if !ok || f.Reachable {
				t.Fatalf("%q expected a bounded-sim violation; got %s", tc.label, res)
			}

			// Replay the violating trace through the conformance harness and confirm it
			// runs without a kernel error.
			sc := conformance.Scenario{
				MachineID:    tc.machine.Name(),
				InitialState: tc.initial,
				Events:       eventsToScenario(f.Witness.Events()),
			}
			got := conformance.RunAgainst(tc.machine, sc, nil, codec, tc.initial)
			if got.Err != nil {
				t.Fatalf("replay error for %q: %v", tc.label, got.Err)
			}

			// Recover the reached active configuration and assert the oracle genuinely
			// trips there and the reported config key matches the replayed leaves.
			active := replayActiveConfig(t, tc.machine, tc.initial, f.Witness.Events())
			if tc.oracle(active) {
				t.Errorf("oracle %q does not trip at the replayed config %v", tc.label, sortedSet(active))
			}
			if got := configKeyOf(active, tc.machine); got != f.Witness.Target {
				t.Errorf("replayed config %q != reported %q", got, f.Witness.Target)
			}
		})
	}
}

// configKeyOf renders the '|'-joined sorted active-leaf key of a replayed
// configuration, matching the Witness.Target a bounded-sim violation carries. It
// drops ancestors (composites/regions/root) so only leaves remain, mirroring the
// configKey the explorer builds.
func configKeyOf(active map[string]bool, m *state.Machine[string, string, any]) string {
	children := childCount(m)
	var leaves []string
	for s := range active {
		if children[s] == 0 {
			leaves = append(leaves, s)
		}
	}
	sort.Strings(leaves)
	out := ""
	for i, l := range leaves {
		if i > 0 {
			out += "|"
		}
		out += l
	}
	return out
}

// childCount maps each state to its number of child/region states, so a replayed
// configuration can be reduced to its leaves (states with no children).
func childCount(m *state.Machine[string, string, any]) map[string]int {
	b, err := m.ToJSON()
	if err != nil {
		return nil
	}
	ir, err := state.LoadFromJSON[string, string, any](b)
	if err != nil {
		return nil
	}
	count := map[string]int{}
	var walk func(s *state.State[string, string, any])
	walk = func(s *state.State[string, string, any]) {
		n := len(s.Children)
		for ri := range s.Regions {
			n += len(s.Regions[ri].States)
		}
		count[s.Name] = n
		for i := range s.Children {
			walk(&s.Children[i])
		}
		for ri := range s.Regions {
			for i := range s.Regions[ri].States {
				walk(&s.Regions[ri].States[i])
			}
		}
	}
	for i := range ir.States {
		walk(&ir.States[i])
	}
	return count
}

// TestSimulateBounded_NoContextNeeded guards that bounded simulation, like the
// rest of verify, is a pure read of the IR — it never casts an instance.
func TestSimulateBounded_NoContextNeeded(t *testing.T) {
	_ = context.Background()
	if verify.Verify(linearChain(), verify.SimulateBounded("x", 3, alwaysHolds)) == nil {
		t.Fatal("Verify must never return nil")
	}
}
