package verify_test

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/verify"
)

// coactiveParallel reaches a configuration where two orthogonal region leaves
// are active at once. From offline, activate descends into both regions
// (Exec=idle, Tele=silent); work advances Exec to busy and report advances Tele
// to loud, so the configuration [busy, loud] is reachable — busy and loud are
// co-active. It is the fixture mutual exclusion must be able to refute.
func coactiveParallel() *state.Machine[string, string, any] {
	return parallelMachine()
}

func TestMutualExclusion_FlatMachine_Holds(t *testing.T) {
	// In a linear a->b->c->d chain only one state is ever active, so any two
	// distinct states are trivially mutually exclusive.
	res := verify.Verify(linearChain(), verify.CheckInvariant(verify.MutualExclusion("b", "c")))
	f, ok := res.Invariant(verify.MutualExclusion("b", "c").Label())
	if !ok {
		t.Fatal("expected an invariant finding")
	}
	if !f.Reachable {
		t.Errorf("b and c are never co-active in a flat chain; invariant must hold; got %s", res)
	}
	if len(f.Witness.Steps) != 0 {
		t.Errorf("a holding invariant carries no counterexample, got %v", f.Witness.Steps)
	}
}

func TestMutualExclusion_ParallelRegions_Violation(t *testing.T) {
	// busy (Exec region) and loud (Tele region) are co-active in [busy, loud], so
	// "busy and loud are never simultaneously active" is violated.
	inv := verify.MutualExclusion("busy", "loud")
	res := verify.Verify(coactiveParallel(), verify.CheckInvariant(inv))
	f, ok := res.Invariant(inv.Label())
	if !ok {
		t.Fatal("expected an invariant finding")
	}
	if f.Reachable {
		t.Fatalf("busy and loud are co-active in parallel regions; invariant must be violated; got %s", res)
	}
	// The counterexample names the violating configuration and both leaves appear.
	config := f.Witness.Target
	if !containsAll(config, "busy", "loud") {
		t.Errorf("counterexample config %q must name both co-active leaves busy and loud", config)
	}
	if len(f.Witness.Steps) == 0 {
		t.Error("a violation must carry a counterexample witness path")
	}
}

func TestImplies_Holds(t *testing.T) {
	// In parallelMachine, whenever busy is active the active superstate "active"
	// is also active (busy is a descendant of active). So "busy implies active"
	// holds in every reachable configuration.
	inv := verify.Implies("busy", "active")
	res := verify.Verify(parallelMachine(), verify.CheckInvariant(inv))
	f, ok := res.Invariant(inv.Label())
	if !ok {
		t.Fatal("expected an invariant finding")
	}
	if !f.Reachable {
		t.Errorf("busy is always nested under active; implication must hold; got %s", res)
	}
}

func TestImplies_Violation(t *testing.T) {
	// "whenever busy is active, loud is active" is false: in [busy, silent] busy
	// is active but loud is not. The counterexample names that configuration.
	inv := verify.Implies("busy", "loud")
	res := verify.Verify(parallelMachine(), verify.CheckInvariant(inv))
	f, ok := res.Invariant(inv.Label())
	if !ok {
		t.Fatal("expected an invariant finding")
	}
	if f.Reachable {
		t.Fatalf("busy can be active without loud (config [busy, silent]); implication must fail; got %s", res)
	}
	config := f.Witness.Target
	if !containsAll(config, "busy") || containsAll(config, "loud") {
		t.Errorf("counterexample config %q must have busy active and loud inactive", config)
	}
}

func TestNeverActive_Holds_Unreachable(t *testing.T) {
	// orphan is unreachable, so it is never active in any reachable config.
	inv := verify.NeverActive("orphan")
	res := verify.Verify(withUnreachable(), verify.CheckInvariant(inv))
	f, ok := res.Invariant(inv.Label())
	if !ok {
		t.Fatal("expected an invariant finding")
	}
	if !f.Reachable {
		t.Errorf("orphan is unreachable; never-active must hold; got %s", res)
	}
}

func TestNeverActive_Violation_Reachable(t *testing.T) {
	// c is reachable, so "c is never active" is violated, with the witness to the
	// config that activates c.
	inv := verify.NeverActive("c")
	res := verify.Verify(linearChain(), verify.CheckInvariant(inv))
	f, ok := res.Invariant(inv.Label())
	if !ok {
		t.Fatal("expected an invariant finding")
	}
	if f.Reachable {
		t.Fatalf("c is reachable; never-active must be violated; got %s", res)
	}
	if !containsAll(f.Witness.Target, "c") {
		t.Errorf("counterexample config %q must name c", f.Witness.Target)
	}
	if got, want := f.Witness.Events(), []string{"next", "next"}; !reflect.DeepEqual(got, want) {
		t.Errorf("witness events = %v, want %v", got, want)
	}
}

func TestNeverActive_Holds_NeverEntered_Compound(t *testing.T) {
	// A compound whose only declared-but-unreached substate never activates.
	m := forge("compoundIsland").
		State("root").
		Transition("root").On("go").GoTo("done").
		State("done").Final().
		State("island"). // unreachable composite
		Transition("island").On("x").GoTo("done").
		Initial("root").
		Quench()
	inv := verify.NeverActive("island")
	res := verify.Verify(m, verify.CheckInvariant(inv))
	f, ok := res.Invariant(inv.Label())
	if !ok {
		t.Fatal("expected an invariant finding")
	}
	if !f.Reachable {
		t.Errorf("island is never entered; never-active must hold; got %s", res)
	}
}

func TestCheckInvariant_Compound_MutualExclusionAcrossArms(t *testing.T) {
	// In a branching machine the two terminal arms are mutually exclusive: no
	// configuration activates both leftEnd and rightEnd.
	inv := verify.MutualExclusion("leftEnd", "rightEnd")
	res := verify.Verify(branching(), verify.CheckInvariant(inv))
	f, ok := res.Invariant(inv.Label())
	if !ok {
		t.Fatal("expected an invariant finding")
	}
	if !f.Reachable {
		t.Errorf("branch arms are mutually exclusive; invariant must hold; got %s", res)
	}
}

func TestCheckInvariant_MultipleInvariants_OneCall(t *testing.T) {
	// CheckInvariant accepts several invariants; each yields its own finding.
	res := verify.Verify(
		parallelMachine(),
		verify.CheckInvariant(
			verify.MutualExclusion("busy", "loud"), // violated
			verify.Implies("busy", "active"),       // holds
			verify.NeverActive("offline"),          // violated (offline is initial)
		),
	)
	want := map[string]bool{
		verify.MutualExclusion("busy", "loud").Label(): false,
		verify.Implies("busy", "active").Label():       true,
		verify.NeverActive("offline").Label():          false,
	}
	for label, holds := range want {
		f, ok := res.Invariant(label)
		if !ok {
			t.Fatalf("expected a finding for %q", label)
		}
		if f.Reachable != holds {
			t.Errorf("invariant %q: holds=%v, want %v; got %s", label, f.Reachable, holds, res)
		}
	}
}

func TestCheckInvariant_Determinism(t *testing.T) {
	m := parallelMachine()
	mk := func() string {
		return verify.Verify(
			m,
			verify.CheckInvariant(
				verify.MutualExclusion("busy", "loud"),
				verify.Implies("busy", "loud"),
			),
		).String()
	}
	first := mk()
	for i := 0; i < 20; i++ {
		if got := mk(); got != first {
			t.Fatalf("run %d differs:\n%s\n---\n%s", i, got, first)
		}
	}
}

// TestCheckInvariant_CrossCheck_Conformance replays each counterexample witness
// through the kernel and asserts the actual reached configuration genuinely
// violates the invariant predicate. This pins the static product-explorer verdict
// to the kernel's executable semantics.
func TestCheckInvariant_CrossCheck_Conformance(t *testing.T) {
	cases := []struct {
		name    string
		machine *state.Machine[string, string, any]
		initial string
		inv     verify.Invariant
		// predicate over the kernel's reached active set (leaves + ancestors).
		violated func(active map[string]bool) bool
	}{
		{
			name:    "mutex-parallel",
			machine: parallelMachine(),
			initial: "offline",
			inv:     verify.MutualExclusion("busy", "loud"),
			violated: func(a map[string]bool) bool {
				return a["busy"] && a["loud"]
			},
		},
		{
			name:    "implies-parallel",
			machine: parallelMachine(),
			initial: "offline",
			inv:     verify.Implies("busy", "loud"),
			violated: func(a map[string]bool) bool {
				return a["busy"] && !a["loud"]
			},
		},
		{
			name:    "never-active-chain",
			machine: linearChain(),
			initial: "a",
			inv:     verify.NeverActive("c"),
			violated: func(a map[string]bool) bool {
				return a["c"]
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := verify.Verify(tc.machine, verify.CheckInvariant(tc.inv))
			f, ok := res.Invariant(tc.inv.Label())
			if !ok || f.Reachable {
				t.Fatalf("%q expected an invariant violation; got %s", tc.inv.Label(), res)
			}
			active := replayActiveConfig(t, tc.machine, tc.initial, f.Witness.Events())
			if !tc.violated(active) {
				t.Errorf("replayed config %v does not violate invariant %q", sortedSet(active), tc.inv.Label())
			}
		})
	}
}

// replayActiveConfig fires the witness event sequence through a real instance and
// returns the full active configuration (active leaves plus every ancestor),
// matching the active-set semantics invariants evaluate over.
func replayActiveConfig(t *testing.T, m *state.Machine[string, string, any], initial string, events []string) map[string]bool {
	t.Helper()
	inst := m.Cast(nil, state.WithInitialState(initial))
	for _, ev := range events {
		inst.Fire(context.Background(), ev)
	}
	active := map[string]bool{}
	parent := parentChain(m)
	for _, leaf := range inst.Configuration() {
		for n := leaf; n != ""; n = parent[n] {
			active[n] = true
		}
	}
	return active
}

// parentChain reconstructs the leaf/substate -> enclosing-composite map from the
// machine IR, used to expand active leaves to their full active configuration.
func parentChain(m *state.Machine[string, string, any]) map[string]string {
	b, err := m.ToJSON()
	if err != nil {
		return nil
	}
	ir, err := state.LoadFromJSON[string, string, any](b)
	if err != nil {
		return nil
	}
	parent := map[string]string{}
	var walk func(s *state.State[string, string, any], p string)
	walk = func(s *state.State[string, string, any], p string) {
		parent[s.Name] = p
		for i := range s.Children {
			walk(&s.Children[i], s.Name)
		}
		for ri := range s.Regions {
			for i := range s.Regions[ri].States {
				walk(&s.Regions[ri].States[i], s.Name)
			}
		}
	}
	for i := range ir.States {
		walk(&ir.States[i], "")
	}
	return parent
}

func containsAll(config string, leaves ...string) bool {
	set := map[string]bool{}
	for _, p := range splitConfig(config) {
		set[p] = true
	}
	for _, l := range leaves {
		if !set[l] {
			return false
		}
	}
	return true
}

func splitConfig(config string) []string {
	var out []string
	cur := ""
	for _, r := range config {
		if r == '|' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
