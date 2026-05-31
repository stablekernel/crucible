package verify

// This file is the fidelity cross-check: an internal test that asserts the model
// verify explores agrees with the analysis package's proven reachability. verify
// builds its own searchGraph from the public IR, and its NEGATIVE verdicts
// (unsatisfiable, no-liveness, unreachable) are only as trustworthy as that model
// — and, unlike a witness, a negative cannot be replayed to confirm it. So the
// model itself must be pinned to the proven authority: verify's reachable-state
// set (from searchGraph exploration) must EQUAL analysis.Analyze's reachability
// verdict, across every fixture and a randomized sample of machines. Any
// divergence is a correctness bug in verify's model, not something to paper over.

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// analysisReachable returns the set of declared states the analysis package
// proves reachable: every declared state minus those it flags
// KindUnreachableState. analysis.reachable() is unexported, so this reconstructs
// its verdict from the public Analyze report — the authority verify's model must
// match.
func analysisReachable[S comparable, E comparable, C any](m *state.Machine[S, E, C]) map[string]bool {
	report := analysis.Analyze(m, analysis.Only(analysis.KindUnreachableState))
	unreachable := map[string]bool{}
	for _, f := range report.OfKind(analysis.KindUnreachableState) {
		unreachable[f.State] = true
	}
	g := buildSearchGraph(m)
	reach := map[string]bool{}
	for name := range g.nodes {
		if !unreachable[name] {
			reach[name] = true
		}
	}
	return reach
}

// assertModelFidelity fails the test if verify's searchGraph reachable set
// diverges from the analysis authority for a machine.
func assertModelFidelity[S comparable, E comparable, C any](t *testing.T, label string, m *state.Machine[S, E, C]) {
	t.Helper()
	want := analysisReachable(m)
	got := buildSearchGraph(m).reachableSet()
	if !sameSet(got, want) {
		t.Errorf("model fidelity divergence for %s:\n verify model reachable: %v\n analysis reachable:    %v",
			label, sortedKeys(got), sortedKeys(want))
	}
}

func TestFidelity_Fixtures_ModelMatchesAnalysis(t *testing.T) {
	cases := []struct {
		name    string
		machine *state.Machine[string, string, any]
	}{
		{"linear", fxLinear()},
		{"branching", fxBranching()},
		{"island", fxIsland()},
		{"parallel", fxParallel()},
		{"liveToGoal", fxLiveToGoal()},
		{"trapBeforeGoal", fxTrap()},
		{"zFreeCycle", fxZFreeCycle()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertModelFidelity(t, tc.name, tc.machine)
		})
	}
}

// TestFidelity_Generated_ModelMatchesAnalysis fuzzes randomized machines and
// asserts verify's model agrees with analysis on every one. A divergence here
// would mean verify's negative verdicts cannot be trusted.
func TestFidelity_Generated_ModelMatchesAnalysis(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const samples = 400
	for i := 0; i < samples; i++ {
		m := generateMachine(rng, i)
		assertModelFidelity(t, fmt.Sprintf("generated#%d", i), m)
	}
}

// generateMachine builds a small random flat machine: a handful of states, a
// random scatter of event-triggered edges, a designated initial state, and some
// states marked final. It deliberately leaves some states unreachable so the
// fidelity check exercises the unreachable verdict on both sides.
func generateMachine(rng *rand.Rand, seed int) *state.Machine[string, string, any] {
	n := 2 + rng.Intn(6) // 2..7 states
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("s%d", i)
	}
	b := state.Forge[string, string, any](fmt.Sprintf("gen%d", seed))
	for i, name := range names {
		b = b.State(name)
		// Each state gets 0..2 outgoing edges to random (distinct) targets.
		edges := rng.Intn(3)
		wrote := 0
		for e := 0; e < edges; e++ {
			to := names[rng.Intn(n)]
			if to == name {
				continue // skip trivial self-edges; they add no reachability
			}
			ev := fmt.Sprintf("e%d_%d", i, e)
			b = b.Transition(name).On(ev).GoTo(to)
			wrote++
		}
		// Mark roughly a third of edge-free states final (Quench rejects a final
		// state that declares an outgoing transition).
		if wrote == 0 && rng.Intn(3) == 0 {
			b = b.Final()
		}
	}
	b = b.Initial(names[rng.Intn(n)])
	return b.Quench()
}

// The fidelity test runs in the internal verify package, so it cannot use the
// verify_test fixtures; these mirror the same shapes the external tests exercise.

func fxForge(name string) *state.Builder[string, string, any] {
	return state.Forge[string, string, any](name)
}

func fxLinear() *state.Machine[string, string, any] {
	return fxForge("linear").
		State("a").Transition("a").On("next").GoTo("b").
		State("b").Transition("b").On("next").GoTo("c").
		State("c").Transition("c").On("next").GoTo("d").
		State("d").Final().
		Initial("a").Quench()
}

func fxBranching() *state.Machine[string, string, any] {
	return fxForge("branching").
		State("start").
		Transition("start").On("left").GoTo("leftEnd").
		Transition("start").On("right").GoTo("rightEnd").
		State("leftEnd").Final().
		State("rightEnd").Final().
		Initial("start").Quench()
}

func fxIsland() *state.Machine[string, string, any] {
	return fxForge("island").
		State("open").Transition("open").On("close").GoTo("closed").
		State("closed").Final().
		State("orphan").Transition("orphan").On("reopen").GoTo("open").
		Initial("open").Quench()
}

func fxParallel() *state.Machine[string, string, any] {
	return fxForge("parallel").
		State("offline").Transition("offline").On("activate").GoTo("active").
		SuperState("active").
		Region("Exec").Initial("idle").
		SubState("idle").On("work").GoTo("busy").
		SubState("busy").Final().
		EndRegion().
		Region("Tele").Initial("silent").
		SubState("silent").On("report").GoTo("loud").
		SubState("loud").Final().
		EndRegion().
		EndSuperState().
		Initial("offline").Quench()
}

func fxLiveToGoal() *state.Machine[string, string, any] {
	return fxForge("liveToGoal").
		State("start").Transition("start").On("begin").GoTo("working").
		State("working").
		Transition("working").On("finish").GoTo("done").
		Transition("working").On("rest").GoTo("resting").
		State("resting").Transition("resting").On("resume").GoTo("working").
		State("done").Final().
		Initial("start").Quench()
}

func fxTrap() *state.Machine[string, string, any] {
	return fxForge("trapBeforeGoal").
		State("start").
		Transition("start").On("trap").GoTo("trapped").
		Transition("start").On("go").GoTo("goal").
		State("trapped").Final().
		State("goal").Final().
		Initial("start").Quench()
}

func fxZFreeCycle() *state.Machine[string, string, any] {
	return fxForge("zFreeCycle").
		State("start").
		Transition("start").On("go").GoTo("goal").
		Transition("start").On("loop").GoTo("spin").
		State("spin").Transition("spin").On("back").GoTo("spinBack").
		State("spinBack").Transition("spinBack").On("fwd").GoTo("spin").
		State("goal").Final().
		Initial("start").Quench()
}

func sameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
