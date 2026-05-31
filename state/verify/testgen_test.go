package verify_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
	"github.com/stablekernel/crucible/state/verify"
)

// cyclicMachine is a machine with a genuine cycle plus an exit: a loop
// (run <-> pause) and an edge to a terminal. A covering suite must exercise the
// back-edge of the cycle as well as the forward edges and the exit.
//
//	idle -start-> run
//	run -pause-> paused
//	paused -resume-> run
//	run -stop-> stopped (final)
func cyclicMachine() *state.Machine[string, string, any] {
	return forge("cyclic").
		State("idle").
		Transition("idle").On("start").GoTo("run").
		State("run").
		Transition("run").On("pause").GoTo("paused").
		Transition("run").On("stop").GoTo("stopped").
		State("paused").
		Transition("paused").On("resume").GoTo("run").
		State("stopped").Final().
		Initial("idle").
		Quench()
}

// singleState is a machine of one state and no transitions: the trivial case a
// covering suite handles by emitting a single empty scenario (the initial
// configuration is covered, nothing else exists to cover).
func singleState() *state.Machine[string, string, any] {
	return forge("single").
		State("only").
		Initial("only").
		Quench()
}

// terminalOnly is a machine whose initial state has no outgoing transition: a
// reachable state with no transitions to cover. The suite covers the lone state
// with one empty scenario and fires nothing.
func terminalOnly() *state.Machine[string, string, any] {
	return forge("terminal").
		State("done").Final().
		Initial("done").
		Quench()
}

// stringResolver is the identity codec used to replay generated suites of
// string events back through the conformance harness.
func stringResolver() conformance.EventCodec[string] {
	return conformance.EventCodec[string]{
		Named:   func(e string) string { return e },
		Resolve: func(name string) (string, bool) { return name, true },
	}
}

// coveringSuiteFixtures are the machines the covering-suite self-validation runs
// over: a linear chain, a fork, a parallel/compound machine, a machine with a
// genuine cycle, and one with an unreachable island. The suite must drive 100%
// coverage of the REACHABLE universe of every one — and ignore the unreachable
// island entirely.
func coveringSuiteFixtures() []struct {
	name    string
	machine *state.Machine[string, string, any]
} {
	return []struct {
		name    string
		machine *state.Machine[string, string, any]
	}{
		{"linear", linearChain()},
		{"branching", branching()},
		{"parallel", parallelMachine()},
		{"cyclic", cyclicMachine()},
		{"island", withUnreachable()},
	}
}

// TestCoveringSuite_SelfValidation is the acceptance gate: feed each generated
// suite into V6's Coverage and assert it leaves nothing reachable uncovered —
// 100% state AND transition coverage, with empty uncovered sets. This proves the
// generator actually covers the reachable space (and, for the island fixture,
// that it covers all REACHABLE elements while ignoring the unreachable orphan).
func TestCoveringSuite_SelfValidation(t *testing.T) {
	for _, fx := range coveringSuiteFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			suite := verify.CoveringSuite(fx.machine)

			rep, ok := verify.Verify(fx.machine, verify.Coverage(suite...)).Coverage()
			if !ok {
				t.Fatalf("no coverage report produced for %s", fx.name)
			}
			if len(rep.UncoveredStates) != 0 {
				t.Errorf("%s: uncovered states %v; suite should cover every reachable state", fx.name, rep.UncoveredStates)
			}
			if len(rep.UncoveredTransitions) != 0 {
				t.Errorf("%s: uncovered transitions %v; suite should cover every reachable transition", fx.name, rep.UncoveredTransitions)
			}
			if got := rep.StateCoverage(); got != 1 {
				t.Errorf("%s: state coverage = %v, want 1.0", fx.name, got)
			}
			if got := rep.TransitionCoverage(); got != 1 {
				t.Errorf("%s: transition coverage = %v, want 1.0", fx.name, got)
			}
		})
	}
}

// TestCoveringSuite_Conformance is the executable cross-check: on guard-free
// fixtures every generated scenario must be drivable end to end through a real
// instance with no illegal or no-op stall — every event in the sequence fires a
// real transition (the final state differs from the start unless the scenario is
// empty), confirming the suite is not merely structural but replayable.
func TestCoveringSuite_Conformance(t *testing.T) {
	codec := stringResolver()
	for _, fx := range coveringSuiteFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			m := fx.machine
			suite := verify.CoveringSuite(m)
			if len(suite) == 0 {
				t.Fatalf("%s: expected at least one scenario", fx.name)
			}
			initial := verify.Verify(m).Initial()
			for i, seq := range suite {
				sc := conformance.Scenario{
					MachineID:    m.Name(),
					InitialState: initial,
					Events:       eventsToScenario(seq),
				}
				got := conformance.RunAgainst(m, sc, nil, codec, initial)
				if got.Err != nil {
					t.Fatalf("%s scenario %d (%v) replay error: %v", fx.name, i, seq, got.Err)
				}
				// No no-op stall: every step must have advanced a real transition, so a
				// non-empty scenario records exactly one trace step per event.
				if len(got.Trace.Steps) != len(seq) {
					t.Errorf("%s scenario %d (%v): %d trace steps for %d events — a no-op stall",
						fx.name, i, seq, len(got.Trace.Steps), len(seq))
				}
				for j, step := range got.Trace.Steps {
					if step.Outcome != "Success" {
						t.Errorf("%s scenario %d step %d (%s): outcome %q, want a real transition",
							fx.name, i, j, step.Event, step.Outcome)
					}
				}
			}
		})
	}
}

// TestCoveringSuite_Determinism asserts repeated generations over the same
// machine yield byte-identical suites — the order-stable property the golden
// pins. Run alongside -count to catch map-iteration nondeterminism.
func TestCoveringSuite_Determinism(t *testing.T) {
	for _, fx := range coveringSuiteFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			first := verify.CoveringSuite(fx.machine)
			for i := 0; i < 20; i++ {
				got := verify.CoveringSuite(fx.machine)
				if !reflect.DeepEqual(got, first) {
					t.Fatalf("%s run %d differs from first:\n%v\n---\n%v", fx.name, i, got, first)
				}
			}
		})
	}
}

// TestGoldenCoveringSuite pins each fixture's generated suite as a golden, so a
// drift in the greedy walk or in successor ordering surfaces as a reviewable
// diff. The suite is deterministic, so the golden is stable; run with
// -update-golden to refresh after an intended change.
func TestGoldenCoveringSuite(t *testing.T) {
	dir := filepath.Join("testdata", "suite")
	if *updateGolden {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir golden dir: %v", err)
		}
	}
	for _, fx := range coveringSuiteFixtures() {
		t.Run(fx.name, func(t *testing.T) {
			got := renderSuite(verify.CoveringSuite(fx.machine))
			path := filepath.Join(dir, fx.name+".txt")
			if *updateGolden {
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (run -update-golden to create): %v", err)
			}
			if got != string(want) {
				t.Errorf("suite golden mismatch for %s; run -update-golden if intended\n got:\n%s\nwant:\n%s", fx.name, got, want)
			}
		})
	}
}

// renderSuite renders a covering suite as one scenario per line, the
// deterministic form a golden pins.
func renderSuite(suite [][]string) string {
	var b strings.Builder
	for _, seq := range suite {
		fmt.Fprintf(&b, "%v\n", seq)
	}
	return b.String()
}

// TestCoveringSuite_SingleState covers the trivial machine: one state, no
// transitions. The suite is a single empty scenario — the initial configuration
// is covered with no events to fire.
func TestCoveringSuite_SingleState(t *testing.T) {
	suite := verify.CoveringSuite(singleState())
	if !reflect.DeepEqual(suite, [][]string{{}}) {
		t.Fatalf("single-state suite = %v, want one empty scenario", suite)
	}
	rep, ok := verify.Verify(singleState(), verify.Coverage(suite...)).Coverage()
	if !ok || rep.StateCoverage() != 1 || rep.TransitionCoverage() != 1 {
		t.Errorf("single-state coverage not full: ok=%t report=%+v", ok, rep)
	}
}

// TestCoveringSuite_TerminalOnly covers a reachable terminal with no outgoing
// transition: a single empty scenario, nothing to fire.
func TestCoveringSuite_TerminalOnly(t *testing.T) {
	suite := verify.CoveringSuite(terminalOnly())
	if !reflect.DeepEqual(suite, [][]string{{}}) {
		t.Fatalf("terminal-only suite = %v, want one empty scenario", suite)
	}
}

// TestCoveringSuite_MaxLength_Honored asserts a length cap is respected: no
// generated scenario fires more events than the bound. The branching fixture's
// transitions all sit one event from the initial state, so a cap of 1 splits the
// suite into single-event scenarios yet still covers every reachable transition.
func TestCoveringSuite_MaxLength_Honored(t *testing.T) {
	m := branching() // start -left-> leftEnd, start -right-> rightEnd
	bounded := verify.CoveringSuite(m, verify.MaxScenarioLength(1))
	for i, seq := range bounded {
		if len(seq) > 1 {
			t.Errorf("scenario %d length %d exceeds cap 1: %v", i, len(seq), seq)
		}
	}
	// Every transition is one hop from start, so a length-1 cap still covers all.
	rep, ok := verify.Verify(m, verify.Coverage(bounded...)).Coverage()
	if !ok || len(rep.UncoveredTransitions) != 0 {
		t.Errorf("bounded suite left transitions uncovered: %v", rep.UncoveredTransitions)
	}
}

// TestCoveringSuite_MaxLength_Splits asserts a cap that still admits full
// coverage produces more, shorter scenarios than the unbounded run. The linear
// chain's deepest transition is three events from the initial state; a cap of 2
// cannot reach it, so the bounded suite covers strictly fewer transitions than the
// unbounded one — the documented trade-off of a tight cap (coverage of what fits).
func TestCoveringSuite_MaxLength_Splits(t *testing.T) {
	m := linearChain() // a -next-> b -next-> c -next-> d
	bounded := verify.CoveringSuite(m, verify.MaxScenarioLength(2))
	for i, seq := range bounded {
		if len(seq) > 2 {
			t.Errorf("scenario %d length %d exceeds cap 2: %v", i, len(seq), seq)
		}
	}
	// The unbounded suite reaches the full depth-3 chain; the capped one cannot, so
	// it leaves the deepest transition uncovered — a real, documented consequence.
	full, _ := verify.Verify(m, verify.Coverage(verify.CoveringSuite(m)...)).Coverage()
	if len(full.UncoveredTransitions) != 0 {
		t.Fatalf("unbounded suite should cover everything, left %v", full.UncoveredTransitions)
	}
	capped, _ := verify.Verify(m, verify.Coverage(bounded...)).Coverage()
	if len(capped.CoveredTransitions) >= len(full.CoveredTransitions) {
		t.Errorf("cap of 2 should cover fewer transitions than unbounded: capped=%v full=%v",
			capped.CoveredTransitions, full.CoveredTransitions)
	}
}
