package verify_test

import (
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/verify"
)

// coverageMachine is a small branching pipeline used across the coverage tests:
// start forks into a paid arm (start->paid->shipped) and a canceled arm
// (start->canceled). Every state is reachable and every transition is live, so a
// scenario set's coverage is unambiguous.
func coverageMachine() *state.Machine[string, string, any] {
	return forge("coverage").
		State("start").
		Transition("start").On("pay").GoTo("paid").
		Transition("start").On("cancel").GoTo("canceled").
		State("paid").
		Transition("paid").On("ship").GoTo("shipped").
		State("shipped").Final().
		State("canceled").Final().
		Initial("start").
		Quench()
}

// TestCoverage_EmptyScenarioSet_ZeroCovered asserts that a Coverage pass with no
// scenarios still produces a report, covering only the initial configuration's
// states and no transitions, with the rest of the universe uncovered.
func TestCoverage_EmptyScenarioSet_ZeroCovered(t *testing.T) {
	res := verify.Verify(coverageMachine(), verify.Coverage[string]())
	rep, ok := res.Coverage()
	if !ok {
		t.Fatalf("expected a coverage report")
	}
	// The initial state is entered before any event, so it is covered; every other
	// reachable state and every transition is uncovered.
	if got, want := rep.CoveredStates, []string{"start"}; !reflect.DeepEqual(got, want) {
		t.Errorf("covered states = %v, want %v", got, want)
	}
	if got, want := rep.UncoveredStates, []string{"canceled", "paid", "shipped"}; !reflect.DeepEqual(got, want) {
		t.Errorf("uncovered states = %v, want %v", got, want)
	}
	if len(rep.CoveredTransitions) != 0 {
		t.Errorf("expected no covered transitions, got %v", rep.CoveredTransitions)
	}
	wantUncoveredT := []string{
		"paid -ship-> shipped",
		"start -cancel-> canceled",
		"start -pay-> paid",
	}
	if got := rep.UncoveredTransitions; !reflect.DeepEqual(got, wantUncoveredT) {
		t.Errorf("uncovered transitions = %v, want %v", got, wantUncoveredT)
	}
	if f, _ := res.For(""); f.Reachable {
		t.Fatalf("unexpected reachability finding")
	}
}

// TestCoverage_SingleHappyPath_CoversItsLine asserts that a single happy-path
// scenario covers exactly the states and transitions along it and names the
// uncovered branches.
func TestCoverage_SingleHappyPath_CoversItsLine(t *testing.T) {
	res := verify.Verify(coverageMachine(),
		verify.Coverage([]string{"pay", "ship"}))
	rep, ok := res.Coverage()
	if !ok {
		t.Fatalf("expected a coverage report")
	}
	if got, want := rep.CoveredStates, []string{"paid", "shipped", "start"}; !reflect.DeepEqual(got, want) {
		t.Errorf("covered states = %v, want %v", got, want)
	}
	if got, want := rep.UncoveredStates, []string{"canceled"}; !reflect.DeepEqual(got, want) {
		t.Errorf("uncovered states = %v, want %v", got, want)
	}
	wantCoveredT := []string{"paid -ship-> shipped", "start -pay-> paid"}
	if got := rep.CoveredTransitions; !reflect.DeepEqual(got, wantCoveredT) {
		t.Errorf("covered transitions = %v, want %v", got, wantCoveredT)
	}
	if got, want := rep.UncoveredTransitions, []string{"start -cancel-> canceled"}; !reflect.DeepEqual(got, want) {
		t.Errorf("uncovered transitions = %v, want %v", got, want)
	}
}

// TestCoverage_FullSuite_HundredPercent asserts that a scenario set exercising
// every arm reaches 100% coverage with an empty uncovered remainder, and that the
// companion finding's Reachable flag is true.
func TestCoverage_FullSuite_HundredPercent(t *testing.T) {
	res := verify.Verify(coverageMachine(),
		verify.Coverage(
			[]string{"pay", "ship"},
			[]string{"cancel"},
		))
	rep, ok := res.Coverage()
	if !ok {
		t.Fatalf("expected a coverage report")
	}
	if len(rep.UncoveredStates) != 0 {
		t.Errorf("expected no uncovered states, got %v", rep.UncoveredStates)
	}
	if len(rep.UncoveredTransitions) != 0 {
		t.Errorf("expected no uncovered transitions, got %v", rep.UncoveredTransitions)
	}
	if rep.StateCoverage() != 1 {
		t.Errorf("state coverage = %v, want 1", rep.StateCoverage())
	}
	if rep.TransitionCoverage() != 1 {
		t.Errorf("transition coverage = %v, want 1", rep.TransitionCoverage())
	}
	f, _ := findCoverageFinding(t, res)
	if !f.Reachable {
		t.Errorf("full coverage should set the finding Reachable true")
	}
}

// TestCoverage_NoOpEvent_HandledCleanly asserts that an event that fires from no
// enabled transition in the current configuration is a clean no-op: it neither
// errors nor advances coverage, and a following valid event still fires.
func TestCoverage_NoOpEvent_HandledCleanly(t *testing.T) {
	res := verify.Verify(coverageMachine(),
		// "ship" does not fire from start (a no-op); "pay" then advances; a trailing
		// "cancel" does not fire from paid (another no-op).
		verify.Coverage([]string{"ship", "pay", "cancel"}))
	rep, ok := res.Coverage()
	if !ok {
		t.Fatalf("expected a coverage report")
	}
	if got, want := rep.CoveredStates, []string{"paid", "start"}; !reflect.DeepEqual(got, want) {
		t.Errorf("covered states = %v, want %v", got, want)
	}
	if got, want := rep.CoveredTransitions, []string{"start -pay-> paid"}; !reflect.DeepEqual(got, want) {
		t.Errorf("covered transitions = %v, want %v", got, want)
	}
}

// TestCoverage_Parallel covers a machine with orthogonal regions: a scenario that
// advances both regions covers their co-active configurations' states and the
// region transitions it fires.
func TestCoverage_Parallel(t *testing.T) {
	res := verify.Verify(parallelMachine(),
		verify.Coverage([]string{"activate", "work", "report"}))
	rep, ok := res.Coverage()
	if !ok {
		t.Fatalf("expected a coverage report")
	}
	covered := toSet(rep.CoveredStates)
	for _, s := range []string{"offline", "active", "idle", "busy", "silent", "loud"} {
		if !covered[s] {
			t.Errorf("expected state %q covered; covered = %v", s, rep.CoveredStates)
		}
	}
	firedT := toSet(rep.CoveredTransitions)
	for _, tr := range []string{
		"offline -activate-> active",
		"idle -work-> busy",
		"silent -report-> loud",
	} {
		if !firedT[tr] {
			t.Errorf("expected transition %q fired; covered = %v", tr, rep.CoveredTransitions)
		}
	}
	if len(rep.UncoveredStates) != 0 {
		t.Errorf("expected full parallel state coverage, uncovered = %v", rep.UncoveredStates)
	}
	if len(rep.UncoveredTransitions) != 0 {
		t.Errorf("expected full parallel transition coverage, uncovered = %v", rep.UncoveredTransitions)
	}
}

// TestCoverage_RepeatedOption_UnionsScenarios asserts that repeated Coverage calls
// union their scenarios into one report.
func TestCoverage_RepeatedOption_UnionsScenarios(t *testing.T) {
	res := verify.Verify(coverageMachine(),
		verify.Coverage([]string{"pay", "ship"}),
		verify.Coverage([]string{"cancel"}),
	)
	rep, _ := res.Coverage()
	if len(rep.UncoveredStates) != 0 || len(rep.UncoveredTransitions) != 0 {
		t.Errorf("unioned scenarios should reach full coverage; uncovered states=%v transitions=%v",
			rep.UncoveredStates, rep.UncoveredTransitions)
	}
}

// TestCoverage_NotRequested_NoReport asserts that without a Coverage option no
// coverage report is produced.
func TestCoverage_NotRequested_NoReport(t *testing.T) {
	res := verify.Verify(coverageMachine())
	if _, ok := res.Coverage(); ok {
		t.Errorf("expected no coverage report without a Coverage option")
	}
}

// TestCoverage_Deterministic asserts the coverage report is identical across
// repeated passes, guarding the determinism the report's goldens rely on.
func TestCoverage_Deterministic(t *testing.T) {
	first := verify.Verify(coverageMachine(), verify.Coverage([]string{"pay"})).String()
	for i := 0; i < 20; i++ {
		got := verify.Verify(coverageMachine(), verify.Coverage([]string{"pay"})).String()
		if got != first {
			t.Fatalf("coverage report differs on run %d:\n%s\n---\n%s", i, got, first)
		}
	}
}

func findCoverageFinding(t *testing.T, res *verify.Result) (verify.Finding, bool) {
	t.Helper()
	for _, f := range res.Findings {
		if f.Kind == verify.KindCoverage {
			return f, true
		}
	}
	t.Fatalf("no coverage finding in %s", res)
	return verify.Finding{}, false
}

func toSet(xs []string) map[string]bool {
	out := map[string]bool{}
	for _, x := range xs {
		out[x] = true
	}
	return out
}
