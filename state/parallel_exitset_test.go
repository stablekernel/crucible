package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// k6ctx is the value context for the K6 broadcast/exit-set tests.
type k6ctx struct{ N int }

// k6note returns an action emitting a fixed string effect, so the exit/entry
// action order is observable in FireResult.Effects.
func k6note(s string) state.ActionFn[k6ctx] {
	return func(state.ActionCtx[k6ctx]) (state.Effect, error) { return s, nil }
}

// TestCrossCutExit_RunsAllRegionLeafExitActions pins the K6/T3 fix: a
// cross-cutting transition OUT of a parallel state runs the OnExit actions of
// every active region leaf, not just the parallel state's own OnExit.
func TestCrossCutExit_RunsAllRegionLeafExitActions(t *testing.T) {
	m := state.Forge[string, string, k6ctx]("t3").
		Action("exitA", k6note("exitA")).
		Action("exitB", k6note("exitB")).
		Action("exitPar", k6note("exitPar")).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").OnExit("exitPar").
		Transition("par").On("abort").GoTo("done").
		Region("a").Initial("aIdle").SubState("aIdle").OnExit("exitA").EndRegion().
		Region("b").Initial("bIdle").SubState("bIdle").OnExit("exitB").EndRegion().
		EndSuperState().
		State("done").Final().
		Initial("off").
		CurrentStateFn(func(k6ctx) string { return "off" }).
		Quench()
	inst := m.Cast(k6ctx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("setup: %v", res.Err)
	}

	res := inst.Fire(ctx, "abort")
	if res.Err != nil {
		t.Fatalf("abort: %v", res.Err)
	}
	var sawA, sawB, sawPar bool
	for _, e := range res.Effects {
		switch e {
		case "exitA":
			sawA = true
		case "exitB":
			sawB = true
		case "exitPar":
			sawPar = true
		}
	}
	if !sawA || !sawB || !sawPar {
		t.Fatalf("exit effects = %v; want exitA, exitB, and exitPar all present", res.Effects)
	}
}

// TestCrossCutExit_ExitActionOrder pins the K6/T3 exit-action ORDER contract:
// region leaves' OnExit actions run innermost-leaf-first within each region,
// regions in declaration order, and the parallel state's own OnExit after its
// children. The machine nests a compound inside region "a" so the
// innermost-first ordering within a region is observable.
func TestCrossCutExit_ExitActionOrder(t *testing.T) {
	m := state.Forge[string, string, k6ctx]("t3order").
		Action("exitLeafA", k6note("exitLeafA")).
		Action("exitMidA", k6note("exitMidA")).
		Action("exitB", k6note("exitB")).
		Action("exitPar", k6note("exitPar")).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").OnExit("exitPar").
		Transition("par").On("abort").GoTo("done").
		// Region a: a compound "midA" whose initial leaf "leafA" carries OnExit, and
		// the compound itself carries OnExit, so the innermost-first order within the
		// region is leafA then midA.
		Region("a").
		Initial("midA").
		SuperState("midA").OnExit("exitMidA").
		SubState("leafA").OnExit("exitLeafA").
		Initial("leafA").
		EndSuperState().
		EndRegion().
		Region("b").Initial("bIdle").SubState("bIdle").OnExit("exitB").EndRegion().
		EndSuperState().
		State("done").Final().
		Initial("off").
		CurrentStateFn(func(k6ctx) string { return "off" }).
		Quench()
	inst := m.Cast(k6ctx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("setup: %v", res.Err)
	}

	res := inst.Fire(ctx, "abort")
	if res.Err != nil {
		t.Fatalf("abort: %v", res.Err)
	}
	var got []string
	for _, e := range res.Effects {
		if s, ok := e.(string); ok {
			got = append(got, s)
		}
	}
	// Innermost-leaf-first within region a (leafA before midA), region a before
	// region b (declaration order), parallel's own OnExit last.
	want := []string{"exitLeafA", "exitMidA", "exitB", "exitPar"}
	if len(got) != len(want) {
		t.Fatalf("exit effect order = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("exit effect order = %v; want %v", got, want)
		}
	}
}

// TestNestedParallel_OuterRegionEventDelivered pins the K6/T9 fix: with a nested
// parallel active, an event handled only by the OUTER parallel's sibling region
// is delivered (the broadcast bubbles outward through the enclosing parallel
// regions rather than stopping at the innermost active parallel).
func TestNestedParallel_OuterRegionEventDelivered(t *testing.T) {
	m := state.Forge[string, string, k6ctx]("t9").
		State("off").
		Transition("off").On("go").GoTo("P").
		SuperState("P").
		Region("r1").
		Initial("Q").
		SuperState("Q").
		Region("q1").Initial("x1").SubState("x1").EndRegion().
		Region("q2").Initial("x2").SubState("x2").EndRegion().
		EndSuperState().
		EndRegion().
		Region("r2").
		Initial("y1").
		SubState("y1").
		SubState("y2").
		EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(k6ctx) string { return "off" }).
		Transition("y1").On("evt").GoTo("y2").
		Quench()
	inst := m.Cast(k6ctx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("setup: %v", res.Err)
	}

	res := inst.Fire(ctx, "evt")
	if res.Err != nil {
		t.Fatalf("evt: %v", res.Err)
	}
	cfg := inst.Configuration()
	var hasY1, hasY2, hasX1, hasX2 bool
	for _, l := range cfg {
		switch l {
		case "y1":
			hasY1 = true
		case "y2":
			hasY2 = true
		case "x1":
			hasX1 = true
		case "x2":
			hasX2 = true
		}
	}
	if hasY1 || !hasY2 {
		t.Fatalf("config = %v; want y1 advanced to y2", cfg)
	}
	// The inner parallel's leaves must be undisturbed.
	if !hasX1 || !hasX2 {
		t.Fatalf("config = %v; want inner parallel leaves x1 and x2 preserved", cfg)
	}
}
