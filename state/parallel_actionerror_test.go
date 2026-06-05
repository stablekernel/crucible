package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// regionActionErrorMachine builds a parallel machine whose "work" region has a
// transition wIdle->wArmed on "arm" that runs a failing transition effect, plus
// variants that fail on an exited substate's OnExit action and on an entered
// substate's OnEntry action. The "side" region holds a single flat leaf so the
// parallel state stays active while "work" advances.
func regionActionErrorMachine(boom error, on string) *state.Machine[string, string, *trec] {
	f := state.Forge[string, string, *trec]("region-action-error").
		Action("fail", func(state.ActionCtx[*trec]) (state.Effect, error) {
			return nil, boom
		}).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("work").
		Initial("wIdle").
		SubState("wIdle")

	// The exited leaf's OnExit action fails.
	if on == "exit" {
		f = f.OnExit("fail")
	}
	f = f.SubState("wArmed")
	// The entered leaf's OnEntry action fails.
	if on == "entry" {
		f = f.OnEntry("fail")
	}

	f = f.EndRegion().
		Region("side").
		Initial("sIdle").
		SubState("sIdle").
		EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(*trec) string { return "off" })

	tr := f.Transition("wIdle").On("arm").GoTo("wArmed")
	// The transition effect itself fails.
	if on == "transition" {
		tr = tr.Do("fail")
	}
	return tr.Quench()
}

// TestRegion_TransitionActionError_PropagatesEffectError proves a failing action
// on a region-internal transition surfaces a typed *ActionFailedError and
// records OutcomeEffectError, consistent with the flat/compound commit path,
// instead of being silently discarded.
func TestRegion_TransitionActionError_PropagatesEffectError(t *testing.T) {
	for _, tc := range []struct {
		name string
		on   string
	}{
		{"transition-effect", "transition"},
		{"exit-action", "exit"},
		{"entry-action", "entry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			boom := errors.New("boom")
			m := regionActionErrorMachine(boom, tc.on)
			inst := m.Cast(&trec{}, state.WithInitialState("off"))
			ctx := context.Background()

			// Enter the parallel state cleanly.
			if res := inst.Fire(ctx, "go"); res.Err != nil {
				t.Fatalf("entering parallel state: %v", res.Err)
			}

			res := inst.Fire(ctx, "arm")
			var af *state.ActionFailedError
			if !errors.As(res.Err, &af) {
				t.Fatalf("err = %v, want *ActionFailedError", res.Err)
			}
			if !errors.Is(res.Err, boom) {
				t.Fatalf("err does not unwrap to boom: %v", res.Err)
			}
			if res.Trace.Outcome != state.OutcomeEffectError {
				t.Fatalf("outcome = %v, want OutcomeEffectError", res.Trace.Outcome)
			}
		})
	}
}

// TestRegion_MultiRegionActionError_AggregatesAndClassifies proves that when two
// orthogonal regions both fail an action on the same event, the failures are
// aggregated into a *MultiRegionError whose Unwrap exposes each region's typed
// *ActionFailedError and the outcome is classified OutcomeEffectError.
func TestRegion_MultiRegionActionError_AggregatesAndClassifies(t *testing.T) {
	boom := errors.New("boom")
	m := state.Forge[string, string, *trec]("region-multi-error").
		Action("fail", func(state.ActionCtx[*trec]) (state.Effect, error) {
			return nil, boom
		}).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").
		Initial("aIdle").
		SubState("aIdle").
		SubState("aNext").
		EndRegion().
		Region("b").
		Initial("bIdle").
		SubState("bIdle").
		SubState("bNext").
		EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(*trec) string { return "off" }).
		Transition("aIdle").On("step").GoTo("aNext").Do("fail").
		Transition("bIdle").On("step").GoTo("bNext").Do("fail").
		Quench()

	inst := m.Cast(&trec{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel state: %v", res.Err)
	}

	res := inst.Fire(ctx, "step")
	var mre *state.MultiRegionError
	if !errors.As(res.Err, &mre) {
		t.Fatalf("err = %v, want *MultiRegionError", res.Err)
	}
	if len(mre.Errors) != 2 {
		t.Fatalf("MultiRegionError.Errors = %d, want 2", len(mre.Errors))
	}
	var af *state.ActionFailedError
	if !errors.As(res.Err, &af) {
		t.Fatalf("MultiRegionError does not unwrap to *ActionFailedError: %v", res.Err)
	}
	if res.Trace.Outcome != state.OutcomeEffectError {
		t.Fatalf("outcome = %v, want OutcomeEffectError", res.Trace.Outcome)
	}
}
