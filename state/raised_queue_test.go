package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// rqCtx is a trivial context for the raised-queue regression tests.
type rqCtx struct{ N int }

// TestRaisedQueue_DoesNotLeakAcrossFires proves that internal events raised
// during a macrostep that ERRORS are not carried over into the next Fire.
//
// Machine: s1 -go-> s2 raising {r1, r2}. The drained r1 transition (s2 -r1-> s3)
// runs a failing action, so the macrostep errors before r2 is drained, leaving
// r2 queued. A failed Fire rolls the whole macrostep back, so the instance returns
// to s1 and the residual queue is discarded. A subsequent Fire("go2") that moves
// s1 -> s4 must NOT then replay the stale r2 (which would otherwise drive s4 -> s5).
func TestRaisedQueue_DoesNotLeakAcrossFires(t *testing.T) {
	boom := errors.New("boom")
	m := state.ForgeFor[rqCtx]("rq-leak").
		Action("fail", func(state.ActionCtx[rqCtx]) (state.Effect, error) { return nil, boom }).
		State("s1").
		State("s2").
		State("s3").
		State("s4").
		State("s5").
		Initial("s1").
		CurrentStateFn(func(rqCtx) string { return "s1" }).
		Transition("s1").On("go").GoTo("s2").Raise("r1", "r2").
		Transition("s2").On("r1").GoTo("s3").Do("fail").
		Transition("s1").On("go2").GoTo("s4").
		Transition("s4").On("r2").GoTo("s5").
		Quench()

	inst := m.Cast(rqCtx{}, state.WithInitialState("s1"))
	ctx := context.Background()

	res1 := inst.Fire(ctx, "go")
	if res1.Err == nil {
		t.Fatalf("expected first macrostep to error, got state %v", res1.NewState)
	}
	// The failed macrostep rolls back: the instance returns to s1, not the
	// half-advanced s3 the triggering transition reached before the action failed.
	if res1.NewState != "s1" {
		t.Fatalf("failed macrostep did not roll back: state=%v, want s1", res1.NewState)
	}

	res2 := inst.Fire(ctx, "go2")
	if res2.Err != nil {
		t.Fatalf("second Fire errored unexpectedly: %v", res2.Err)
	}
	if res2.NewState == "s5" {
		t.Fatalf("stale raised event r2 leaked into the next Fire: state=%v, want s4", res2.NewState)
	}
	if res2.NewState != "s4" {
		t.Fatalf("second Fire settled at %v, want s4", res2.NewState)
	}
}
