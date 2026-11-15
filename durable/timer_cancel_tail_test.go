package durable_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// cancelableTimerMachine arms an `after` timer on entering waiting, and `leave` exits
// waiting, which cancels it. left also takes elapsed, so a timer that wrongly stays
// armed shows when it fires.
func cancelableTimerMachine() *state.Machine[string, string, *timerCtx] {
	return state.ForgeFor[*timerCtx]("cancelableTimer").
		Action("markFired", func(c state.ActionCtx[*timerCtx]) (state.Effect, error) {
			c.Entity.Fired++
			return nil, nil
		}).
		State("idle").
		State("waiting").
		State("left").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("arm").GoTo("waiting").
		Transition("waiting").On("poke").GoTo("waiting").
		Transition("waiting").On("leave").GoTo("left").
		Transition("waiting").After(10 * time.Second).On("elapsed").GoTo("done").Do("markFired").
		// A timer left armed past leave would fire elapsed here, where it is visible.
		Transition("left").On("elapsed").GoTo("done").Do("markFired").
		Quench()
}

// TestRecover_KeepsATailCancelOfACheckpointTimer pins that a timer pending at a
// checkpoint and canceled by a later step stays canceled after Recover. The tail's
// cancel replays before Recover re-arms the checkpoint's timers, so the re-arm must
// skip a timer the tail canceled.
func TestRecover_KeepsATailCancelOfACheckpointTimer(t *testing.T) {
	ctx := context.Background()
	m := cancelableTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("tail-cancel")
	clk := state.NewFakeClock(epoch)
	h, err := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](2)).
		Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	// arm and poke land a checkpoint while the timer is pending; leave, in the tail,
	// cancels it.
	for _, ev := range []string{"arm", "poke", "leave"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%q): %v", ev, err)
		}
	}

	recClock := state.NewFakeClock(epoch.Add(time.Hour))
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](recClock),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	recClock.Advance(11 * time.Second)
	if _, err = recovered.Tick(ctx); err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	snap := recovered.Instance().Snapshot()
	if snap.Context.Fired != 0 || snap.Current != "left" {
		t.Fatalf("after Recover the canceled timer fired: state %q, fired %d; want left, 0", snap.Current, snap.Context.Fired)
	}
}
