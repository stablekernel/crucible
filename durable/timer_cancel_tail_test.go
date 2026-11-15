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

// TestRecover_FiresACheckpointTimerInATailTick pins the tail-Tick case: a timer the
// checkpoint recorded fires in a tail Tick, so Recover must fire it in replay too.
// The checkpoint's timers used to be armed only after the replay walk, so the tick's
// replay fired nothing, the recovered Handle kept the pre-tick state, and the timer
// was armed a second time on top of it.
func TestRecover_FiresACheckpointTimerInATailTick(t *testing.T) {
	ctx := context.Background()
	m := cancelableTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("tail-tick")
	clk := state.NewFakeClock(epoch)
	h, err := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](2)).
		Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range []string{"arm", "poke"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(11 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	live := h.Instance().Snapshot()
	rec, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch.Add(time.Hour))),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	got := rec.Instance().Snapshot()
	if got.Current != live.Current || got.Context.Fired != live.Context.Fired {
		t.Fatalf("recovered %q fired=%d, live %q fired=%d", got.Current, got.Context.Fired, live.Current, live.Context.Fired)
	}
}

// TestRecover_ATimerFiredInATailTickDoesNotFireAgainAfterRecover pins that the
// timer a tail Tick fired stays fired after Recover: it fired once in the tick's
// replay, so the recovered Handle's own Tick must fire nothing, and the state and
// fired count must stay at the live run's.
func TestRecover_ATimerFiredInATailTickDoesNotFireAgainAfterRecover(t *testing.T) {
	ctx := context.Background()
	m := cancelableTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("tail-tick-once")
	clk := state.NewFakeClock(epoch)
	h, err := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](2)).
		Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range []string{"arm", "poke"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(11 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	live := h.Instance().Snapshot()
	recClock := state.NewFakeClock(epoch.Add(time.Hour))
	rec, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](recClock),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	for round := 1; round <= 2; round++ {
		recClock.Advance(11 * time.Second)
		results, err := rec.Tick(ctx)
		if err != nil {
			t.Fatalf("recovered Tick %d: %v", round, err)
		}
		if len(results) != 0 {
			t.Fatalf("recovered Tick %d fired %d timers, want none: the tail Tick already fired it", round, len(results))
		}
		got := rec.Instance().Snapshot()
		if got.Current != live.Current || got.Context.Fired != live.Context.Fired {
			t.Fatalf("after recovered Tick %d: %q fired=%d, live %q fired=%d", round, got.Current, got.Context.Fired, live.Current, live.Context.Fired)
		}
	}
}

// tickedMachine is cancelableTimerMachine with a plain target for its timer, so a
// Fire recorded after the tail Tick needs the post-tick state: from the pre-tick
// waiting, the kernel refuses it, and replay fails.
func tickedMachine() *state.Machine[string, string, *timerCtx] {
	return state.ForgeFor[*timerCtx]("ticked").
		Action("markFired", func(c state.ActionCtx[*timerCtx]) (state.Effect, error) {
			c.Entity.Fired++
			return nil, nil
		}).
		State("idle").
		State("waiting").
		State("ticked").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("arm").GoTo("waiting").
		Transition("waiting").On("poke").GoTo("waiting").
		Transition("waiting").After(10 * time.Second).On("elapsed").GoTo("ticked").Do("markFired").
		Transition("ticked").On("settle").GoTo("done").
		Quench()
}

// TestRecover_ReplaysATailFireAfterATailTickFiredACheckpointTimer pins that a tail
// Fire recorded after the tail Tick still replays: its transition is declared only
// in the state the tick entered, so a replay that missed the tick's timer used to
// refuse it and fail Recover.
func TestRecover_ReplaysATailFireAfterATailTickFiredACheckpointTimer(t *testing.T) {
	ctx := context.Background()
	m := tickedMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("tail-tick-fire")
	clk := state.NewFakeClock(epoch)
	h, err := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](2)).
		Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range []string{"arm", "poke"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	clk.Advance(11 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = h.Fire(ctx, "settle"); err != nil {
		t.Fatal(err)
	}
	live := h.Instance().Snapshot()
	rec, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch.Add(time.Hour))),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	got := rec.Instance().Snapshot()
	if got.Current != live.Current || got.Context.Fired != live.Context.Fired {
		t.Fatalf("recovered %q fired=%d, live %q fired=%d", got.Current, got.Context.Fired, live.Current, live.Context.Fired)
	}
}
