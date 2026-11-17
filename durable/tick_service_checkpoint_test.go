package durable_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests pin what a Tick does to a service in flight: a timer transition hands its
// effects to the service runner, as a Fire does, so a timer that leaves the state that
// declares the service stops it, and one that re-enters the state starts a fresh
// activation. The live Handle, one recovered from the journal and one recovered from a
// checkpoint agree.

var timerSvc = state.InvokeID("tsvc", "loading", 0)

// timerSvcMachine invokes fn in "loading", which "begin" enters from "prep", itself
// entered from "idle" by "prep", so a run's Tick can land on a checkpoint. Its after
// timer leaves for "timedOut", or with reenter re-enters "loading".
func timerSvcMachine(fn state.ServiceFn[svcCtx], reenter bool) *state.Machine[string, string, svcCtx] {
	f := state.ForgeFor[svcCtx]("tsvc").
		Service("fetch", fn).
		State("idle").
		State("prep").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done")).
		State("ready").Final().
		State("timedOut").
		Initial("idle").
		Transition("idle").On("prep").GoTo("prep").
		Transition("prep").On("begin").GoTo("loading").
		Transition("loading").On("done").GoTo("ready")
	if reenter {
		f = f.Transition("loading").After(5 * time.Second).On("redo").GoTo("loading").Reenter()
	} else {
		f = f.Transition("loading").After(5 * time.Second).On("timeout").GoTo("timedOut")
	}
	return f.Quench()
}

// timerSvcRun starts an instance of timerSvcMachine, fires prep and begin, so loading's
// service is in flight, and Ticks its after timer, failing unless the timer fired.
// Every third step checkpoints, so the Tick's Record carries a checkpoint and anything
// recorded after it follows the checkpoint.
func timerSvcRun(t *testing.T, reenter bool) (*durable.Handle[string, string, svcCtx], *state.Machine[string, string, svcCtx], *durable.MemStore, []durable.Option[string, string, svcCtx], *int64) {
	t.Helper()
	ctx := context.Background()
	runs := new(int64)
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		return atomic.AddInt64(runs, 1), nil
	}
	m := timerSvcMachine(fn, reenter)
	clk := state.NewFakeClock(epoch)
	store := durable.NewMemStore()
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
		durable.WithCheckpointEvery[string, string, svcCtx](3),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, "t-1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"prep", "begin"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire %s: %v", ev, err)
		}
	}
	clk.Advance(6 * time.Second)
	if results, terr := h.Tick(ctx); terr != nil || len(results) != 1 {
		t.Fatalf("Tick = %d results, %v; want the timer to fire", len(results), terr)
	}
	if ckpt, recs, lerr := store.Load(ctx, "t-1"); lerr != nil || len(ckpt) == 0 || len(recs) != 0 {
		t.Fatalf("after the Tick the store holds a %d-byte checkpoint and %d Records (%v); want the Tick's checkpoint and nothing after it", len(ckpt), len(recs), lerr)
	}
	return h, m, store, opts, runs
}

// TestTimerLeavingTheState_SettleAfterCheckpointStaysRecoverable: loading's service is
// in flight, its after timer leaves for timedOut, the Tick's Record carries a
// checkpoint, which holds only the services of the states the instance is in, and the
// live Handle then calls RunService. While Tick left the service in flight, that ran it
// and journaled its settle after the checkpoint, which replay couldn't apply ("no
// in-flight service to settle"), so every Recover failed. Now the timer stopped the
// service: RunService finds nothing in flight, records nothing, and every Recover
// succeeds.
func TestTimerLeavingTheState_SettleAfterCheckpointStaysRecoverable(t *testing.T) {
	ctx := context.Background()
	h, m, store, opts, runs := timerSvcRun(t, false)
	_, ran, rerr := h.RunService(ctx, timerSvc)
	for i := range 2 {
		rh, err := durable.Recover(ctx, m, store, "t-1", opts...)
		if err != nil {
			t.Fatalf("Recover %d: %v; want the instance recoverable", i, err)
		}
		if got := rh.Instance().Current(); got != "timedOut" {
			t.Fatalf("Recover %d in %q, want timedOut", i, got)
		}
		if _, hran, herr := rh.RunService(ctx, timerSvc); hran || herr != nil {
			t.Fatalf("RunService after Recover %d = %v, %v; want nothing in flight", i, hran, herr)
		}
	}
	if ran || rerr != nil || atomic.LoadInt64(runs) != 0 {
		t.Fatalf("live RunService after the Tick = %v, %v with the ServiceFn run %d times; want nothing in flight: the timer stopped the service", ran, rerr, atomic.LoadInt64(runs))
	}
}

// TestTimerReenteringTheState_SettleAfterCheckpointRunsTheFreshActivation: the timer
// re-enters loading, so the Tick stops the old activation and starts a fresh one, and
// the checkpoint the Tick's Record carries holds it in flight. RunService on the live
// Handle runs it, and a Recover after that settle, which follows the checkpoint, replays
// it and ends in ready, as live did, with the ServiceFn run once.
func TestTimerReenteringTheState_SettleAfterCheckpointRunsTheFreshActivation(t *testing.T) {
	ctx := context.Background()
	h, m, store, opts, runs := timerSvcRun(t, true)
	if _, ran, err := h.RunService(ctx, timerSvc); !ran || err != nil {
		t.Fatalf("live RunService after the Tick = %v, %v; want the fresh activation run and settled", ran, err)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("live in %q, want ready", got)
	}
	rh, err := durable.Recover(ctx, m, store, "t-1", opts...)
	if err != nil {
		t.Fatalf("Recover after the settle: %v; want the instance recoverable", err)
	}
	if got := rh.Instance().Current(); got != "ready" {
		t.Fatalf("recovered in %q, want ready, as live", got)
	}
	if _, ran, rerr := rh.RunService(ctx, timerSvc); ran || rerr != nil {
		t.Fatalf("RunService after Recover = %v, %v; want nothing in flight", ran, rerr)
	}
	if got := atomic.LoadInt64(runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want once", got)
	}
}
