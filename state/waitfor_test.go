package state_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// TestWaitFor_ResolvesImmediately asserts an instance already satisfying the
// predicate returns at once without driving anything.
func TestWaitFor_ResolvesImmediately(t *testing.T) {
	m := afterMachine()
	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("idle"), state.WithClock[string](clk))

	snap, err := state.WaitFor(context.Background(), inst,
		state.WaitInState[string, string, *trec]("idle"))
	if err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if snap.Current != "idle" {
		t.Fatalf("snapshot current = %q, want idle", snap.Current)
	}
}

// TestWaitFor_ResolvesViaScheduler drives a delayed (`after`) machine with a fake
// clock through a Scheduler: WaitFor advances the clock and ticks the scheduler
// until the delayed transition fires and the instance reaches the target.
func TestWaitFor_ResolvesViaScheduler(t *testing.T) {
	ctx := context.Background()
	m := afterMachine()
	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("idle"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)

	// Enter "armed" so the after-timer is scheduled.
	res := inst.Fire(ctx, "go")
	sch.Absorb(ctx, res.Effects)
	if inst.Current() != "armed" {
		t.Fatalf("state = %q, want armed", inst.Current())
	}

	snap, err := state.WaitFor(ctx, inst,
		state.WaitInState[string, string, *trec]("fired"),
		state.WithWaitScheduler[string, string, *trec](sch),
		state.WithWaitStep[string, string, *trec](time.Second),
		state.WithWaitTimeout[string, string, *trec](time.Minute),
	)
	if err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if snap.Current != "fired" {
		t.Fatalf("snapshot current = %q, want fired", snap.Current)
	}
}

// TestWaitFor_DoneViaCustomStep drives the wait with a custom step function (a
// ServiceRunner-style host loop here approximated by firing the elapse event) and
// waits on WaitDone until the instance reaches its final state.
func TestWaitFor_DoneViaCustomStep(t *testing.T) {
	ctx := context.Background()
	// A two-state machine: armed -> done (final) on "finish".
	m := state.ForgeFor[*trec]("done-wait").
		State("armed").
		State("done").Final().
		Initial("armed").
		CurrentStateFn(func(*trec) string { return "armed" }).
		Transition("armed").On("finish").GoTo("done").
		Quench()
	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("armed"), state.WithClock[string](clk))

	fired := false
	snap, err := state.WaitFor(ctx, inst,
		state.WaitDone[string, string, *trec](),
		state.WithWaitStepFunc[string, string, *trec](func(ctx context.Context, clock state.Clock, step time.Duration) {
			if fc, ok := clock.(*state.FakeClock); ok {
				fc.Advance(step)
			}
			if !fired {
				inst.Fire(ctx, "finish")
				fired = true
			}
		}),
		state.WithWaitStep[string, string, *trec](time.Second),
	)
	if err != nil {
		t.Fatalf("WaitFor: %v", err)
	}
	if snap.Status != state.StatusDone {
		t.Fatalf("status = %v, want StatusDone", snap.Status)
	}
}

// TestWaitFor_TimesOut asserts a predicate that never holds yields a typed
// *WaitTimeoutError once the budget elapses on the fake clock — deterministically,
// with no real waiting.
func TestWaitFor_TimesOut(t *testing.T) {
	ctx := context.Background()
	m := afterMachine()
	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("idle"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)

	res := inst.Fire(ctx, "go")
	sch.Absorb(ctx, res.Effects)

	// "armed" never reaches "idle" again under the scheduler (the only after-timer
	// goes to "fired"), so waiting for "idle" must time out.
	_, err := state.WaitFor(ctx, inst,
		state.WaitInState[string, string, *trec]("idle"),
		state.WithWaitScheduler[string, string, *trec](sch),
		state.WithWaitStep[string, string, *trec](time.Second),
		state.WithWaitTimeout[string, string, *trec](10*time.Second),
	)
	var timeout *state.WaitTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("error = %v, want *WaitTimeoutError", err)
	}
	if timeout.Timeout != 10*time.Second {
		t.Fatalf("timeout budget = %s, want 10s", timeout.Timeout)
	}
}

// TestWaitFor_ContextCanceled asserts a canceled context short-circuits the wait
// with the context error rather than the typed timeout.
func TestWaitFor_ContextCanceled(t *testing.T) {
	m := afterMachine()
	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("armed"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := state.WaitFor(ctx, inst,
		state.WaitInState[string, string, *trec]("fired"),
		state.WithWaitScheduler[string, string, *trec](sch),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
