package state

import (
	"context"
	"time"
)

// This file ships WaitFor — the host-side helper that drives an instance until a
// predicate over its observable state holds, mirroring xstate v5's
// `waitFor(actor, predicate, { timeout })`. The pure Fire step never blocks or
// waits; WaitFor is the driver-side convenience that repeatedly advances a host
// driver (a Scheduler ticking `after` timers, or any caller-supplied step) and
// checks the predicate after each advance, returning the matching snapshot or a
// typed timeout.
//
// Determinism: with a FakeClock-backed Scheduler, WaitFor advances the fake clock
// in fixed increments and ticks the scheduler, so an `after`-driven machine
// reaches its predicate with no real waiting and no wall-clock dependence. A test
// asserts both the match path and the timeout path deterministically.

// WaitPredicate is the condition WaitFor waits to become true. It is evaluated
// against the instance's live Snapshot after each advance (and once before any
// advance). It must be a pure read of the snapshot — WaitFor never mutates the
// instance on the predicate's behalf.
type WaitPredicate[S comparable, E comparable, C any] func(snap Snapshot[S, E, C]) bool

// WaitFor drives inst until predicate holds over its Snapshot, or until the
// supplied context is canceled or the wait budget elapses. It returns the
// matching snapshot on success, or the zero snapshot and a typed *WaitTimeoutError
// when the budget elapses (or the wrapped context error when ctx is canceled)
// without the predicate ever holding.
//
// The predicate is checked once immediately, before any advance, so an instance
// already in the desired state returns at once without driving. When it does not
// yet hold, WaitFor advances its driver one step at a time and rechecks: by
// default it ticks a Scheduler over a FakeClock (WithWaitScheduler), advancing the
// fake clock by a fixed step each iteration so `after` machines progress
// deterministically; a caller with a different driver supplies WithWaitStep.
//
// With no driver option WaitFor cannot make progress (an undriven instance never
// changes on its own), so it checks the predicate once and, if unmet, waits out
// the budget and returns the typed timeout — the correct result for "the instance
// will never reach this state without being driven".
//
// WaitFor never reads the wall clock: time is measured by the driver's clock (the
// Scheduler's, a FakeClock in tests), so the whole helper is deterministic under a
// fake clock.
func WaitFor[S comparable, E comparable, C any](
	ctx context.Context,
	inst *Instance[S, E, C],
	predicate WaitPredicate[S, E, C],
	opts ...WaitOption[S, E, C],
) (Snapshot[S, E, C], error) {
	cfg := waitConfig[S, E, C]{
		timeout: defaultWaitTimeout,
		step:    defaultWaitStep,
	}
	for _, o := range opts {
		o(&cfg)
	}

	// Immediate check: an instance already satisfying the predicate returns at once.
	if snap := inst.Snapshot(); predicate(snap) {
		return snap, nil
	}

	// Measure elapsed time against the instance's clock so a FakeClock test is
	// deterministic; the driver advances that same clock.
	clock := inst.clock
	start := clock.Now()
	deadline := start.Add(cfg.timeout)

	for {
		if err := ctx.Err(); err != nil {
			return Snapshot[S, E, C]{}, err
		}
		if !clock.Now().Before(deadline) {
			return Snapshot[S, E, C]{}, &WaitTimeoutError{
				Machine: inst.machine.name,
				Timeout: cfg.timeout,
				Last:    fmtState(inst.current),
			}
		}

		// Advance the driver one step. A driver advances the clock and fires any due
		// work; an absent driver (the zero advance) makes no progress, so the loop
		// runs out the budget and times out — the right answer for an undriven wait.
		if cfg.advance != nil {
			cfg.advance(ctx, clock, cfg.step)
		} else {
			// No driver: advance the deadline tracking without progress so the loop
			// terminates at the budget rather than spinning. Use a fake-clock advance
			// when the clock supports it; otherwise return the timeout immediately,
			// since a real clock with no driver would busy-spin.
			if fc, ok := clock.(*FakeClock); ok {
				fc.Advance(cfg.step)
			} else {
				return Snapshot[S, E, C]{}, &WaitTimeoutError{
					Machine: inst.machine.name,
					Timeout: cfg.timeout,
					Last:    fmtState(inst.current),
				}
			}
		}

		if snap := inst.Snapshot(); predicate(snap) {
			return snap, nil
		}
	}
}

// WaitInState returns a WaitPredicate that holds when the instance's primary
// active leaf equals target — the common "wait until it reaches state X" case
// (xstate's `waitFor(actor, (s) => s.matches('X'))`).
func WaitInState[S comparable, E comparable, C any](target S) WaitPredicate[S, E, C] {
	return func(snap Snapshot[S, E, C]) bool { return snap.Current == target }
}

// WaitDone returns a WaitPredicate that holds when the instance has reached
// completion (its whole active configuration is final), mirroring
// `waitFor(actor, (s) => s.status === 'done')`.
func WaitDone[S comparable, E comparable, C any]() WaitPredicate[S, E, C] {
	return func(snap Snapshot[S, E, C]) bool { return snap.Status == StatusDone }
}

// defaultWaitTimeout bounds a WaitFor that supplies no WithWaitTimeout, measured
// on the instance's clock. It mirrors xstate's default infinite wait being
// bounded in practice; here it is a finite, deterministic budget so a test never
// hangs.
const defaultWaitTimeout = 10 * time.Second

// defaultWaitStep is the per-iteration clock advance WaitFor applies when driving
// over a fake clock without an explicit WithWaitStep.
const defaultWaitStep = time.Millisecond

// WaitOption configures WaitFor.
type WaitOption[S comparable, E comparable, C any] func(*waitConfig[S, E, C])

type waitConfig[S comparable, E comparable, C any] struct {
	timeout time.Duration
	step    time.Duration
	advance func(ctx context.Context, clock Clock, step time.Duration)
}

// WithWaitTimeout sets the wait budget, measured on the instance's clock. When the
// budget elapses before the predicate holds, WaitFor returns a *WaitTimeoutError.
func WithWaitTimeout[S comparable, E comparable, C any](d time.Duration) WaitOption[S, E, C] {
	return func(c *waitConfig[S, E, C]) { c.timeout = d }
}

// WithWaitStep sets the per-iteration advance increment WaitFor applies to the
// driver's clock between predicate checks. A smaller step lands closer to the
// exact instant a delayed transition becomes due; a larger step polls less often.
func WithWaitStep[S comparable, E comparable, C any](d time.Duration) WaitOption[S, E, C] {
	return func(c *waitConfig[S, E, C]) {
		if d > 0 {
			c.step = d
		}
	}
}

// WithWaitScheduler drives the wait by advancing the FakeClock the Scheduler reads
// and ticking it each iteration, so `after`-driven transitions fire and the
// instance progresses toward the predicate deterministically. It is the common
// driver for delayed-transition machines: cast with WithClock(fakeClock), build a
// Scheduler, then WaitFor(ctx, inst, pred, WithWaitScheduler(sch)). The Scheduler's
// clock must be the instance's clock (it is, by construction of NewScheduler).
func WithWaitScheduler[S comparable, E comparable, C any](sch *Scheduler[S, E, C]) WaitOption[S, E, C] {
	return func(c *waitConfig[S, E, C]) {
		c.advance = func(ctx context.Context, clock Clock, step time.Duration) {
			if fc, ok := clock.(*FakeClock); ok {
				fc.Advance(step)
			}
			sch.Tick(ctx)
		}
	}
}

// WithWaitStepFunc supplies a custom driver advance: a function WaitFor calls each
// iteration to move time forward and fire any due work (e.g. a ServiceRunner a
// test settles, or a bespoke host loop). The function should advance the supplied
// clock by step when it is a FakeClock so the wait budget is consumed
// deterministically.
func WithWaitStepFunc[S comparable, E comparable, C any](
	advance func(ctx context.Context, clock Clock, step time.Duration),
) WaitOption[S, E, C] {
	return func(c *waitConfig[S, E, C]) { c.advance = advance }
}
