package durable_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// stepIDHandler applies every effect except those of step failStep, for which it
// returns an error, and records every id it was called with, failed or not.
type stepIDHandler struct {
	failStep string
	tried    []string
	applied  []string
}

func (s *stepIDHandler) handle(_ context.Context, id string, _ state.Effect) error {
	s.tried = append(s.tried, id)
	if strings.HasPrefix(id, s.failStep+"#") {
		return errors.New("downstream unavailable")
	}
	s.applied = append(s.applied, id)
	return nil
}

// panicHandler panics on every effect.
func panicHandler(context.Context, string, state.Effect) error {
	panic("handler exploded")
}

// noPanic runs f and turns a panic out of it into a test failure, so a test of a
// panic guard reports instead of crashing the test binary.
func noPanic(t *testing.T, what string, f func() error) (err error) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("%s panicked: %v", what, p)
		}
	}()
	return f()
}

// pairNotifyMachine has two parallel regions whose timers both fall due one second
// after "go", each emitting a different host effect, so one Tick fires both.
func pairNotifyMachine() *state.Machine[string, string, *runCtx] {
	return state.ForgeFor[*runCtx]("pairNotify").
		Action("notify", func(state.ActionCtx[*runCtx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		Action("charge", func(state.ActionCtx[*runCtx]) (state.Effect, error) {
			return chargeEffect{Cents: 500}, nil
		}).
		State("idle").
		SuperState("both").
		Region("left").
		Initial("l1").
		SubState("l1").
		SubState("l2").
		EndRegion().
		Region("right").
		Initial("r1").
		SubState("r1").
		SubState("r2").
		EndRegion().
		EndSuperState().
		Initial("idle").
		Transition("idle").On("go").GoTo("both").
		Transition("l1").After(time.Second).On("toL2").GoTo("l2").Do("notify").
		Transition("r1").After(time.Second).On("toR2").GoTo("r2").Do("charge").
		Quench()
}

// assertAppliedOnceAsLive checks the recovered handler applied exactly the ids the
// live run tried and failed, each once, and nothing the live run already applied.
func assertAppliedOnceAsLive(t *testing.T, live *stepIDHandler, healthy *effectRecorder, wantFailed int) {
	t.Helper()
	var failed []string
	for _, id := range live.tried {
		if strings.HasPrefix(id, live.failStep+"#") {
			failed = append(failed, id)
		}
	}
	if len(failed) != wantFailed {
		t.Fatalf("live run tried %d effects of step %s (%v), want %d", len(failed), live.failStep, live.tried, wantFailed)
	}
	for _, id := range failed {
		if n := healthy.count(id); n != 1 {
			t.Fatalf("Recover applied the live run's failed effect %q %d times, want once (applied %v)", id, n, healthy.ids())
		}
	}
	for _, id := range live.applied {
		if n := healthy.count(id); n != 0 {
			t.Fatalf("Recover applied %q again, which the live run already applied", id)
		}
	}
	if n := healthy.totalApplies(); n != len(failed) {
		t.Fatalf("Recover applied %d effects (%v), want the %d the live run failed", n, healthy.ids(), len(failed))
	}
}

// TestRecover_RedispatchesAFailedTimerEffect proves an effect a timer transition
// emitted, whose dispatch failed live, is applied exactly once by Recover, under the
// EffectID the live attempt saw.
func TestRecover_RedispatchesAFailedTimerEffect(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	clk := state.NewFakeClock(epoch)
	live := &stepIDHandler{failStep: "1"} // step 0 is "go", step 1 the tick
	h, err := durable.NewRunner(timerNotifyMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](live.handle),
		durable.WithRunnerClock[string, string, *runCtx](clk)).Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire go: %v", err)
	}
	clk.Advance(2 * time.Second)
	if _, err = h.Tick(ctx); !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("Tick: %v, want ErrHandleFailed wrapping ErrEffectDispatch", err)
	}
	healthy := newEffectRecorder()
	if _, err = durable.Recover(ctx, timerNotifyMachine(), st, "i1",
		durable.WithEffectHandler[string, string, *runCtx](healthy.handler()),
		durable.WithRunnerClock[string, string, *runCtx](clk)); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertAppliedOnceAsLive(t, live, healthy, 1)
}

// TestRecover_RedispatchesAFailedServiceSettleEffect proves the same for an effect
// the transition a service result drives emitted.
func TestRecover_RedispatchesAFailedServiceSettleEffect(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	var calls int64
	fn := counterService(&calls)
	live := &stepIDHandler{failStep: "1"} // step 0 is "start", step 1 the settle
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}
	h, err := durable.NewRunner(fetchThenWaitMachine(fn), st,
		append(opts, durable.WithEffectHandler[string, string, svcCtx](live.handle))...).Start(ctx, "i1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire start: %v", err)
	}
	if _, _, err = h.RunService(ctx, state.InvokeID("fetchThenWait", "loading", 0)); !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("RunService: %v, want ErrEffectDispatch", err)
	}
	healthy := newEffectRecorder()
	if _, err = durable.Recover(ctx, fetchThenWaitMachine(fn), st, "i1",
		append(opts, durable.WithEffectHandler[string, string, svcCtx](healthy.handler()))...); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertAppliedOnceAsLive(t, live, healthy, 1)
	if calls != 1 {
		t.Fatalf("the service ran %d times, want once: Recover replays its recorded result", calls)
	}
}

// TestRecover_RedispatchesAMultiTimerTicksEffects proves a tick that fires two timers,
// each emitting an effect, re-dispatches both on Recover under the ids the live
// attempt saw.
func TestRecover_RedispatchesAMultiTimerTicksEffects(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	clk := state.NewFakeClock(epoch)
	live := &stepIDHandler{failStep: "1"}
	h, err := durable.NewRunner(pairNotifyMachine(), st,
		durable.WithEffectHandler[string, string, *runCtx](live.handle),
		durable.WithRunnerClock[string, string, *runCtx](clk)).Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire go: %v", err)
	}
	clk.Advance(2 * time.Second)
	fired, err := h.Tick(ctx)
	if len(fired) != 2 || !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("Tick fired %d timers, err %v; want both, and ErrEffectDispatch", len(fired), err)
	}
	// The handler stops at the first failure, so the live run tried only the first
	// effect; the tick's Record names both ids it stamped.
	_, recs, err := st.Load(ctx, "i1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var stamped []string
	for _, r := range recs {
		if r.Tick {
			for _, env := range r.Effects {
				stamped = append(stamped, env.EffectID)
			}
		}
	}
	if len(stamped) != 2 || len(live.tried) != 1 || live.tried[0] != stamped[0] {
		t.Fatalf("the tick stamped %v and the live run tried %v; want two ids, the first tried live", stamped, live.tried)
	}
	live.tried = stamped
	healthy := newEffectRecorder()
	if _, err = durable.Recover(ctx, pairNotifyMachine(), st, "i1",
		durable.WithEffectHandler[string, string, *runCtx](healthy.handler()),
		durable.WithRunnerClock[string, string, *runCtx](clk)); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertAppliedOnceAsLive(t, live, healthy, 2)
}

// TestStart_HandlerPanic_ReturnsEffectDispatchAndLeavesNoInstance proves a handler
// that panics on the initial entry's effect fails Start with ErrEffectDispatch, as a
// handler error does, and leaves no instance behind.
func TestStart_HandlerPanic_ReturnsEffectDispatchAndLeavesNoInstance(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	r := durable.NewRunner(entryNotifyMachine(), st, durable.WithEffectHandler[string, string, *runCtx](panicHandler))
	err := noPanic(t, "Start", func() error {
		_, serr := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("ready"))
		return serr
	})
	if !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("Start: %v, want ErrEffectDispatch", err)
	}
	if _, _, err = st.Load(ctx, "i1"); !errors.Is(err, durable.ErrInstanceNotFound) {
		t.Fatalf("Load after a failed Start: %v, want ErrInstanceNotFound", err)
	}
}

// TestRecover_HandlerPanic_ReturnsEffectDispatch proves a handler that panics while
// Recover re-dispatches a step's unmarked effect, a Fire's, a tick's or a service
// settle's, fails Recover with ErrEffectDispatch rather than panicking out of it.
func TestRecover_HandlerPanic_ReturnsEffectDispatch(t *testing.T) {
	ctx := context.Background()
	t.Run("fire", func(t *testing.T) {
		st := durable.NewMemStore()
		live := &stepIDHandler{failStep: "0"}
		h, err := durable.NewRunner(effectMachine(), st, durable.WithEffectHandler[string, string, *runCtx](live.handle)).
			Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "go"); !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("Fire go: %v, want ErrEffectDispatch", err)
		}
		err = noPanic(t, "Recover", func() error {
			_, rerr := durable.Recover(ctx, effectMachine(), st, "i1", durable.WithEffectHandler[string, string, *runCtx](panicHandler))
			return rerr
		})
		if !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("Recover: %v, want ErrEffectDispatch", err)
		}
	})
	t.Run("tick", func(t *testing.T) {
		st := durable.NewMemStore()
		clk := state.NewFakeClock(epoch)
		live := &stepIDHandler{failStep: "1"}
		h, err := durable.NewRunner(timerNotifyMachine(), st,
			durable.WithEffectHandler[string, string, *runCtx](live.handle),
			durable.WithRunnerClock[string, string, *runCtx](clk)).Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "go"); err != nil {
			t.Fatalf("Fire go: %v", err)
		}
		clk.Advance(2 * time.Second)
		if _, err = h.Tick(ctx); !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("Tick: %v, want ErrEffectDispatch", err)
		}
		err = noPanic(t, "Recover", func() error {
			_, rerr := durable.Recover(ctx, timerNotifyMachine(), st, "i1",
				durable.WithEffectHandler[string, string, *runCtx](panicHandler),
				durable.WithRunnerClock[string, string, *runCtx](clk))
			return rerr
		})
		if !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("Recover: %v, want ErrEffectDispatch", err)
		}
	})
	t.Run("service", func(t *testing.T) {
		st := durable.NewMemStore()
		var calls int64
		fn := counterService(&calls)
		reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
		live := &stepIDHandler{failStep: "1"}
		h, err := durable.NewRunner(fetchThenWaitMachine(fn), st, reg,
			durable.WithEffectHandler[string, string, svcCtx](live.handle)).Start(ctx, "i1", svcCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "start"); err != nil {
			t.Fatalf("Fire start: %v", err)
		}
		if _, _, err = h.RunService(ctx, state.InvokeID("fetchThenWait", "loading", 0)); !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("RunService: %v, want ErrEffectDispatch", err)
		}
		err = noPanic(t, "Recover", func() error {
			_, rerr := durable.Recover(ctx, fetchThenWaitMachine(fn), st, "i1", reg,
				durable.WithEffectHandler[string, string, svcCtx](panicHandler))
			return rerr
		})
		if !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("Recover: %v, want ErrEffectDispatch", err)
		}
	})
}

// olderVersionJournal copies src's journal for id into a fresh MemStore as a version
// before tick and service effects were dispatched would have left it: the checkpoint
// and every Record are of journal format 1, the tick and service Records name no
// effects, and only the Fire effects are marked dispatched.
func olderVersionJournal(t *testing.T, src *durable.MemStore, id durable.InstanceID) *durable.MemStore {
	t.Helper()
	ctx := context.Background()
	snap, recs, err := src.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	dst := durable.NewMemStore()
	if err = dst.Checkpoint(ctx, id, withCheckpointFormat(t, snap, 1), -1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	stripped := 0
	for _, rec := range recs {
		if len(rec.Event) == 0 {
			if len(rec.Effects) > 0 {
				stripped++
			}
			rec.Effects = nil
		} else {
			for _, env := range rec.Effects {
				if err = dst.MarkDispatched(ctx, id, env.EffectID); err != nil {
					t.Fatalf("MarkDispatched: %v", err)
				}
			}
		}
		rec.Format = 1
		if _, err = dst.Append(ctx, id, rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if stripped == 0 {
		t.Fatal("no tick or service Record named an effect to strip")
	}
	return dst
}

// TestRecover_DoesNotSendAnOlderVersionsTickOrServiceEffects proves Recover doesn't
// dispatch an effect a tick or service settle emitted under a version that never
// dispatched them: their Records name no effects, so there is nothing write-ahead to
// retry, and sending it now would apply an effect the run never applied.
func TestRecover_DoesNotSendAnOlderVersionsTickOrServiceEffects(t *testing.T) {
	ctx := context.Background()
	t.Run("tick", func(t *testing.T) {
		st := durable.NewMemStore()
		clk := state.NewFakeClock(epoch)
		h, err := durable.NewRunner(timerNotifyMachine(), st, durable.WithRunnerClock[string, string, *runCtx](clk)).
			Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "go"); err != nil {
			t.Fatalf("Fire go: %v", err)
		}
		clk.Advance(2 * time.Second)
		if _, err = h.Tick(ctx); err != nil {
			t.Fatalf("Tick: %v", err)
		}
		old := olderVersionJournal(t, st, "i1")
		rec := newEffectRecorder()
		if _, err = durable.Recover(ctx, timerNotifyMachine(), old, "i1",
			durable.WithEffectHandler[string, string, *runCtx](rec.handler()),
			durable.WithRunnerClock[string, string, *runCtx](clk)); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if n := rec.totalApplies(); n != 0 {
			t.Fatalf("Recover sent %d effects from an older version's tick (%v), want 0", n, rec.ids())
		}
	})
	t.Run("service", func(t *testing.T) {
		st := durable.NewMemStore()
		var calls int64
		fn := counterService(&calls)
		reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
		h, err := durable.NewRunner(fetchThenWaitMachine(fn), st, reg).Start(ctx, "i1", svcCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "start"); err != nil {
			t.Fatalf("Fire start: %v", err)
		}
		mustRunService(t, h, state.InvokeID("fetchThenWait", "loading", 0))
		old := olderVersionJournal(t, st, "i1")
		rec := newEffectRecorder()
		if _, err = durable.Recover(ctx, fetchThenWaitMachine(fn), old, "i1", reg,
			durable.WithEffectHandler[string, string, svcCtx](rec.handler())); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if n := rec.totalApplies(); n != 0 {
			t.Fatalf("Recover sent %d effects from an older version's service settle (%v), want 0", n, rec.ids())
		}
	})
}
