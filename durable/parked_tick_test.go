package durable_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests pin what a Tick does to a parked result: a timer transition hands its
// effects to the service runner, as a Fire does, so a timer that leaves the state that
// declares a parked service stops the service and discards the parked result, and one
// that re-enters the state starts a fresh activation, whose ServiceFn runs. The live
// Handle, one recovered from the journal and one recovered from a checkpoint agree, and
// the ServiceFn runs once per activation.

// timerParkMachine invokes fn in "loading", which "begin" enters from "idle" (Start
// arms no timer). Its after timer leaves for "timedOut", or with reenter re-enters
// "loading".
func timerParkMachine(fn state.ServiceFn[svcCtx], a *scnAssigns, reenter bool) *state.Machine[string, string, svcCtx] {
	f := state.ForgeFor[svcCtx]("park").
		Service("fetch", fn).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			a.done++
			a.seen = append(a.seen, in.Event)
			return in.Entity
		}).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done")).
		State("ready").Final().
		State("timedOut").
		Initial("idle").
		Transition("idle").On("begin").GoTo("loading").
		Transition("loading").On("done").GoTo("ready").Assign("noteDone")
	if reenter {
		f = f.Transition("loading").After(5 * time.Second).On("redo").GoTo("loading").Reenter()
	} else {
		f = f.Transition("loading").After(5 * time.Second).On("timeout").GoTo("timedOut")
	}
	return f.Quench()
}

// parkProbe is what a RunService of a parked service reports: whether it found the
// service in flight, and whether it found the result parked (ErrEventDataEncoding,
// with the Handle healthy).
type parkProbe struct {
	inFlight, parked bool
}

func probePark(t *testing.T, h *durable.Handle[string, string, svcCtx], id string) parkProbe {
	t.Helper()
	_, ran, err := h.RunService(context.Background(), id)
	if err != nil && !errors.Is(err, durable.ErrEventDataEncoding) || errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("RunService = %v, %v; want nil or ErrEventDataEncoding", ran, err)
	}
	return parkProbe{inFlight: ran, parked: ran && errors.Is(err, durable.ErrEventDataEncoding)}
}

// timerPark is a run that parks the result of loading's service, then Ticks its after
// timer. Its decoder refuses onDone while refuse is set, and decodes it as scnUser
// otherwise, so the live Handle can settle a result once the test clears refuse.
type timerPark struct {
	m      *state.Machine[string, string, svcCtx]
	a      *scnAssigns
	store  *durable.MemStore
	runs   *int64
	refuse *atomic.Bool
	opts   []durable.Option[string, string, svcCtx]
	live   *durable.Handle[string, string, svcCtx]
}

const timerParkID = "p-1"

var timerParkSvc = state.InvokeID("park", "loading", 0)

// newTimerPark parks the result, Ticks the timer, and fails unless the timer fired.
// With every above zero, every step checkpoints, so the Tick's Record carries one.
func newTimerPark(t *testing.T, reenter bool, every int) *timerPark {
	t.Helper()
	ctx := context.Background()
	r := &timerPark{a: &scnAssigns{}, store: durable.NewMemStore(), runs: new(int64), refuse: new(atomic.Bool)}
	r.refuse.Store(true)
	fn := countingUsers(r.runs)
	r.m = timerParkMachine(fn, r.a, reenter)
	clk := state.NewFakeClock(epoch)
	r.opts = []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
		decoderFor("done", func(data []byte) (any, error) {
			if r.refuse.Load() {
				return nil, errors.New("decoder refused")
			}
			return durable.DecodeAs[scnUser](data)
		}),
	}
	if every > 0 {
		r.opts = append(r.opts, durable.WithCheckpointEvery[string, string, svcCtx](every))
	}

	h, err := durable.NewRunner(r.m, r.store, r.opts...).Start(ctx, timerParkID, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "begin"); err != nil {
		t.Fatalf("Fire begin: %v", err)
	}
	_, ran, err := h.RunService(ctx, timerParkSvc)
	mustPark(t, "RunService", ran, err)
	clk.Advance(6 * time.Second)
	if results, terr := h.Tick(ctx); terr != nil || len(results) != 1 {
		t.Fatalf("Tick = %d results, %v; want the timer to fire", len(results), terr)
	}
	want := "timedOut"
	if reenter {
		want = "loading"
	}
	if got := h.Instance().Current(); got != want {
		t.Fatalf("after the Tick in %q, want %q", got, want)
	}
	r.live = h
	return r
}

func (r *timerPark) recover(t *testing.T, what string) *durable.Handle[string, string, svcCtx] {
	t.Helper()
	h, err := durable.Recover(context.Background(), r.m, r.store, timerParkID, r.opts...)
	if err != nil {
		t.Fatalf("Recover %s: %v; want the instance recoverable", what, err)
	}
	return h
}

// mustRun fails unless the ServiceFn ran n times.
func (r *timerPark) mustRun(t *testing.T, n int64, why string) {
	t.Helper()
	if got := atomic.LoadInt64(r.runs); got != n {
		t.Fatalf("the ServiceFn ran %d times, want %d: %s", got, n, why)
	}
}

func everyName(every int) string {
	if every > 0 {
		return "checkpoint every step"
	}
	return "journal"
}

// TestParkedResult_TimerLeavingTheStateStopsTheService pins that an after timer leaving
// the state that declares a parked service stops the service and discards the parked
// result, as a Fire does: RunService finds nothing in flight on the live Handle and on
// one recovered from the journal or from the Tick's checkpoint, which all agree, and
// the ServiceFn ran once. A fixed decoder settles nothing either.
func TestParkedResult_TimerLeavingTheStateStopsTheService(t *testing.T) {
	for _, every := range []int{0, 1} {
		t.Run(everyName(every), func(t *testing.T) {
			r := newTimerPark(t, false, every)
			if got := probePark(t, r.live, timerParkSvc); got != (parkProbe{}) {
				t.Errorf("live after the Tick: %+v, want nothing in flight", got)
			}
			if got := probePark(t, r.recover(t, "after the Tick"), timerParkSvc); got != (parkProbe{}) {
				t.Errorf("recovered after the Tick: %+v, want nothing in flight, as live", got)
			}
			r.refuse.Store(false)
			fh := r.recover(t, "with the fixed decoder")
			if got := probePark(t, fh, timerParkSvc); got != (parkProbe{}) {
				t.Errorf("recovered with the fixed decoder: %+v, want nothing in flight", got)
			}
			if got := fh.Instance().Current(); got != "timedOut" || r.a.done != 0 {
				t.Errorf("recovered in %q with onDone run %d times, want timedOut and none", got, r.a.done)
			}
			if _, recs, _ := r.store.Load(context.Background(), timerParkID); every == 0 && len(recs) != 3 {
				t.Errorf("the journal holds %d Records, want 3 (begin, the park, the Tick): no RunService recorded anything", len(recs))
			}
			r.mustRun(t, 1, "the service stopped with its parked result")
		})
	}
}

// TestParkedResult_TimerThenCheckpointThenSettleStaysRecoverable is the sequence that
// failed every Recover while a Tick left the service in flight: park a result, let a
// timer leave the service's state, take a checkpoint that no longer holds the service,
// then call RunService on the live Handle. The Handle settled the service there, kept
// or run again, and journaled a settle replay can't apply ("no in-flight service to
// settle"). Now the timer stopped the service, so RunService finds nothing, records
// nothing, and every Recover succeeds.
func TestParkedResult_TimerThenCheckpointThenSettleStaysRecoverable(t *testing.T) {
	ctx := context.Background()
	// Every third step checkpoints: begin, the park, then the Tick, so the checkpoint
	// lands on the Tick's Record and anything RunService records follows it.
	r := newTimerPark(t, false, 3)
	ckpt, recs, err := r.store.Load(ctx, timerParkID)
	if err != nil || len(ckpt) == 0 || len(recs) != 0 {
		t.Fatalf("after the Tick the store holds a %d-byte checkpoint and %d Records (%v); want the Tick's checkpoint and nothing after it", len(ckpt), len(recs), err)
	}
	r.refuse.Store(false)
	_, ran, rerr := r.live.RunService(ctx, timerParkSvc)
	for i := range 2 {
		h := r.recover(t, "after the live RunService")
		if got := h.Instance().Current(); got != "timedOut" {
			t.Fatalf("Recover %d in %q, want timedOut", i, got)
		}
		if _, hran, herr := h.RunService(ctx, timerParkSvc); hran || herr != nil {
			t.Fatalf("RunService after Recover %d = %v, %v; want nothing in flight", i, hran, herr)
		}
	}
	if ran || rerr != nil {
		t.Fatalf("live RunService after the Tick = %v, %v; want nothing in flight: the timer stopped the service", ran, rerr)
	}
	r.mustRun(t, 1, "the timer stopped the service")
}

// TestParkedResult_TimerReenteringTheStateRunsAFreshActivation pins that an after timer
// that re-enters the state that declares a parked service stops the old activation,
// discarding its parked result, and starts a fresh one, whose ServiceFn runs: on the
// live Handle and on one recovered from the journal or the Tick's checkpoint. The fixed
// decoder settles the fresh run's result, which reaches ready with the second
// scnUser, and the ServiceFn runs once per activation.
func TestParkedResult_TimerReenteringTheStateRunsAFreshActivation(t *testing.T) {
	for _, every := range []int{0, 1} {
		for _, where := range []string{"live", "recovered"} {
			t.Run(everyName(every)+"/"+where, func(t *testing.T) {
				r := newTimerPark(t, true, every)
				h := r.live
				if where == "recovered" {
					h = r.recover(t, "after the Tick")
				}
				r.refuse.Store(false)
				if got := probePark(t, h, timerParkSvc); got != (parkProbe{inFlight: true}) {
					t.Fatalf("%s after the Tick: %+v, want the fresh activation run and settled", where, got)
				}
				r.mustRun(t, 2, "once for each activation")
				if got := h.Instance().Current(); got != "ready" {
					t.Fatalf("in %q, want ready", got)
				}
				if describe(r.a.seen[0]) != describe(scnUser{ID: 2, Name: "ann"}) {
					t.Fatalf("onDone saw %v, want the fresh activation's scnUser", r.a.seen)
				}
				if got := probePark(t, h, timerParkSvc); got != (parkProbe{}) {
					t.Fatalf("after the settle: %+v, want nothing in flight", got)
				}
				if got := r.recover(t, "after the settle").Instance().Current(); got != "ready" {
					t.Fatalf("recovered after the settle in %q, want ready", got)
				}
				r.mustRun(t, 2, "once for each activation")
			})
		}
	}
}

// TestTimerLeavingTheState_SettleAfterCheckpointStaysRecoverable is the same sequence
// with nothing parked: loading's service is in flight, its after timer leaves for
// timedOut, the Tick's Record carries a checkpoint, which holds only the services of the
// states the instance is in, and the live Handle then calls RunService. While Tick left
// the service in flight, that ran it and journaled its settle after the checkpoint,
// which replay couldn't apply, so every Recover failed. Now the timer stopped the
// service: RunService finds nothing in flight and Recover succeeds.
func TestTimerLeavingTheState_SettleAfterCheckpointStaysRecoverable(t *testing.T) {
	ctx := context.Background()
	runs := new(int64)
	fn := countingUsers(runs)
	// prep adds a step before loading, so the Tick lands on the checkpoint and anything
	// RunService records follows it.
	m := state.ForgeFor[svcCtx]("park").
		Service("fetch", fn).
		State("idle").
		State("prep").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done")).
		State("ready").Final().
		State("timedOut").
		Initial("idle").
		Transition("idle").On("prep").GoTo("prep").
		Transition("prep").On("begin").GoTo("loading").
		Transition("loading").On("done").GoTo("ready").
		Transition("loading").After(5 * time.Second).On("timeout").GoTo("timedOut").
		Quench()
	clk := state.NewFakeClock(epoch)
	store := durable.NewMemStore()
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
		decoderFor("done", durable.DecodeAs[scnUser]),
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
	_, ran, rerr := h.RunService(ctx, timerParkSvc)
	if _, err = durable.Recover(ctx, m, store, "t-1", opts...); err != nil {
		t.Fatalf("Recover: %v; want the instance recoverable", err)
	}
	if ran || rerr != nil || atomic.LoadInt64(runs) != 0 {
		t.Fatalf("live RunService after the Tick = %v, %v with the ServiceFn run %d times; want nothing in flight: the timer stopped the service", ran, rerr, atomic.LoadInt64(runs))
	}
}
