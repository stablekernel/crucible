package durable_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests pin what a host sees for a service result the decoder for its onDone
// event refuses: RunService returns ErrEventDataEncoding, runs no Assign, and parks the
// result in the journal, with the Handle healthy and the service in flight; the
// ServiceFn never runs again, and a RunService after the decoder is fixed settles the
// parked result.

// countingFetch returns a scnUser and counts its runs, standing in for a ServiceFn
// with a side effect.
func countingFetch(runs *int64) state.ServiceFn[svcCtx] {
	return func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		atomic.AddInt64(runs, 1)
		return scnUser{ID: 7, Name: "ann"}, nil
	}
}

// mustPark fails unless err says a RunService parked its result: ErrEventDataEncoding,
// not ErrHandleFailed, with the service found in flight.
func mustPark(t *testing.T, what string, ran bool, err error) {
	t.Helper()
	if !ran || !errors.Is(err, durable.ErrEventDataEncoding) || errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("%s = %v, %v; want ran and ErrEventDataEncoding without ErrHandleFailed", what, ran, err)
	}
}

// TestServiceResult_UndecodableResult_RunsTheServiceOnce: a result the decoder refuses,
// by failing or by returning another type, is parked. RunService returns
// ErrEventDataEncoding and the Handle stays healthy. Every later RunService, on the
// same Handle or after any number of Recovers, returns ErrEventDataEncoding again
// without running the ServiceFn, and records nothing more. No Assign runs.
func TestServiceResult_UndecodableResult_RunsTheServiceOnce(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  durable.Option[string, string, svcCtx]
	}{
		{"decoder fails", failingDecoder()},
		{"type mismatch", decoderFor("done", durable.DecodeAs[scnAdmin])},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var runs int64
			fn := countingFetch(&runs)
			a := &scnAssigns{}
			m := scnMachine(fn, a)
			store := durable.NewMemStore()
			opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)), tc.opt}
			id := state.InvokeID(scnInvoke, "loading", 0)
			h := scnStart(t, m, store, opts...)
			_, ran, err := h.RunService(ctx, id)
			mustPark(t, "RunService", ran, err)
			_, ran, err = h.RunService(ctx, id)
			mustPark(t, "RunService again on the same Handle", ran, err)
			if verr := h.View(ctx, func(*state.Instance[string, string, svcCtx]) {}); verr != nil {
				t.Fatalf("View after the park: %v, want the Handle healthy", verr)
			}
			for i := 1; i <= 3; i++ {
				rh, rerr := durable.Recover(ctx, m, store, "scn-1", opts...)
				if rerr != nil {
					t.Fatalf("Recover %d: %v", i, rerr)
				}
				_, ran, err = rh.RunService(ctx, id)
				mustPark(t, "RunService after Recover", ran, err)
				if got := rh.Instance().Current(); got != "loading" {
					t.Fatalf("Recover %d: in %q, want loading", i, got)
				}
			}
			if got := atomic.LoadInt64(&runs); got != 1 {
				t.Fatalf("the ServiceFn ran %d times, want 1", got)
			}
			if a.done != 0 || a.fails != 0 {
				t.Fatalf("onDone's Assign ran %d times and onError's %d, want neither", a.done, a.fails)
			}
			if _, recs, lerr := store.Load(ctx, "scn-1"); lerr != nil || len(recs) != 1 {
				t.Fatalf("the journal holds %d Records (err %v), want the one that parked the result", len(recs), lerr)
			}
		})
	}
}

// parkRun starts scn-1 in loading, with the ServiceFn counting its runs, and runs the
// service once under a decoder that refuses the result.
type parkRun struct {
	m     *state.Machine[string, string, svcCtx]
	a     *scnAssigns
	store *durable.MemStore
	runs  *int64
	reg   durable.Option[string, string, svcCtx]
	err   error // the first RunService's error
}

func newParkRun(t *testing.T, extra ...durable.Option[string, string, svcCtx]) *parkRun {
	t.Helper()
	runs := new(int64)
	fn := countingFetch(runs)
	r := &parkRun{
		a:     &scnAssigns{},
		store: durable.NewMemStore(durable.WithHistory()),
		runs:  runs,
		reg:   durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
	}
	r.m = scnMachine(fn, r.a)
	h := scnStart(t, r.m, r.store, append([]durable.Option[string, string, svcCtx]{r.reg, failingDecoder()}, extra...)...)
	_, _, r.err = h.RunService(context.Background(), state.InvokeID(scnInvoke, "loading", 0))
	return r
}

// recoverAndRun Recovers with opts and, if that works, runs the service. It returns
// the Handle, the RunService error and the Recover error.
func (r *parkRun) recoverAndRun(opts ...durable.Option[string, string, svcCtx]) (*durable.Handle[string, string, svcCtx], error, error) {
	ctx := context.Background()
	h, err := durable.Recover(ctx, r.m, r.store, "scn-1", append([]durable.Option[string, string, svcCtx]{r.reg}, opts...)...)
	if err != nil {
		return nil, nil, err
	}
	_, _, runErr := h.RunService(ctx, state.InvokeID(scnInvoke, "loading", 0))
	return h, runErr, nil
}

// The ServiceFn, and its side effect, runs once however often the instance is
// recovered and the service run while the decoder still refuses the result.
func TestParkedResult_ServiceRunsOnce(t *testing.T) {
	r := newParkRun(t)
	for i := 0; i < 3; i++ {
		_, _, _ = r.recoverAndRun(failingDecoder())
	}
	if got := atomic.LoadInt64(r.runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want 1", got)
	}
}

// A decode failure runs no Assign, onDone's or onError's, live, on replay, or while
// the result is parked.
func TestParkedResult_NoAssignOnDecodeFailure(t *testing.T) {
	r := newParkRun(t)
	_, _, _ = r.recoverAndRun(failingDecoder())
	_, _, _ = r.recoverAndRun(decoderFor("done", durable.DecodeAs[scnAdmin]))
	if r.a.done != 0 || r.a.fails != 0 {
		t.Fatalf("onDone's Assign ran %d times and onError's %d, want neither", r.a.done, r.a.fails)
	}
}

// Once the decoder is fixed, a RunService settles the result the service returned the
// first time, through onDone, without running the ServiceFn again; and a later
// Recover replays that settle.
func TestParkedResult_FixedDecoderSettlesTheOriginalResult(t *testing.T) {
	r := newParkRun(t)
	fixed := decoderFor("done", durable.DecodeAs[scnUser])
	h, runErr, err := r.recoverAndRun(fixed)
	if err != nil || runErr != nil {
		t.Fatalf("Recover: %v, RunService: %v, with the fixed decoder", err, runErr)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("the instance is in %q, want ready", got)
	}
	if got := atomic.LoadInt64(r.runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want 1: the fix should settle the recorded result", got)
	}
	if r.a.done != 1 || describe(r.a.seen[0]) != describe(scnUser{ID: 7, Name: "ann"}) {
		t.Fatalf("onDone's Assign ran %d times and saw %v, want once with the scnUser", r.a.done, r.a.seen)
	}
	rh, err := durable.Recover(context.Background(), r.m, r.store, "scn-1", r.reg, fixed)
	if err != nil {
		t.Fatalf("Recover after the settle: %v", err)
	}
	if got := rh.Instance().Current(); got != "ready" {
		t.Fatalf("recovered in %q, want ready", got)
	}
	if r.a.done != 2 || describe(r.a.seen[1]) != describe(r.a.seen[0]) {
		t.Fatalf("replay: onDone's Assign ran %d times in all, saw %v; want twice with the same value", r.a.done, r.a.seen)
	}
	if got := atomic.LoadInt64(r.runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want 1", got)
	}
}

// A parked result settles only as the type the ServiceFn returned, as live: a decoder
// fixed to another type is refused with ErrEventDataEncoding, and no Assign runs.
// JSON's generic form is accepted, as live.
func TestParkedResult_SettlesOnlyAsTheReturnedType(t *testing.T) {
	r := newParkRun(t)
	_, runErr, err := r.recoverAndRun(decoderFor("done", durable.DecodeAs[scnAdmin]))
	if err != nil || !errors.Is(runErr, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover: %v, RunService: %v; want ErrEventDataEncoding for the wrong type", err, runErr)
	}
	if r.a.done != 0 || r.a.fails != 0 {
		t.Fatalf("an Assign ran: onDone %d, onError %d", r.a.done, r.a.fails)
	}
	h, runErr, err := r.recoverAndRun() // the default decoder: JSON's generic form
	if err != nil || runErr != nil {
		t.Fatalf("Recover: %v, RunService: %v, with the default decoder", err, runErr)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("in %q, want ready", got)
	}
	if _, ok := r.a.seen[0].(map[string]any); !ok {
		t.Fatalf("onDone's Assign saw %s, want JSON's generic form", describe(r.a.seen[0]))
	}
}

// The instance can still be recovered while the decoder refuses the result.
func TestParkedResult_RecoverableWhileBroken(t *testing.T) {
	r := newParkRun(t)
	h, err := durable.Recover(context.Background(), r.m, r.store, "scn-1", r.reg, failingDecoder())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := h.Instance().Current(); got != "loading" {
		t.Fatalf("recovered in %q, want loading", got)
	}
}

// The host is told: the RunService that met the failure returns ErrEventDataEncoding,
// and says the result is parked, not ErrHandleFailed.
func TestParkedResult_HostIsTold(t *testing.T) {
	r := newParkRun(t)
	mustPark(t, "RunService", true, r.err)
	if !strings.Contains(r.err.Error(), "parked") {
		t.Fatalf("RunService: %v, want it to say the result is parked", r.err)
	}
}

// The machine routing the failure itself, reaching its onError state with no host
// action, isn't what parking does: the owner chose to have the host fix the decoder.
func TestParkedResult_MachineRoutesTheFailure(t *testing.T) {
	t.Skip("not this design: a parked result waits for the host to fix the decoder and call RunService; the machine doesn't route the failure through onError")
}

// A checkpoint that compacts the Record that parked the result carries the parked
// result: the step that parks offers its checkpoint as any step does, and a Recover
// from it knows the service is parked, not in flight to run. The ServiceFn doesn't run
// again, and the fixed decoder gets the parked result.
func TestParkedResult_CheckpointKeepsIt(t *testing.T) {
	ctx := context.Background()
	r := newParkRun(t, durable.WithCheckpointEvery[string, string, svcCtx](1))
	mustPark(t, "RunService", true, r.err)
	snap, recs, err := r.store.Load(ctx, "scn-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("the tail holds %d Records, want none: the park step should checkpoint", len(recs))
	}
	if !strings.Contains(string(snap), `"parkedServices"`) {
		t.Fatalf("the checkpoint carries no parked result: %s", snap)
	}
	// Still broken: the service is parked, not run.
	if _, runErr, rerr := r.recoverAndRun(failingDecoder()); rerr != nil || !errors.Is(runErr, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover: %v, RunService: %v; want ErrEventDataEncoding", rerr, runErr)
	}
	if got := atomic.LoadInt64(r.runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times after the checkpoint, want 1", got)
	}
	h, runErr, err := r.recoverAndRun(decoderFor("done", durable.DecodeAs[scnUser]))
	if err != nil || runErr != nil {
		t.Fatalf("Recover: %v, RunService: %v, with the fixed decoder", err, runErr)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("in %q, want ready", got)
	}
	if got := atomic.LoadInt64(r.runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want 1", got)
	}
	if r.a.done != 1 || describe(r.a.seen[0]) != describe(scnUser{ID: 7, Name: "ann"}) {
		t.Fatalf("onDone's Assign ran %d times and saw %v, want the parked scnUser", r.a.done, r.a.seen)
	}
}

// StateAt replays a parked Record as the live run left it: nothing settled, so the
// view at the park step is the instance before it, whatever the decoder, and runs no
// Assign. A later settle replays as any recorded result.
func TestParkedResult_StateAt(t *testing.T) {
	ctx := context.Background()
	r := newParkRun(t)
	fixed := decoderFor("done", durable.DecodeAs[scnUser])
	if _, runErr, err := r.recoverAndRun(fixed); err != nil || runErr != nil {
		t.Fatalf("Recover: %v, RunService: %v", err, runErr)
	}
	steps, err := durable.Steps(ctx, r.store, "scn-1")
	if err != nil || len(steps) != 2 {
		t.Fatalf("Steps = %v, %v; want the park and the settle", steps, err)
	}
	doneBefore := r.a.done
	for _, dec := range []durable.Option[string, string, svcCtx]{failingDecoder(), fixed} {
		v, serr := durable.StateAt(ctx, r.m, r.store, "scn-1", steps[0], r.reg, dec)
		if serr != nil {
			t.Fatalf("StateAt the park: %v", serr)
		}
		if got := v.Instance().Current(); got != "loading" {
			t.Fatalf("StateAt the park: in %q, want loading", got)
		}
	}
	if r.a.done != doneBefore || r.a.fails != 0 {
		t.Fatalf("StateAt the park ran an Assign: onDone %d (was %d), onError %d", r.a.done, doneBefore, r.a.fails)
	}
	v, err := durable.StateAt(ctx, r.m, r.store, "scn-1", steps[1], r.reg, fixed)
	if err != nil {
		t.Fatalf("StateAt the settle: %v", err)
	}
	if got := v.Instance().Current(); got != "ready" {
		t.Fatalf("StateAt the settle: in %q, want ready", got)
	}
}

// cancelMachine invokes fn in "loading", which "cancel" leaves for idle and "retry"
// re-enters, so a parked service can be stopped and started again under the same
// invoke id.
func cancelMachine(fn state.ServiceFn[svcCtx], a *scnAssigns) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("park").
		Service("fetch", fn).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			a.done++
			a.seen = append(a.seen, in.Event)
			return in.Entity
		}).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done")).
		State("ready").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").Assign("noteDone").
		Transition("loading").On("cancel").GoTo("idle").
		Transition("idle").On("retry").GoTo("loading").
		Quench()
}

// countingUsers returns a scnUser whose ID counts the runs.
func countingUsers(runs *int64) state.ServiceFn[svcCtx] {
	return func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		return scnUser{ID: atomic.AddInt64(runs, 1), Name: "ann"}, nil
	}
}

// Leaving the state that declares a parked service stops the service and discards the
// parked result, live and on replay, with or without a checkpoint: RunService finds
// nothing in flight, and re-entering the state starts a new activation whose ServiceFn
// runs, rather than settling the discarded result.
func TestParkedResult_LeavingTheStateDiscardsIt(t *testing.T) {
	for _, every := range []int{0, 1} {
		name := "no checkpoint"
		if every > 0 {
			name = "checkpoint every step"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			runs := new(int64)
			fn := countingUsers(runs)
			a := &scnAssigns{}
			m := cancelMachine(fn, a)
			store := durable.NewMemStore()
			base := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}
			if every > 0 {
				base = append(base, durable.WithCheckpointEvery[string, string, svcCtx](every))
			}
			broken := append(append([]durable.Option[string, string, svcCtx]{}, base...), failingDecoder())
			fixed := append(append([]durable.Option[string, string, svcCtx]{}, base...), decoderFor("done", durable.DecodeAs[scnUser]))
			id := state.InvokeID("park", "loading", 0)

			h, err := durable.NewRunner(m, store, broken...).Start(ctx, "p-1", svcCtx{}, state.WithInitialState("loading"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			_, ran, err := h.RunService(ctx, id)
			mustPark(t, "RunService", ran, err)
			if _, err = h.Fire(ctx, "cancel"); err != nil {
				t.Fatalf("Fire cancel: %v", err)
			}
			if _, ran, err = h.RunService(ctx, id); ran || err != nil {
				t.Fatalf("RunService after cancel = %v, %v; want nothing in flight", ran, err)
			}
			rh, err := durable.Recover(ctx, m, store, "p-1", fixed...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if _, ran, err = rh.RunService(ctx, id); ran || err != nil {
				t.Fatalf("RunService after Recover = %v, %v; want nothing in flight", ran, err)
			}
			if got := rh.Instance().Current(); got != "idle" || a.done != 0 {
				t.Fatalf("recovered in %q with onDone run %d times, want idle and none", got, a.done)
			}

			// Re-entering starts a new activation: its ServiceFn runs, and the discarded
			// result never settles.
			if _, err = rh.Fire(ctx, "retry"); err != nil {
				t.Fatalf("Fire retry: %v", err)
			}
			mustRunService(t, rh, id)
			if got := atomic.LoadInt64(runs); got != 2 {
				t.Fatalf("the ServiceFn ran %d times, want 2: once per activation", got)
			}
			if a.done != 1 || describe(a.seen[0]) != describe(scnUser{ID: 2, Name: "ann"}) {
				t.Fatalf("onDone saw %v, want the second activation's scnUser", a.seen)
			}
		})
	}
}

// A service parked, stopped and started again on the same Handle runs its ServiceFn
// for the new activation, rather than settle the first one's parked result, even
// before any Recover.
func TestParkedResult_ReenteredStateRunsTheServiceAgain(t *testing.T) {
	ctx := context.Background()
	runs := new(int64)
	fn := countingUsers(runs)
	a := &scnAssigns{}
	m := cancelMachine(fn, a)
	store := durable.NewMemStore()
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	id := state.InvokeID("park", "loading", 0)
	h, err := durable.NewRunner(m, store, reg, failingDecoder()).Start(ctx, "p-1", svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, ran, err := h.RunService(ctx, id)
	mustPark(t, "RunService", ran, err)
	for _, ev := range []string{"cancel", "retry"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire %s: %v", ev, err)
		}
	}
	_, ran, err = h.RunService(ctx, id)
	mustPark(t, "RunService of the new activation", ran, err)
	if got := atomic.LoadInt64(runs); got != 2 {
		t.Fatalf("the ServiceFn ran %d times, want 2: the new activation runs it", got)
	}
	// The fixed decoder settles the new activation's parked result, not the first's.
	rh, err := durable.Recover(ctx, m, store, "p-1", reg, decoderFor("done", durable.DecodeAs[scnUser]))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	mustRunService(t, rh, id)
	if a.done != 1 || describe(a.seen[0]) != describe(scnUser{ID: 2, Name: "ann"}) {
		t.Fatalf("onDone saw %v, want the second activation's scnUser", a.seen)
	}
	if got := atomic.LoadInt64(runs); got != 2 {
		t.Fatalf("the ServiceFn ran %d times, want 2", got)
	}
}

// A Recover whose journal parks a result, then stops and starts its service again,
// replays the new activation as unparked: RunService runs the ServiceFn for it rather
// than settle the discarded result.
func TestParkedResult_ReplayedReentryRunsTheServiceAgain(t *testing.T) {
	ctx := context.Background()
	runs := new(int64)
	fn := countingUsers(runs)
	a := &scnAssigns{}
	m := cancelMachine(fn, a)
	store := durable.NewMemStore()
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	id := state.InvokeID("park", "loading", 0)
	h, err := durable.NewRunner(m, store, reg, failingDecoder()).Start(ctx, "p-1", svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, ran, err := h.RunService(ctx, id)
	mustPark(t, "RunService", ran, err)
	for _, ev := range []string{"cancel", "retry"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire %s: %v", ev, err)
		}
	}
	rh, err := durable.Recover(ctx, m, store, "p-1", reg, decoderFor("done", durable.DecodeAs[scnUser]))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	mustRunService(t, rh, id)
	if got := atomic.LoadInt64(runs); got != 2 {
		t.Fatalf("the ServiceFn ran %d times, want 2: the new activation runs it", got)
	}
	if a.done != 1 || describe(a.seen[0]) != describe(scnUser{ID: 2, Name: "ann"}) {
		t.Fatalf("onDone saw %v, want the second activation's scnUser", a.seen)
	}
}
