package durable_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// svcCtx is a JSON-marshalable context for the invoked-service record/replay
// proofs. Notes folds each settled service's routed result/error so a recovered
// instance's context can be compared for byte-identity against the live run.
type svcCtx struct {
	Notes []string `json:"notes"`
}

// mustRunService runs the in-flight service id through h and fails the test unless
// it settled. It keeps the call sites free of a shadowed err declaration.
func mustRunService[S comparable, E comparable, C any](t *testing.T, h *durable.Handle[S, E, C], id string) {
	t.Helper()
	_, ok, err := h.RunService(context.Background(), id)
	if err != nil {
		t.Fatalf("RunService(%q): %v", id, err)
	}
	if !ok {
		t.Fatalf("RunService(%q): reported no in-flight service", id)
	}
}

// counterService returns a DIFFERENT value on each invocation. It is the heart of
// the record/replay proof: if recovery re-ran the service, the recovered context
// would fold a fresh (higher) counter value; byte-identity to the live run is only
// possible if the recorded result was replayed and the service was not re-invoked.
func counterService(calls *int64) state.ServiceFn[svcCtx] {
	return func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		n := atomic.AddInt64(calls, 1)
		return fmt.Sprintf("call-%d", n), nil
	}
}

// fetchMachine is a single-service machine: idle -(start)-> loading [invoke fetch]
// -(ok)-> ready, with an onError path to errored. The onDone transition folds the
// service result into context via an Assign reading AssignCtx.Event, so the routed
// result is observable in the recovered snapshot. fn is bound under "fetch" so the
// machine and the durable Runner's registry share one implementation.
func fetchMachine(fn state.ServiceFn[svcCtx]) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("fetch").
		Service("fetch", fn).
		Reducer("recordResult", func(in state.AssignCtx[svcCtx]) svcCtx {
			c := in.Entity
			if r, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "result:"+r)
			}
			return c
		}).
		Reducer("recordError", func(in state.AssignCtx[svcCtx]) svcCtx {
			c := in.Entity
			if e, ok := in.Event.(error); ok {
				c.Notes = append(c.Notes, "error:"+e.Error())
			}
			return c
		}).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok"), state.WithInvokeOnError("fail")).
		State("ready").Final().
		State("errored").Final().
		Initial("idle").
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("ready").Assign("recordResult").
		Transition("loading").On("fail").GoTo("errored").Assign("recordError").
		Quench()
}

// fetchRegistry binds fn under "fetch" so the durable Runner can resolve and run
// the same service the machine declares.
func fetchRegistry(fn state.ServiceFn[svcCtx]) *state.Registry[svcCtx] {
	return state.NewRegistry[svcCtx]().Service("fetch", fn)
}

// liveFetchRun drives a fresh instance through start -> service settle, returning
// the live snapshot bytes, the recorded call count, and the store for recovery.
func liveFetchRun(t *testing.T, calls *int64) ([]byte, durable.InstanceID, durable.Store) {
	t.Helper()
	ctx := context.Background()
	fn := counterService(calls)
	reg := fetchRegistry(fn)
	m := fetchMachine(fn)
	store := durable.NewMemStore()
	id := durable.InstanceID("fetch-1")

	runner := durable.NewRunner(m, store,
		durable.WithServiceRegistry[string, string, svcCtx](reg))
	h, err := runner.Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	mustRunService(t, h, state.InvokeID("fetch", "loading", 0))
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("after service done, want ready, got %q", got)
	}
	snap, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	return snap, id, store
}

// TestService_DoneReplay_ByteIdentical_NotReInvoked is the acceptance gate. A
// service whose result differs each call is run live (recording result R), the
// instance is recovered, and the recovery is asserted to be (a) byte-identical to
// the live snapshot AND (b) achieved WITHOUT re-invoking the service: the live
// call count is 1 and recovery does not increment it.
func TestService_DoneReplay_ByteIdentical_NotReInvoked(t *testing.T) {
	ctx := context.Background()
	var calls int64
	liveSnap, id, store := liveFetchRun(t, &calls)

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("live run invoked the service %d times, want 1", got)
	}

	// Recover with a registry whose service would yield a DIFFERENT value if re-run
	// (the same counter, already at 1). Byte-identity proves the recorded result was
	// replayed, not a fresh invocation.
	fn := counterService(&calls)
	reg := fetchRegistry(fn)
	m := fetchMachine(fn)
	h, err := durable.Recover(ctx, m, store, id,
		durable.WithServiceRegistry[string, string, svcCtx](reg))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("recovery re-invoked the service: call count %d, want 1", got)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("recovered state = %q, want ready", got)
	}
	recSnap, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot(recovered): %v", err)
	}
	if string(recSnap) != string(liveSnap) {
		t.Fatalf("recovered snapshot not byte-identical to live:\n live=%s\n  rec=%s", liveSnap, recSnap)
	}
}

// TestService_ErrorReplay_ByteIdentical drives the onError path: a failing service
// settles through SettleError, recording the error payload; recovery replays the
// recorded error (not a fresh invocation) and reaches a byte-identical snapshot.
func TestService_ErrorReplay_ByteIdentical(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		atomic.AddInt64(&calls, 1)
		return nil, errors.New("boom")
	}
	reg := fetchRegistry(fn)
	m := fetchMachine(fn)
	store := durable.NewMemStore()
	id := durable.InstanceID("fetch-err")

	runner := durable.NewRunner(m, store,
		durable.WithServiceRegistry[string, string, svcCtx](reg))
	h, err := runner.Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	mustRunService(t, h, state.InvokeID("fetch", "loading", 0))
	if got := h.Instance().Current(); got != "errored" {
		t.Fatalf("after service error, want errored, got %q", got)
	}
	liveSnap, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("live error run invoked service %d times, want 1", got)
	}

	fn2 := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		atomic.AddInt64(&calls, 1)
		return nil, errors.New("different-error")
	}
	reg2 := fetchRegistry(fn2)
	m2 := fetchMachine(fn2)
	rh, err := durable.Recover(ctx, m2, store, id,
		durable.WithServiceRegistry[string, string, svcCtx](reg2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("recovery re-invoked the failing service: call count %d, want 1", got)
	}
	recSnap, err := state.MarshalSnapshot(rh.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot(recovered): %v", err)
	}
	if string(recSnap) != string(liveSnap) {
		t.Fatalf("recovered error snapshot not byte-identical:\n live=%s\n  rec=%s", liveSnap, recSnap)
	}
}

// chainMachine invokes two services in sequence within one instance: the first
// service's done advances to a second invoking state, whose own service done
// reaches the final. It proves multiple services in one instance record and replay
// in order.
func chainMachine(svcA, svcB state.ServiceFn[svcCtx]) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("chain").
		Service("svcA", svcA).
		Service("svcB", svcB).
		Reducer("note", func(in state.AssignCtx[svcCtx]) svcCtx {
			c := in.Entity
			if r, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, r)
			}
			return c
		}).
		State("idle").
		State("first").Invoke("svcA", state.WithInvokeOnDone("aDone"), state.WithInvokeOnError("aFail")).
		State("second").Invoke("svcB", state.WithInvokeOnDone("bDone"), state.WithInvokeOnError("bFail")).
		State("done").Final().
		State("failed").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("first").
		Transition("first").On("aDone").GoTo("second").Assign("note").
		Transition("first").On("aFail").GoTo("failed").
		Transition("second").On("bDone").GoTo("done").Assign("note").
		Transition("second").On("bFail").GoTo("failed").
		Quench()
}

// chainRegistry binds svcA and svcB so the durable Runner resolves them.
func chainRegistry(svcA, svcB state.ServiceFn[svcCtx]) *state.Registry[svcCtx] {
	return state.NewRegistry[svcCtx]().Service("svcA", svcA).Service("svcB", svcB)
}

// TestService_MultipleServices_OneInstance_Replay records two sequential service
// settlements in a single instance and asserts the recovered snapshot is
// byte-identical with neither service re-invoked.
func TestService_MultipleServices_OneInstance_Replay(t *testing.T) {
	ctx := context.Background()
	var aCalls, bCalls int64
	svcA, svcB := counterService(&aCalls), counterService(&bCalls)
	reg := chainRegistry(svcA, svcB)
	m := chainMachine(svcA, svcB)
	store := durable.NewMemStore()
	id := durable.InstanceID("chain-1")

	runner := durable.NewRunner(m, store,
		durable.WithServiceRegistry[string, string, svcCtx](reg))
	h, err := runner.Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	mustRunService(t, h, state.InvokeID("chain", "first", 0))
	mustRunService(t, h, state.InvokeID("chain", "second", 0))
	if got := h.Instance().Current(); got != "done" {
		t.Fatalf("after both services, want done, got %q", got)
	}
	liveSnap, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}

	svcA2, svcB2 := counterService(&aCalls), counterService(&bCalls)
	reg2 := chainRegistry(svcA2, svcB2)
	m2 := chainMachine(svcA2, svcB2)
	rh, err := durable.Recover(ctx, m2, store, id,
		durable.WithServiceRegistry[string, string, svcCtx](reg2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if a, b := atomic.LoadInt64(&aCalls), atomic.LoadInt64(&bCalls); a != 1 || b != 1 {
		t.Fatalf("recovery re-invoked services: aCalls=%d bCalls=%d, want 1,1", a, b)
	}
	recSnap, err := state.MarshalSnapshot(rh.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot(recovered): %v", err)
	}
	if string(recSnap) != string(liveSnap) {
		t.Fatalf("chained recovered snapshot not byte-identical:\n live=%s\n  rec=%s", liveSnap, recSnap)
	}
}

// TestService_CrashAfterSettle_ResumeContinues models a crash AFTER a service
// settled but before the next step: the live run settles the first service (and is
// persisted), then a fresh process recovers and drives the second service to
// completion. The resumed instance reaches the final state with the first result
// replayed (not re-invoked) and the second freshly run exactly once.
func TestService_CrashAfterSettle_ResumeContinues(t *testing.T) {
	ctx := context.Background()
	var aCalls, bCalls int64
	svcA, svcB := counterService(&aCalls), counterService(&bCalls)
	reg := chainRegistry(svcA, svcB)
	m := chainMachine(svcA, svcB)
	store := durable.NewMemStore()
	id := durable.InstanceID("chain-crash")

	runner := durable.NewRunner(m, store,
		durable.WithServiceRegistry[string, string, svcCtx](reg))
	h, err := runner.Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	mustRunService(t, h, state.InvokeID("chain", "first", 0))
	// Simulate a crash here: drop the live handle entirely. The first service has
	// settled and been persisted; the second has not yet been driven.

	svcA2, svcB2 := counterService(&aCalls), counterService(&bCalls)
	reg2 := chainRegistry(svcA2, svcB2)
	m2 := chainMachine(svcA2, svcB2)
	rh, err := durable.Recover(ctx, m2, store, id,
		durable.WithServiceRegistry[string, string, svcCtx](reg2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := rh.Instance().Current(); got != "second" {
		t.Fatalf("resumed at %q, want second (mid-chain)", got)
	}
	if a := atomic.LoadInt64(&aCalls); a != 1 {
		t.Fatalf("svcA re-invoked on resume: aCalls=%d, want 1", a)
	}
	// Drive the second service live on the resumed handle.
	mustRunService(t, rh, state.InvokeID("chain", "second", 0))
	if got := rh.Instance().Current(); got != "done" {
		t.Fatalf("after resumed svcB, want done, got %q", got)
	}
	if b := atomic.LoadInt64(&bCalls); b != 1 {
		t.Fatalf("svcB invoked %d times, want exactly 1", b)
	}

	// A second recovery now replays BOTH recorded services, re-invoking neither.
	svcA3, svcB3 := counterService(&aCalls), counterService(&bCalls)
	reg3 := chainRegistry(svcA3, svcB3)
	m3 := chainMachine(svcA3, svcB3)
	rh2, err := durable.Recover(ctx, m3, store, id,
		durable.WithServiceRegistry[string, string, svcCtx](reg3))
	if err != nil {
		t.Fatalf("Recover (second): %v", err)
	}
	if a, b := atomic.LoadInt64(&aCalls), atomic.LoadInt64(&bCalls); a != 1 || b != 1 {
		t.Fatalf("second recovery re-invoked services: aCalls=%d bCalls=%d, want 1,1", a, b)
	}
	if got := rh2.Instance().Current(); got != "done" {
		t.Fatalf("second recovery state = %q, want done", got)
	}
}

// TestService_RunService_NoRegistry asserts RunService reports an error when no
// service registry was wired, so a misconfigured host fails loudly rather than
// silently skipping the service.
func TestService_RunService_NoRegistry(t *testing.T) {
	ctx := context.Background()
	m := fetchMachine(counterService(new(int64)))
	store := durable.NewMemStore()
	id := durable.InstanceID("no-reg")
	runner := durable.NewRunner(m, store) // no WithServiceRegistry
	h, err := runner.Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	_, ok, err := h.RunService(ctx, state.InvokeID("fetch", "loading", 0))
	if ok {
		t.Fatalf("RunService reported ok with no registry")
	}
	if err == nil {
		t.Fatalf("RunService with no registry should error")
	}
}

// TestService_RunService_NotPending asserts RunService is a no-op (ok=false, no
// error) for an id that names no in-flight service.
func TestService_RunService_NotPending(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchMachine(fn)
	store := durable.NewMemStore()
	id := durable.InstanceID("not-pending")
	runner := durable.NewRunner(m, store,
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)))
	h, err := runner.Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	// In idle no service is armed yet.
	res, ok, err := h.RunService(ctx, "no-such-service")
	if ok || err != nil {
		t.Fatalf("RunService for unknown id: ok=%v err=%v, want false,nil", ok, err)
	}
	if res.NewState != "" {
		t.Fatalf("RunService for unknown id returned non-zero result: %+v", res)
	}
	if got := atomic.LoadInt64(&calls); got != 0 {
		t.Fatalf("RunService for unknown id invoked a service %d times, want 0", got)
	}
}

// corruptingStore wraps a Store and corrupts the first recorded service-result
// payload returned by Load, so recovery exercises the replay decode-error path.
type corruptingStore struct {
	durable.Store
	corrupted bool
}

func (s *corruptingStore) Load(ctx context.Context, id durable.InstanceID) ([]byte, []durable.Record, error) {
	snap, tail, err := s.Store.Load(ctx, id)
	if err != nil {
		return snap, tail, err
	}
	for i := range tail {
		for j := range tail[i].Entries {
			if tail[i].Entries[j].Kind == state.JournalServiceResult {
				tail[i].Entries[j].Payload = []byte("{not-json")
				s.corrupted = true
			}
		}
	}
	return snap, tail, nil
}

// TestService_Replay_CorruptPayload asserts recovery surfaces a decode error when
// a recorded service outcome payload is corrupt, rather than silently diverging.
func TestService_Replay_CorruptPayload(t *testing.T) {
	ctx := context.Background()
	var calls int64
	_, id, base := liveFetchRun(t, &calls)
	store := &corruptingStore{Store: base}

	fn := counterService(&calls)
	m := fetchMachine(fn)
	if _, err := durable.Recover(ctx, m, store, id,
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))); err == nil {
		t.Fatal("Recover with corrupt service payload should error")
	}
	if !store.corrupted {
		t.Fatal("no service-result entry was corrupted; test did not exercise the path")
	}
}

// TestService_Determinism asserts two independent live runs of the same machine
// produce byte-identical snapshots, confirming the seam introduces no ordering or
// timing nondeterminism of its own.
func TestService_Determinism(t *testing.T) {
	var c1, c2 int64
	s1, _, _ := liveFetchRun(t, &c1)
	s2, _, _ := liveFetchRun(t, &c2)
	if string(s1) != string(s2) {
		t.Fatalf("two live runs diverged:\n run1=%s\n run2=%s", s1, s2)
	}
}

// fetchThenWaitMachine routes its service result into a state that arms a timer,
// emitting a host effect on the way, so both originate in RunService.
func fetchThenWaitMachine(fn state.ServiceFn[svcCtx]) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("fetchThenWait").
		Service("fetch", fn).
		Action("notify", func(state.ActionCtx[svcCtx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok")).
		State("waiting").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("waiting").Do("notify").
		Transition("waiting").On("poke").GoTo("waiting").
		Transition("waiting").After(5 * time.Second).On("elapsed").GoTo("done").
		Quench()
}

// TestService_TimerArmedBySettle_ReplaysOnRecover proves a timer armed by a service
// result survives recovery, with an idle tick between the settle and the tick that
// fires it.
func TestService_TimerArmedBySettle_ReplaysOnRecover(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchThenWaitMachine(fn)
	store := durable.NewMemStore()
	id := durable.InstanceID("fetch-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
	}
	runner := durable.NewRunner(m, store, opts...)
	h, err := runner.Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	clk.Advance(time.Second) // the settle's read differs from the earlier ones
	mustRunService(t, h, state.InvokeID("fetchThenWait", "loading", 0))
	for _, d := range []time.Duration{2 * time.Second, 4 * time.Second} { // idle, then due
		clk.Advance(d)
		if _, err = h.Tick(ctx); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}
	if got := h.Instance().Current(); got != "done" {
		t.Fatalf("live state %q, want done", got)
	}

	// A clock still at the start proves the timer replays at its recorded instant.
	recovered, err := durable.Recover(ctx, m, store, id, append(opts, durable.WithRunnerClock[string, string, svcCtx](state.NewFakeClock(epoch)))...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if live, got := handleSnapshot(t, h), handleSnapshot(t, recovered); got != live {
		t.Fatalf("recovered instance differs from live:\n live: %s\n got:  %s", live, got)
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("service ran %d times, want 1: recovery replays the recorded result", got)
	}
}

// TestStateAt_TimerArmedBySettle_Fires proves a historical read arms a timer from a
// recorded service result, so it fires in the replayed window as it did live.
func TestStateAt_TimerArmedBySettle_Fires(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchThenWaitMachine(fn)
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("fetch-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	mustRunService(t, h, state.InvokeID("fetchThenWait", "loading", 0))
	clk.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	view, err := durable.StateAt(ctx, m, store, id, 2, opts...) // the tick's barrier
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if got := view.Instance().Current(); got != "done" {
		t.Fatalf("StateAt state %q, want done", got)
	}
}

// TestRecover_TimerArmedBySettle_SurvivesALaterCheckpoint proves recovery keeps a timer
// armed by a replayed service result in the table the next checkpoint persists, so a
// second recovery from that checkpoint still fires it.
func TestRecover_TimerArmedBySettle_SurvivesALaterCheckpoint(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchThenWaitMachine(fn)
	store := durable.NewMemStore()
	id := durable.InstanceID("fetch-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
		durable.WithCheckpointEvery[string, string, svcCtx](3),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	mustRunService(t, h, state.InvokeID("fetchThenWait", "loading", 0))

	first, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	for range 3 { // cross a checkpoint boundary after the settle has replayed
		if _, err = first.Fire(ctx, "poke"); err != nil {
			t.Fatalf("Fire(poke): %v", err)
		}
	}

	second, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	clk.Advance(6 * time.Second)
	if _, err = second.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := second.Instance().Current(); got != "done" {
		t.Fatalf("state %q after the second recovery, want done", got)
	}
}

// fetchChainMachine arms a timer on a service result, then chains a second one.
func fetchChainMachine(fn state.ServiceFn[svcCtx]) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("fetchChain").
		Service("fetch", fn).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok")).
		State("waiting").
		State("next").
		State("done").
		Initial("idle").
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("waiting").
		Transition("waiting").On("poke").GoTo("waiting").
		Transition("waiting").After(5 * time.Second).On("first").GoTo("next").
		Transition("next").After(5 * time.Second).On("second").GoTo("done").
		Quench()
}

// olderSettle splits a settle Record as an older journal wrote it: the Record without its
// clock read, and that read, which the older code left for the next Record to carry.
func olderSettle(settle durable.Record) (durable.Record, state.JournalEntry) {
	var read state.JournalEntry
	var kept []state.JournalEntry
	for _, e := range settle.Entries {
		if e.Kind == state.JournalClockRead {
			read = e
			continue
		}
		kept = append(kept, e)
	}
	settle.Entries = kept
	return settle, read
}

// clockReadAt is a recorded clock read d after epoch.
func clockReadAt(d time.Duration) state.JournalEntry {
	return state.JournalEntry{Kind: state.JournalClockRead, ClockUnixNano: epoch.Add(d).UnixNano()}
}

// startFetchChain starts a fetchChainMachine and settles its service.
func startFetchChain(
	t *testing.T,
	m *state.Machine[string, string, svcCtx],
	store durable.Store,
	id durable.InstanceID,
	opts []durable.Option[string, string, svcCtx],
) *durable.Handle[string, string, svcCtx] {
	t.Helper()
	ctx := context.Background()
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	mustRunService(t, h, state.InvokeID("fetchChain", "loading", 0))
	return h
}

// appendAll appends recs to store, failing the test on any error.
func appendAll(t *testing.T, store durable.Store, id durable.InstanceID, recs ...durable.Record) {
	t.Helper()
	for _, rec := range recs {
		if _, err := store.Append(context.Background(), id, rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

// TestRecover_OlderJournal_SettleReadInIdleTick_KeepsReplayClockAligned proves replay stays
// on the recorded readings when an older journal carries a settle's clock read in the idle
// tick Record after it. Replay consumes that read at the settle, so skipping the idle tick
// must not consume it again, or the last tick reads the live clock and fires a timer no
// Record holds.
func TestRecover_OlderJournal_SettleReadInIdleTick_KeepsReplayClockAligned(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchChainMachine(fn)
	clk := state.NewFakeClock(epoch)
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
	}
	store := durable.NewMemStore()
	h := startFetchChain(t, m, store, id, opts)
	clk.Advance(6 * time.Second)
	if _, err := h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	settle, settleRead := olderSettle(tail[1])
	firstIdle := durable.Record{Step: 2, Tick: true, Entries: []state.JournalEntry{settleRead, clockReadAt(time.Second)}}
	secondIdle := durable.Record{Step: 3, Tick: true, Entries: []state.JournalEntry{clockReadAt(2 * time.Second)}}
	tick := tail[2]
	tick.Step = 4
	older := durable.NewMemStore()
	if _, err = durable.NewRunner(m, older, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	appendAll(t, older, id, tail[0], settle, firstIdle, secondIdle, tick)

	clk.Advance(94 * time.Second) // the wall clock is far past every timer at recovery
	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "next" {
		t.Fatalf("recovered state %q, want next", got)
	}
}

// TestRecover_OlderJournal_TickPassesAnEarlierSettleRead proves a tail tick fires its
// timer when an older journal left a read at the front of its Record that no replayed
// settle consumed, because a checkpoint compacted the settle away.
func TestRecover_OlderJournal_TickPassesAnEarlierSettleRead(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchChainMachine(fn)
	clk := state.NewFakeClock(epoch)
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
	}
	// One run takes the checkpoint at the settle; a second, with none, gives the tick Record.
	checkpointed := durable.NewMemStore()
	startFetchChain(t, m, checkpointed, id, append(opts[:2:2], durable.WithCheckpointEvery[string, string, svcCtx](2)))
	snapshot, compacted, err := checkpointed.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(compacted) != 0 {
		t.Fatalf("tail holds %d records, want the settle checkpointed", len(compacted))
	}
	store := durable.NewMemStore()
	h := startFetchChain(t, m, store, id, opts)
	clk.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	tick := tail[2]
	tick.Entries = append([]state.JournalEntry{clockReadAt(0)}, tick.Entries...)
	older := durable.NewMemStore()
	if _, err = durable.NewRunner(m, older, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	appendAll(t, older, id, durable.Record{Step: 0}, durable.Record{Step: 1})
	if err = older.Checkpoint(ctx, id, snapshot, 1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	appendAll(t, older, id, tick)

	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "next" {
		t.Fatalf("recovered state %q, want next", got)
	}
	view, err := durable.StateAt(ctx, m, older, id, tick.Step, opts...)
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if got := view.Instance().Current(); got != "next" {
		t.Fatalf("StateAt state %q, want next", got)
	}
}

// TestRecover_OlderJournal_SettleSkipsAnUnusedRead proves a replayed settle arms from its
// own reading, not a stale one an older journal left in an earlier Record that no Absorb
// consumed.
func TestRecover_OlderJournal_SettleSkipsAnUnusedRead(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchChainMachine(fn)
	clk := state.NewFakeClock(epoch)
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
	}
	store := durable.NewMemStore()
	startFetchChain(t, m, store, id, opts)
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	fire := tail[0]
	fire.Entries = append([]state.JournalEntry{clockReadAt(0)}, fire.Entries...) // unused
	settle, _ := olderSettle(tail[1])
	older := durable.NewMemStore()
	if _, err = durable.NewRunner(m, older, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	appendAll(t, older, id, fire, settle,
		durable.Record{Step: 2, Tick: true, Entries: []state.JournalEntry{clockReadAt(time.Second)}})

	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	clk.Advance(5*time.Second + 500*time.Millisecond) // the timer is due at 6s, from the settle's read
	for range 2 {                                     // a misaligned replay leaves a reading for the first
		fired, err := recovered.Tick(ctx)
		if err != nil {
			t.Fatalf("Tick: %v", err)
		}
		if len(fired) != 0 {
			t.Fatalf("Tick fired %d timers before the deadline, want 0", len(fired))
		}
	}
}

// TestRecover_OlderJournal_SettleReadLostToARestart_LeavesTheNextStepsReads proves a
// read-less settle doesn't consume the next Record's reading when a restart lost its own.
// That reading stands in for the lost one, and the tick after it still fires what it
// recorded.
func TestRecover_OlderJournal_SettleReadLostToARestart_LeavesTheNextStepsReads(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchChainMachine(fn)
	clk := state.NewFakeClock(epoch)
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
	}
	store := durable.NewMemStore()
	h := startFetchChain(t, m, store, id, opts)
	clk.Advance(6 * time.Second)
	if _, err := h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	settle, _ := olderSettle(tail[1]) // the restart lost its read
	older := durable.NewMemStore()
	if _, err = durable.NewRunner(m, older, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tick := tail[2]
	tick.Step = 3
	appendAll(t, older, id, tail[0], settle,
		durable.Record{Step: 2, Tick: true, Entries: []state.JournalEntry{clockReadAt(time.Second)}}, tick)

	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "next" {
		t.Fatalf("recovered state %q, want next", got)
	}
	view, err := durable.StateAt(ctx, m, older, id, tick.Step, opts...)
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if got := view.Instance().Current(); got != "next" {
		t.Fatalf("StateAt state %q, want next", got)
	}
}

// TestRecover_OlderJournal_TailEndingOnASettle_CarriesItsRead proves recovery keeps the
// read it took for a settle that ended an older journal: the next step's Record carries
// it, so a later recovery arms that timer at the same instant.
func TestRecover_OlderJournal_TailEndingOnASettle_CarriesItsRead(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchChainMachine(fn)
	clk := state.NewFakeClock(epoch)
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
	}
	store := durable.NewMemStore()
	startFetchChain(t, m, store, id, opts)
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	settle, _ := olderSettle(tail[1])
	older := durable.NewMemStore()
	if _, err = durable.NewRunner(m, older, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	appendAll(t, older, id, tail[0], settle)

	clk.Advance(3 * time.Second)
	first, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	clk.Advance(6 * time.Second) // due at 8s, from the recovery's read
	if _, err = first.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := first.Instance().Current(); got != "next" {
		t.Fatalf("live state %q, want next", got)
	}
	second, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	if got := second.Instance().Current(); got != "next" {
		t.Fatalf("recovered state %q, want next", got)
	}
}

// TestRecover_OlderJournal_LeftoverReadStaysOutOfTheLiveClock proves a reading no step
// consumed at the end of a tail doesn't reach the live clock, so a timer armed after
// recovery measures from the real time.
func TestRecover_OlderJournal_LeftoverReadStaysOutOfTheLiveClock(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchChainMachine(fn)
	clk := state.NewFakeClock(epoch)
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
	}
	store := durable.NewMemStore()
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	fire := tail[0]
	fire.Entries = append([]state.JournalEntry{clockReadAt(0)}, fire.Entries...) // no Absorb used it
	older := durable.NewMemStore()
	if _, err = durable.NewRunner(m, older, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	appendAll(t, older, id, fire)

	clk.Advance(94 * time.Second)
	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	mustRunService(t, recovered, state.InvokeID("fetchChain", "loading", 0)) // arms 5s, due at 99s
	clk.Advance(time.Second)
	fired, err := recovered.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("Tick fired %d timers before the deadline, want 0", len(fired))
	}
}

// TestRecover_OlderJournal_SettleTimerReachesTheNextCheckpoint proves a timer armed by an
// older settle Record, which holds no clock read, still reaches the Handle's table, so the
// next checkpoint persists it and a second recovery fires it.
func TestRecover_OlderJournal_SettleTimerReachesTheNextCheckpoint(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchChainMachine(fn)
	clk := state.NewFakeClock(epoch)
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithRunnerClock[string, string, svcCtx](clk),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
		durable.WithCheckpointEvery[string, string, svcCtx](6),
	}
	store := durable.NewMemStore()
	startFetchChain(t, m, store, id, opts)
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	settle, settleRead := olderSettle(tail[1])
	older := durable.NewMemStore()
	if _, err = durable.NewRunner(m, older, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	appendAll(t, older, id, tail[0], settle,
		durable.Record{Step: 2, Tick: true, Entries: []state.JournalEntry{settleRead, clockReadAt(time.Second)}})

	first, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	for range 3 { // cross a checkpoint boundary after the settle has replayed
		if _, err = first.Fire(ctx, "poke"); err != nil {
			t.Fatalf("Fire(poke): %v", err)
		}
	}
	second, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	clk.Advance(6 * time.Second)
	if _, err = second.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := second.Instance().Current(); got != "next" {
		t.Fatalf("state %q after the second recovery, want next", got)
	}
}

// TestService_ResultFromCanceledTurn_NotCommittedOnRecover proves a service result
// whose settle is canceled with the turn's context, and so never reaches the live
// instance, is not committed by recovery either.
func TestService_ResultFromCanceledTurn_NotCommittedOnRecover(t *testing.T) {
	ctx := context.Background()
	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		cancel() // the turn's deadline passes while the call is in flight
		return "late", nil
	}
	m := fetchMachine(fn)
	store := durable.NewMemStore()
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}
	runner := durable.NewRunner(m, store, opts...)
	h, err := runner.Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	_, _, _ = h.RunService(turnCtx, state.InvokeID("fetch", "loading", 0))

	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// Compare state, not bytes: live keeps the Trace of the refused settle.
	if live, got := h.Instance().Current(), recovered.Instance().Current(); got != live || live != "loading" {
		t.Fatalf("recovered %q, live %q, want both loading: the canceled turn's result was committed", got, live)
	}
	if notes := recovered.Instance().Entity().Notes; len(notes) != 0 {
		t.Fatalf("recovered notes %v, want none", notes)
	}
}

// TestService_CanceledTurn_RecoverRerunsTheService proves the canceled result isn't
// journaled, the Handle refuses further steps, and a recovered Handle can run the
// service to completion.
func TestService_CanceledTurn_RecoverRerunsTheService(t *testing.T) {
	ctx := context.Background()
	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	calls := 0
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		calls++
		cancel()
		return "late", nil
	}
	m := fetchMachine(fn)
	store := durable.NewMemStore()
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	invoke := state.InvokeID("fetch", "loading", 0)
	if _, _, err = h.RunService(turnCtx, invoke); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunService = %v, want context.Canceled", err)
	}
	if _, err = h.Fire(ctx, "start"); !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("Fire after the cancel = %v, want ErrHandleFailed", err)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, ok, err := recovered.RunService(ctx, invoke); err != nil || !ok {
		t.Fatalf("RunService after Recover = (%v, %v)", ok, err)
	}
	if calls != 2 {
		t.Fatalf("service ran %d times, want 2", calls)
	}
}

// TestRecover_CanceledContext_DoesNotLoseARecordedResult proves a canceled Recover
// reports the cancellation rather than a Handle short of the journal.
func TestRecover_CanceledContext_DoesNotLoseARecordedResult(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchMachine(fn)
	store := durable.NewMemStore()
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	mustRunService(t, h, state.InvokeID("fetch", "loading", 0))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = durable.Recover(canceled, m, store, id, opts...); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover = %v, want context.Canceled", err)
	}
}

// TestStateAt_CanceledContext_ReportsIt proves a canceled historical read reports the
// cancellation, not a view short of the journal.
func TestStateAt_CanceledContext_ReportsIt(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := fetchMachine(fn)
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("fetch-1")
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	mustRunService(t, h, state.InvokeID("fetch", "loading", 0))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = durable.StateAt(canceled, m, store, id, 0, opts...); !errors.Is(err, context.Canceled) {
		t.Fatalf("StateAt = %v, want context.Canceled", err)
	}
}

// TestRecover_CanceledContext_ChainedServices_ReportsIt proves a canceled replay of two
// chained services reports the cancellation, not a journal that looks corrupt.
func TestRecover_CanceledContext_ChainedServices_ReportsIt(t *testing.T) {
	ctx := context.Background()
	var calls int64
	fn := counterService(&calls)
	m := chainMachine(fn, fn)
	store := durable.NewMemStore()
	id := durable.InstanceID("chain-1")
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](chainRegistry(fn, fn))}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("first"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	mustRunService(t, h, state.InvokeID("chain", "first", 0))
	mustRunService(t, h, state.InvokeID("chain", "second", 0))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = durable.Recover(canceled, m, store, id, opts...); !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover = %v, want context.Canceled", err)
	}
}

// TestTick_ContextEndedPartway_RecordsNothing proves a due timer the kernel refused isn't
// journaled: the Handle fails and the journal doesn't grow.
func TestTick_ContextEndedPartway_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	m := timerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	clk := &cancelingClock{Clock: state.NewFakeClock(epoch)}
	opts := []durable.Option[string, string, *timerCtx]{durable.WithRunnerClock[string, string, *timerCtx](clk)}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	_, before, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	clk.Clock.(*state.FakeClock).Advance(6 * time.Second)
	clk.onNow = cancel // the turn ends as the Tick reads the clock
	if _, err = h.Tick(turn); !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Tick = %v, want ErrHandleFailed wrapping context.Canceled", err)
	}
	clk.onNow = nil
	_, after, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("journal grew from %d to %d records", len(before), len(after))
	}
}

// cancelingClock runs onNow once on the next clock read.
type cancelingClock struct {
	state.Clock
	onNow func()
}

func (c *cancelingClock) Now() time.Time {
	if c.onNow != nil {
		f := c.onNow
		c.onNow = nil
		f()
	}
	return c.Clock.Now()
}

// TestRunService_AssignSeesTheDecodedResult proves a typed result reaches Assign in the
// form a replay supplies, so the two can't differ.
func TestRunService_AssignSeesTheDecodedResult(t *testing.T) {
	type user struct{ Email string }
	ctx := context.Background()
	var seen []any
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return user{Email: "a@b.c"}, nil }
	m := state.ForgeFor[svcCtx]("decoded").
		Service("fetch", fn).
		Reducer("note", func(in state.AssignCtx[svcCtx]) svcCtx {
			seen = append(seen, in.Event)
			return in.Entity
		}).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").Final().
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").Assign("note").
		Transition("loading").On("fail").GoTo("failed").
		Quench()
	store := durable.NewMemStore()
	id := durable.InstanceID("d-1")
	opts := []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	mustRunService(t, h, state.InvokeID("decoded", "loading", 0))
	if _, err = durable.Recover(ctx, m, store, id, opts...); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(seen) != 2 || fmt.Sprintf("%T:%v", seen[0], seen[0]) != fmt.Sprintf("%T:%v", seen[1], seen[1]) {
		t.Fatalf("live and replay saw %#v", seen)
	}
}
