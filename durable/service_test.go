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
	before := tailLen(t, store, id)
	if _, _, err = h.RunService(turnCtx, invoke); !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunService = %v, want ErrHandleFailed wrapping context.Canceled", err)
	}
	if after := tailLen(t, store, id); after != before {
		t.Fatalf("journal grew from %d to %d records, want the canceled result unrecorded", before, after)
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

// TestService_ContextEndedDuringAbsorb_RecordsNothing proves a service that settles
// cleanly, and whose context then ends while the settle's actor spawn is absorbed,
// fails the Handle and records nothing: the spawn's onError Fire was refused, so the
// live instance never applied what the journal would otherwise commit.
func TestService_ContextEndedDuringAbsorb_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	turnCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return "ok", nil }
	spawn := func(map[string]any) (state.ActorInstance, error) {
		cancel() // the turn's deadline passes while the settle's spawn is absorbed
		return nil, errors.New("worker unavailable")
	}
	m := state.ForgeFor[svcCtx]("absorb").
		Service("fetch", fn).
		Actor("spawnChild").
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok")).
		State("spawning").InvokeActor("spawnChild", state.WithInvokeOnError("spawnFailed")).
		State("failed").Final().
		Initial("idle").
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("spawning").
		Transition("spawning").On("spawnFailed").GoTo("failed").
		Quench()
	store := durable.NewMemStore()
	h, err := durable.NewRunner(m, store,
		durable.WithServiceRegistry[string, string, svcCtx](state.NewRegistry[svcCtx]().Service("fetch", fn)),
		durable.WithActorPalette[string, string, svcCtx](map[string]state.ActorBehavior{"spawnChild": spawn})).
		Start(ctx, "absorb-1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	before := tailLen(t, store, "absorb-1")
	_, ran, err := h.RunService(turnCtx, state.InvokeID("absorb", "loading", 0))
	if !ran || !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("RunService = %v, %v; want true, ErrHandleFailed", ran, err)
	}
	if after := tailLen(t, store, "absorb-1"); after != before {
		t.Fatalf("journal grew from %d to %d records, want nothing recorded", before, after)
	}
}

// countingClock counts clock reads and runs onRead on each.
type countingClock struct {
	state.Clock
	n      int
	onRead func(n int)
}

func (c *countingClock) Now() time.Time {
	c.n++
	if c.onRead != nil {
		c.onRead(c.n)
	}
	return c.Clock.Now()
}

// TestTick_ContextEndedAfterTheTimersApplied_RecordsNothing proves a Tick whose context
// ends after its due timer's Fire applied still fails the Handle and records nothing,
// as RunService and DeliverToActor do, and Recover fires the timer again.
func TestTick_ContextEndedAfterTheTimersApplied_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	m := chainedTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("chain-1")
	fc := state.NewFakeClock(epoch)
	clk := &countingClock{Clock: fc}
	opts := []durable.Option[string, string, *timerCtx]{durable.WithRunnerClock[string, string, *timerCtx](clk)}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	before := tailLen(t, store, id)
	fc.Advance(4 * time.Second)
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	base := clk.n
	clk.onRead = func(n int) {
		if n == base+3 { // the scheduler's last read, after the first timer applied
			cancel()
		}
	}
	results, err := h.Tick(turn)
	clk.onRead = nil
	if turn.Err() == nil {
		t.Fatal("the test didn't end the context")
	}
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("Tick results %v, want one applied timer", results)
	}
	if !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Tick = %v, want ErrHandleFailed wrapping context.Canceled", err)
	}
	if after := tailLen(t, store, id); after != before {
		t.Fatalf("journal grew from %d to %d records, want nothing recorded", before, after)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "first" {
		t.Fatalf("recovered state %q, want first: the unrecorded tick must fire again", got)
	}
}

// TestService_RefusedByADerivedContext_RecordsNothing proves a settle refused because a
// context middleware derived from the caller's ended, while the caller's is still live,
// fails the Handle and records nothing.
func TestService_RefusedByADerivedContext_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	fn := counterService(new(int64))
	cancelOnOK := func(next state.FireFunc[string, string, svcCtx]) state.FireFunc[string, string, svcCtx] {
		return func(ctx context.Context, event string) state.FireResult[string] {
			if event == "ok" {
				derived, cancel := context.WithCancel(ctx)
				cancel()
				return next(derived, event)
			}
			return next(ctx, event)
		}
	}
	m := state.ForgeFor[svcCtx]("derived").
		Use(cancelOnOK).
		Service("fetch", fn).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok")).
		State("ready").Final().
		Initial("idle").
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("ready").
		Quench()
	store := durable.NewMemStore()
	id := durable.InstanceID("derived-1")
	h, err := durable.NewRunner(m, store, durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))).
		Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	before := tailLen(t, store, id)
	if _, _, err = h.RunService(ctx, state.InvokeID("derived", "loading", 0)); !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunService = %v, want ErrHandleFailed wrapping context.Canceled", err)
	}
	if after := tailLen(t, store, id); after != before {
		t.Fatalf("journal grew from %d to %d records, want nothing recorded", before, after)
	}
}

// TestRecover_DoesNotRearmASuperstatesSettledService proves a service a compound state
// declares, which settled into one of the compound's children, isn't in flight after
// Recover, so RunService doesn't run it again: not from the journal, and not from a
// checkpoint taken after the settle, where the compound is still active.
func TestRecover_DoesNotRearmASuperstatesSettledService(t *testing.T) {
	ctx := context.Background()
	// every=-1 runs live with no checkpoint, then checkpoints from each recovered Handle,
	// so the checkpoint comes from a replay of the settle.
	for _, every := range []int{0, 1, -1} {
		t.Run(fmt.Sprintf("every=%d", every), func(t *testing.T) {
			var runs int64
			fetch := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) {
				atomic.AddInt64(&runs, 1)
				return "ok", nil
			}
			m := state.ForgeFor[stepCtx]("nestSvc").
				Service("fetch", fetch).
				SuperState("outer").Invoke("fetch", state.WithInvokeOnDone("fetched")).
				SubState("a").
				SubState("b").
				Initial("a").
				EndSuperState().
				Initial("outer").
				Transition("a").On("fetched").GoTo("b").
				Transition("b").On("poke").GoTo("b").
				Quench()
			opts := []stepOption{durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fetch))}
			live := opts
			switch {
			case every > 0:
				opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
				live = opts
			case every < 0:
				opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](1))
			}
			store, id := durable.NewMemStore(), durable.InstanceID("nest-svc")
			svcID := state.InvokeID("nestSvc", "outer", 0)
			h, err := durable.NewRunner(m, store, live...).Start(ctx, id, stepCtx{}, state.WithInitialState("a"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, ran, err := h.RunService(ctx, svcID); err != nil || !ran {
				t.Fatalf("RunService = %v, %v", ran, err)
			}
			if got := h.Instance().Current(); got != "b" {
				t.Fatalf("state = %q, want b after the settle", got)
			}
			for round := 1; round <= 2; round++ {
				recovered, err := durable.Recover(ctx, m, store, id, opts...)
				if err != nil {
					t.Fatalf("Recover %d: %v", round, err)
				}
				if _, ran, runErr := recovered.RunService(ctx, svcID); runErr != nil || ran {
					t.Fatalf("RunService after Recover %d = %v, %v, want the settled service not in flight", round, ran, runErr)
				}
				if n := atomic.LoadInt64(&runs); n != 1 {
					t.Fatalf("the service ran %d times, want once", n)
				}
				// A step on the recovered Handle checkpoints again, from its own view.
				if _, err = recovered.Fire(ctx, "poke"); err != nil {
					t.Fatalf("Fire(poke) after Recover %d: %v", round, err)
				}
			}
		})
	}
}

// TestRecover_StartsAServiceThatNeverSettled proves a checkpoint records as settled only
// a service that settled, not every declared service that isn't in flight, so Recover
// starts the rest: one whose state a Tick's timer entered (a Tick starts no service),
// and one whose state was re-entered after it settled, which is a new service, whether
// a Fire re-entered it, starting it live, or a Tick did. The checkpoint is the live
// run's, or one a recovered Handle takes after replaying the run.
func TestRecover_StartsAServiceThatNeverSettled(t *testing.T) {
	ctx := context.Background()
	for _, how := range []string{"timer entered", "Fire re-entered", "timer re-entered"} {
		for _, where := range []string{"live checkpoint", "replayed checkpoint"} {
			t.Run(how+"/"+where, func(t *testing.T) {
				fetch := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { return "ok", nil }
				m := state.ForgeFor[stepCtx]("unsettled").
					Service("fetch", fetch).
					State("idle").
					State("waiting").
					State("fetching").Invoke("fetch", state.WithInvokeOnDone("fetched")).
					Initial("idle").
					Transition("idle").On("begin").GoTo("waiting").
					Transition("waiting").On("go").GoTo("fetching").
					Transition("waiting").After(10 * time.Second).On("elapsed").GoTo("fetching").
					Transition("fetching").On("fetched").GoTo("fetching").
					Transition("fetching").On("again").GoTo("fetching").Reenter().
					Transition("fetching").After(10 * time.Second).On("redo").GoTo("fetching").Reenter().
					Transition("fetching").On("poke").GoTo("fetching").
					Quench()
				clk := state.NewFakeClock(epoch)
				opts := []stepOption{
					durable.WithRunnerClock[string, string, stepCtx](clk),
					durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fetch)),
				}
				checkpointing := append(opts, durable.WithCheckpointEvery[string, string, stepCtx](1))
				live := checkpointing
				if where == "replayed checkpoint" {
					live = opts
				}
				store, id := durable.NewMemStore(), durable.InstanceID("unsettled")
				svcID := state.InvokeID("unsettled", "fetching", 0)
				h, err := durable.NewRunner(m, store, live...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				if _, err = h.Fire(ctx, "begin"); err != nil {
					t.Fatalf("Fire(begin): %v", err)
				}
				switch how {
				case "timer entered":
					clk.Advance(10 * time.Second)
					if _, err = h.Tick(ctx); err != nil {
						t.Fatalf("Tick: %v", err)
					}
				default:
					if _, err = h.Fire(ctx, "go"); err != nil {
						t.Fatalf("Fire(go): %v", err)
					}
					if _, ran, runErr := h.RunService(ctx, svcID); !ran || runErr != nil {
						t.Fatalf("RunService = %v, %v", ran, runErr)
					}
					if how == "Fire re-entered" {
						if _, err = h.Fire(ctx, "again"); err != nil {
							t.Fatalf("Fire(again): %v", err)
						}
					} else {
						clk.Advance(10 * time.Second)
						if results, tickErr := h.Tick(ctx); tickErr != nil || len(results) != 1 {
							t.Fatalf("Tick = %d results, %v, want the redo timer to fire", len(results), tickErr)
						}
					}
				}
				if got := h.Instance().Current(); got != "fetching" {
					t.Fatalf("state = %q, want fetching", got)
				}
				if where == "replayed checkpoint" {
					// Replay the journal, then checkpoint from the recovered Handle's view.
					if h, err = durable.Recover(ctx, m, store, id, checkpointing...); err != nil {
						t.Fatalf("Recover from the journal: %v", err)
					}
				}
				if _, err = h.Fire(ctx, "poke"); err != nil {
					t.Fatalf("Fire(poke): %v", err)
				}
				recovered, err := durable.Recover(ctx, m, store, id, checkpointing...)
				if err != nil {
					t.Fatalf("Recover: %v", err)
				}
				if _, ran, runErr := recovered.RunService(ctx, svcID); !ran || runErr != nil {
					t.Fatalf("RunService after Recover = %v, %v, want the service in flight: it never settled in this activation of fetching", ran, runErr)
				}
			})
		}
	}
}
