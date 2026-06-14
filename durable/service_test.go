package durable_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

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
