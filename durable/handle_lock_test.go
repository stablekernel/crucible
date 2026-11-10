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

// waitFor fails the test if done doesn't close within a few seconds, so a deadlock
// fails the test instead of hanging it.
func waitFor(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s didn't finish: deadlocked", what)
	}
}

// TestHandle_EffectHandlerFiresBack_DoesNotDeadlock proves an effect handler can call
// back into its own Handle: effects dispatch after the step releases the lock.
func TestHandle_EffectHandlerFiresBack_DoesNotDeadlock(t *testing.T) {
	ctx := context.Background()
	m := effectMachine()
	store := durable.NewMemStore()
	var h *durable.Handle[string, string, *runCtx]
	var firedBack atomic.Bool
	var backErr error
	handler := func(ctx context.Context, _ string, _ state.Effect) error {
		if firedBack.CompareAndSwap(false, true) {
			_, backErr = h.Fire(ctx, "bill")
		}
		return nil
	}
	opt := durable.WithEffectHandler[string, string, *runCtx](handler)
	h, err := durable.NewRunner(m, store, opt).Start(ctx, "e-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err = h.Fire(ctx, "go")
	}()
	waitFor(t, done, "Fire(go)")
	if err != nil || backErr != nil {
		t.Fatalf("Fire(go) = %v, the handler's Fire(bill) = %v; want both nil", err, backErr)
	}
	if got := h.Instance().Current(); got != "billed" {
		t.Fatalf("live state %q, want billed", got)
	}
	recovered, err := durable.Recover(ctx, m, store, "e-1", opt)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "billed" {
		t.Fatalf("recovered state %q, want billed", got)
	}
}

// blockingFetch starts the single-service machine with a service that signals entered
// and then waits for release, so a test can hold a step open.
func blockingFetch(t *testing.T, entered chan<- struct{}, release <-chan struct{}) *durable.Handle[string, string, svcCtx] {
	t.Helper()
	ctx := context.Background()
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		entered <- struct{}{}
		<-release
		return "done", nil
	}
	h, err := durable.NewRunner(fetchMachine(fn), durable.NewMemStore(),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))).
		Start(ctx, "fetch-1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	return h
}

// TestHandle_WaitingStep_StopsAtItsDeadline proves a step waiting for the lock returns
// its context's error when the context ends, instead of waiting out a slow step.
func TestHandle_WaitingStep_StopsAtItsDeadline(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	h := blockingFetch(t, entered, release)
	running := make(chan struct{})
	go func() {
		defer close(running)
		mustRunService(t, h, fetchID)
	}()
	<-entered // the service holds the step lock

	waits := map[string]func(context.Context) error{
		"Fire": func(ctx context.Context) error { _, err := h.Fire(ctx, "ok"); return err },
		"Tick": func(ctx context.Context) error { _, err := h.Tick(ctx); return err },
		"View": func(ctx context.Context) error { return h.View(ctx, func(*state.Instance[string, string, svcCtx]) {}) },
	}
	for name, wait := range waits {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		returned := make(chan struct{})
		var err error
		go func() {
			defer close(returned)
			err = wait(ctx)
		}()
		waitFor(t, returned, name)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s while the lock is held = %v, want context.DeadlineExceeded", name, err)
		}
	}
	close(release)
	waitFor(t, running, "RunService")
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("state %q, want ready: a timed-out step changed the instance", got)
	}
}

// TestHandle_ServiceCallsBackWithItsContext_IsRefused proves a service that calls back
// into its own Handle with the context its step passed gets ErrReentrantStep at once
// instead of deadlocking on the lock its step holds.
func TestHandle_ServiceCallsBackWithItsContext_IsRefused(t *testing.T) {
	ctx := context.Background()
	var h *durable.Handle[string, string, svcCtx]
	var fireErr, viewErr error
	fn := func(ctx context.Context, _ state.ServiceCtx[svcCtx]) (any, error) {
		_, fireErr = h.Fire(ctx, "ok")
		viewErr = h.View(ctx, func(*state.Instance[string, string, svcCtx]) {})
		return "done", nil
	}
	h, err := durable.NewRunner(fetchMachine(fn), durable.NewMemStore(),
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))).
		Start(ctx, "fetch-1", svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		mustRunService(t, h, fetchID)
	}()
	waitFor(t, done, "RunService")
	if !errors.Is(fireErr, durable.ErrReentrantStep) || !errors.Is(viewErr, durable.ErrReentrantStep) {
		t.Fatalf("the service's Fire = %v and View = %v, want ErrReentrantStep", fireErr, viewErr)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("state %q, want ready", got)
	}
}

// TestHandle_Checkpoint_WaitsForAnEarlierStepsDispatch proves a checkpoint doesn't
// compact a step whose effect another goroutine is still dispatching, and is written
// once that dispatch lands.
func TestHandle_Checkpoint_WaitsForAnEarlierStepsDispatch(t *testing.T) {
	ctx := context.Background()
	store := durable.NewMemStore()
	entered, release := make(chan struct{}), make(chan struct{})
	var waited atomic.Bool
	handler := func(context.Context, string, state.Effect) error {
		if waited.CompareAndSwap(false, true) { // only step 0's notify waits
			close(entered)
			<-release
		}
		return nil
	}
	h, err := durable.NewRunner(effectMachine(), store,
		durable.WithEffectHandler[string, string, *runCtx](handler),
		durable.WithCheckpointEvery[string, string, *runCtx](2)).
		Start(ctx, "e-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	first := make(chan struct{})
	var firstErr error
	go func() {
		defer close(first)
		_, firstErr = h.Fire(ctx, "go") // step 0, its notify still dispatching
	}()
	<-entered
	if _, err = h.Fire(ctx, "bill"); err != nil { // step 1, where a checkpoint falls due
		t.Fatalf("Fire(bill): %v", err)
	}
	if _, tail, err := store.Load(ctx, "e-1"); err != nil || len(tail) != 2 {
		t.Fatalf("while step 0 dispatches: tail of %d, err %v; want both steps kept", len(tail), err)
	}
	close(release)
	waitFor(t, first, "Fire(go)")
	if firstErr != nil {
		t.Fatalf("Fire(go): %v", firstErr)
	}
	if _, tail, err := store.Load(ctx, "e-1"); err != nil || len(tail) != 0 {
		t.Fatalf("after step 0 dispatched: tail of %d, err %v; want the checkpoint written", len(tail), err)
	}
}

// TestHandle_FailedDispatch_FailsTheHandle_AndRecoverRedispatches proves a failed
// effect fails the Handle, so no later checkpoint compacts it away, and Recover
// dispatches it again.
func TestHandle_FailedDispatch_FailsTheHandle_AndRecoverRedispatches(t *testing.T) {
	ctx := context.Background()
	m := effectMachine()
	store := durable.NewMemStore()
	rec := newEffectRecorder()
	rec.failKinds["demo.notify"] = errors.New("mail server down")
	opts := []durable.Option[string, string, *runCtx]{
		durable.WithEffectHandler[string, string, *runCtx](rec.handler()),
		durable.WithCheckpointEvery[string, string, *runCtx](2),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, "e-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, err = h.Fire(ctx, "go")
	if !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("Fire(go) = %v, want ErrHandleFailed wrapping ErrEffectDispatch", err)
	}
	if _, err = h.Fire(ctx, "bill"); !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("Fire(bill) after the failure = %v, want ErrHandleFailed", err)
	}
	delete(rec.failKinds, "demo.notify")
	recovered, err := durable.Recover(ctx, m, store, "e-1", opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := rec.totalApplies(); got != 1 {
		t.Fatalf("Recover applied %d effects, want the failed notify once", got)
	}
	if _, err = recovered.Fire(ctx, "bill"); err != nil {
		t.Fatalf("Fire(bill) on the recovered Handle: %v", err)
	}
}

// TestHandle_View_DoesNotRaceSteps proves View reads under the step lock: run with
// -race, a read outside it would be reported against the concurrent Fires.
func TestHandle_View_DoesNotRaceSteps(t *testing.T) {
	ctx := context.Background()
	h, err := durable.NewRunner(pollingMachine(), durable.NewMemStore()).
		Start(ctx, "poll-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			if _, err := h.Fire(ctx, "poke"); err != nil {
				t.Errorf("Fire(poke): %v", err)
				return
			}
		}
	}()
	var last int
	for range 100 {
		if err := h.View(ctx, func(inst *state.Instance[string, string, *runCtx]) {
			last = inst.Entity().Count
		}); err != nil {
			t.Fatalf("View: %v", err)
		}
	}
	waitFor(t, done, "the Fires")
	if err := h.View(ctx, func(inst *state.Instance[string, string, *runCtx]) { last = inst.Entity().Count }); err != nil || last != 100 {
		t.Fatalf("View after the Fires: count %d, err %v; want 100", last, err)
	}
}

// panicAppendStore panics on the next Append once armed.
type panicAppendStore struct {
	durable.Store
	armed atomic.Bool
}

func (s *panicAppendStore) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	if s.armed.CompareAndSwap(true, false) {
		panic("store exploded")
	}
	return s.Store.Append(ctx, id, rec, opts...)
}

// TestHandle_PanicUnderTheLock_ReleasesIt proves a panic while Fire holds the step
// lock doesn't leave the lock held: the next step still gets it.
func TestHandle_PanicUnderTheLock_ReleasesIt(t *testing.T) {
	ctx := context.Background()
	store := &panicAppendStore{Store: durable.NewMemStore()}
	h, err := durable.NewRunner(pollingMachine(), store).Start(ctx, "poll-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	store.armed.Store(true)
	func() {
		defer func() { _ = recover() }()
		_, _ = h.Fire(ctx, "start")
	}()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := h.View(waitCtx, func(*state.Instance[string, string, *runCtx]) {}); errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("View after the panic waited out its deadline: the step lock was left held")
	}
}

// TestHandle_PanickingEffectHandler_FailsTheHandle proves a handler that panics fails
// the Handle like one that returns an error, instead of leaving its step dispatching
// forever, which would silently stop every later checkpoint.
func TestHandle_PanickingEffectHandler_FailsTheHandle(t *testing.T) {
	ctx := context.Background()
	m := effectMachine()
	store := durable.NewMemStore()
	var panicked atomic.Bool
	handler := func(context.Context, string, state.Effect) error {
		if panicked.CompareAndSwap(false, true) {
			panic("handler exploded")
		}
		return nil
	}
	opts := []durable.Option[string, string, *runCtx]{
		durable.WithEffectHandler[string, string, *runCtx](handler),
		durable.WithCheckpointEvery[string, string, *runCtx](2),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, "e-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, err = h.Fire(ctx, "go")
	if !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("Fire(go) = %v, want ErrHandleFailed wrapping ErrEffectDispatch", err)
	}
	recovered, err := durable.Recover(ctx, m, store, "e-1", opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, err = recovered.Fire(ctx, "bill"); err != nil { // step 1: a checkpoint falls due
		t.Fatalf("Fire(bill) on the recovered Handle: %v", err)
	}
	if _, tail, err := store.Load(ctx, "e-1"); err != nil || len(tail) != 0 {
		t.Fatalf("tail of %d, err %v; want the checkpoint written", len(tail), err)
	}
}

// failingMemStore is a MemStore, dispatch tracking included, that fails the next
// Append once armed.
type failingMemStore struct {
	*durable.MemStore
	armed atomic.Bool
}

func (s *failingMemStore) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	if s.armed.CompareAndSwap(true, false) {
		return 0, errors.New("store unavailable")
	}
	return s.MemStore.Append(ctx, id, rec, opts...)
}

// TestHandle_Drain_WaitsForOtherDispatches proves a call that fails the Handle returns
// at once, and Drain returns only once another goroutine's effect has finished
// sending, so a host that recovers after Drain can't send that effect a second time.
func TestHandle_Drain_WaitsForOtherDispatches(t *testing.T) {
	ctx := context.Background()
	store := &failingMemStore{MemStore: durable.NewMemStore()}
	entered, release := make(chan struct{}), make(chan struct{})
	var waited atomic.Bool
	handler := func(context.Context, string, state.Effect) error {
		if waited.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}
	h, err := durable.NewRunner(effectMachine(), store,
		durable.WithEffectHandler[string, string, *runCtx](handler)).
		Start(ctx, "e-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	first := make(chan struct{})
	go func() {
		defer close(first)
		_, _ = h.Fire(ctx, "go") // its notify is still sending
	}()
	<-entered
	store.armed.Store(true)
	failed := make(chan error, 1)
	go func() { _, fireErr := h.Fire(ctx, "bill"); failed <- fireErr }() // its Append fails
	select {
	case err := <-failed:
		if !errors.Is(err, durable.ErrHandleFailed) {
			t.Fatalf("Fire(bill) = %v, want ErrHandleFailed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Fire(bill) waited for another goroutine's dispatch; it should return at once")
	}
	drained := make(chan error, 1)
	go func() { drained <- h.Drain(ctx) }()
	select {
	case err := <-drained:
		t.Fatalf("Drain returned %v while step 0's effect was still sending", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("Drain = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Drain never returned after the dispatch finished")
	}
	waitFor(t, first, "Fire(go)")
}

// TestHandle_HandlerFailsTheHandleWithAnotherContext_DoesNotHang proves an effect handler
// that calls back with a context other than its own, and fails the Handle, gets
// ErrHandleFailed back at once instead of waiting on its own dispatch.
func TestHandle_HandlerFailsTheHandleWithAnotherContext_DoesNotHang(t *testing.T) {
	ctx := context.Background()
	store := &failingMemStore{MemStore: durable.NewMemStore()}
	var h *durable.Handle[string, string, *runCtx]
	var once atomic.Bool
	var backErr error
	handler := func(context.Context, string, state.Effect) error {
		if once.CompareAndSwap(false, true) {
			store.armed.Store(true)
			_, backErr = h.Fire(context.Background(), "bill") // not the handler's context
		}
		return nil
	}
	h, err := durable.NewRunner(effectMachine(), store,
		durable.WithEffectHandler[string, string, *runCtx](handler)).
		Start(ctx, "e-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = h.Fire(ctx, "go")
	}()
	waitFor(t, done, "Fire(go)")
	if !errors.Is(backErr, durable.ErrHandleFailed) {
		t.Fatalf("the handler's Fire = %v, want ErrHandleFailed", backErr)
	}
	if err := h.Drain(ctx); err != nil {
		t.Fatalf("Drain after the dispatch finished = %v, want nil", err)
	}
}

// TestHandle_DrainFromAHandler_IsRefused proves Drain called with an effect handler's
// own context returns ErrReentrantStep instead of waiting on its own dispatch.
func TestHandle_DrainFromAHandler_IsRefused(t *testing.T) {
	ctx := context.Background()
	var h *durable.Handle[string, string, *runCtx]
	var drainErr error
	var once atomic.Bool
	handler := func(ctx context.Context, _ string, _ state.Effect) error {
		if once.CompareAndSwap(false, true) {
			drainErr = h.Drain(ctx)
		}
		return nil
	}
	h, err := durable.NewRunner(effectMachine(), durable.NewMemStore(),
		durable.WithEffectHandler[string, string, *runCtx](handler)).
		Start(ctx, "e-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = h.Fire(ctx, "go")
	}()
	waitFor(t, done, "Fire(go)")
	if !errors.Is(drainErr, durable.ErrReentrantStep) {
		t.Fatalf("Drain from the handler = %v, want ErrReentrantStep", drainErr)
	}
}

// TestHandle_View_AfterAFailure_IsRefused proves View refuses a failed Handle without
// running its function.
func TestHandle_View_AfterAFailure_IsRefused(t *testing.T) {
	ctx := context.Background()
	store := &failingMemStore{MemStore: durable.NewMemStore()}
	h, err := durable.NewRunner(pollingMachine(), store).Start(ctx, "poll-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	store.armed.Store(true)
	if _, err = h.Fire(ctx, "start"); !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("Fire = %v, want ErrHandleFailed", err)
	}
	ran := false
	if err = h.View(ctx, func(*state.Instance[string, string, *runCtx]) { ran = true }); !errors.Is(err, durable.ErrHandleFailed) || ran {
		t.Fatalf("View = %v (ran %v), want ErrHandleFailed without running", err, ran)
	}
}

// blockingMemStore is a MemStore whose next Append, once armed, signals entered and
// waits for release.
type blockingMemStore struct {
	*durable.MemStore
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (s *blockingMemStore) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	if s.armed.CompareAndSwap(true, false) {
		close(s.entered)
		<-s.release
	}
	return s.MemStore.Append(ctx, id, rec, opts...)
}

// TestHandle_StepAfterAFailure_LeavesItsEffectsForRecover proves a step that already
// held the lock when another goroutine's dispatch failed the Handle doesn't send its
// own effects: the failing caller may already be recovering, and Recover sends them.
func TestHandle_StepAfterAFailure_LeavesItsEffectsForRecover(t *testing.T) {
	ctx := context.Background()
	m := effectMachine()
	store := &blockingMemStore{MemStore: durable.NewMemStore(), entered: make(chan struct{}), release: make(chan struct{})}
	handlerIn, failNow := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var first atomic.Bool
	handler := func(context.Context, string, state.Effect) error {
		if first.CompareAndSwap(false, true) { // step 0's notify waits, then fails
			close(handlerIn)
			<-failNow
			return errors.New("mail server down")
		}
		calls.Add(1)
		return nil
	}
	opt := durable.WithEffectHandler[string, string, *runCtx](handler)
	h, err := durable.NewRunner(m, store, opt).Start(ctx, "e-1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	firstDone := make(chan error, 1)
	go func() { _, fireErr := h.Fire(ctx, "go"); firstDone <- fireErr }()
	<-handlerIn
	store.armed.Store(true)
	secondDone := make(chan error, 1)
	go func() { _, fireErr := h.Fire(ctx, "bill"); secondDone <- fireErr }()
	<-store.entered // step 1 holds the lock, in its Append
	close(failNow)
	if got := <-firstDone; !errors.Is(got, durable.ErrHandleFailed) {
		t.Fatalf("Fire(go) = %v, want ErrHandleFailed", got)
	}
	close(store.release)
	if got := <-secondDone; !errors.Is(got, durable.ErrHandleFailed) {
		t.Fatalf("Fire(bill) = %v, want ErrHandleFailed", got)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("step 1 sent %d effects after the Handle failed, want 0", n)
	}
	if _, err = durable.Recover(ctx, m, store.MemStore, "e-1", opt); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n := calls.Load(); n != 3 { // step 0's notify, then step 1's charge and notify
		t.Fatalf("Recover sent %d effects, want 3", n)
	}
}
