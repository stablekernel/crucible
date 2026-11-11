package durable_test

import (
	"context"
	"errors"
	"strings"
	"sync"
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

// TestHandle_CallbackCallsBackWithItsCtx_IsRefused proves a guard, an action and a
// reducer that call back into their Handle with the Ctx their input carries get
// ErrReentrantStep at once instead of deadlocking on the lock their step holds, on
// each step that runs them: Fire, an eventless transition settling after it, a Fire
// through middleware that derives its context, Tick, RunService and DeliverToActor.
func TestHandle_CallbackCallsBackWithItsCtx_IsRefused(t *testing.T) {
	type probe = *state.Builder[string, string, svcCtx]
	type handle = *durable.Handle[string, string, svcCtx]
	derive := func(next state.FireFunc[string, string, svcCtx]) state.FireFunc[string, string, svcCtx] {
		return func(ctx context.Context, event string) state.FireResult[string] {
			return next(context.WithValue(ctx, reentryKey{}, "derived"), event)
		}
	}
	clock := state.NewFakeClock(epoch)
	var runs int64
	cases := []struct {
		name  string
		build func(probe) probe
		opts  []durable.Option[string, string, svcCtx]
		init  string
		drive func(t *testing.T, h handle) error
	}{
		{
			name: "Fire",
			build: func(b probe) probe {
				return b.State("idle").State("done").Final().Initial("idle").
					Transition("idle").On("go").GoTo("done").When("g").Do("a").Assign("r")
			},
			init:  "idle",
			drive: func(_ *testing.T, h handle) error { _, err := h.Fire(context.Background(), "go"); return err },
		},
		{
			name: "eventless after Fire",
			build: func(b probe) probe {
				return b.State("idle").State("mid").State("done").Final().Initial("idle").
					Transition("idle").On("go").GoTo("mid").
					Transition("mid").Always().GoTo("done").When("g").Do("a").Assign("r")
			},
			init:  "idle",
			drive: func(_ *testing.T, h handle) error { _, err := h.Fire(context.Background(), "go"); return err },
		},
		{
			name: "Fire through middleware",
			build: func(b probe) probe {
				return b.Use(derive).State("idle").State("done").Final().Initial("idle").
					Transition("idle").On("go").GoTo("done").When("g").Do("a").Assign("r")
			},
			init:  "idle",
			drive: func(_ *testing.T, h handle) error { _, err := h.Fire(context.Background(), "go"); return err },
		},
		{
			name: "Tick",
			build: func(b probe) probe {
				return b.State("idle").State("arming").State("done").Final().Initial("idle").
					Transition("idle").On("arm").GoTo("arming").
					Transition("arming").After(5 * time.Second).On("elapsed").GoTo("done").When("g").Do("a").Assign("r")
			},
			opts: []durable.Option[string, string, svcCtx]{durable.WithRunnerClock[string, string, svcCtx](clock)},
			init: "idle",
			drive: func(_ *testing.T, h handle) error {
				ctx := context.Background()
				if _, err := h.Fire(ctx, "arm"); err != nil {
					return err
				}
				clock.Advance(6 * time.Second)
				_, err := h.Tick(ctx)
				return err
			},
		},
		{
			name: "RunService",
			build: func(b probe) probe {
				return b.Service("fetch", counterService(&runs)).
					State("idle").
					State("loading").Invoke("fetch", state.WithInvokeOnDone("ok")).
					State("done").Final().Initial("idle").
					Transition("idle").On("start").GoTo("loading").
					Transition("loading").On("ok").GoTo("done").When("g").Do("a").Assign("r")
			},
			opts: []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](
				state.NewRegistry[svcCtx]().Service("fetch", counterService(&runs)))},
			init: "idle",
			drive: func(_ *testing.T, h handle) error {
				ctx := context.Background()
				if _, err := h.Fire(ctx, "start"); err != nil {
					return err
				}
				_, _, err := h.RunService(ctx, state.InvokeID("probe", "loading", 0))
				return err
			},
		},
		{
			name: "DeliverToActor",
			build: func(b probe) probe {
				return b.Actor("spawnChild").
					State("supervising").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone")).
					State("done").Final().Initial("supervising").
					Transition("supervising").On("childDone").GoTo("done").When("g").Do("a").Assign("r")
			},
			opts: []durable.Option[string, string, svcCtx]{durable.WithActorPalette[string, string, svcCtx](counterPalette(&runs))},
			init: "supervising",
			drive: func(t *testing.T, h handle) error {
				ref, ok := h.ActorRef(state.ActorID("probe", "supervising", 0))
				if !ok {
					t.Fatal("no actor ref for the spawned child")
				}
				_, err := h.DeliverToActor(context.Background(), ref, "finish")
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var h handle
			got := map[string]error{}
			view := func(name string, ctx context.Context) {
				got[name] = h.View(ctx, func(*state.Instance[string, string, svcCtx]) {})
			}
			machine := tc.build(state.ForgeFor[svcCtx]("probe").
				Guard("g", func(in state.GuardCtx[svcCtx]) bool { view("guard", in.Ctx); return true }).
				Action("a", func(in state.ActionCtx[svcCtx]) (state.Effect, error) { view("action", in.Ctx); return nil, nil }).
				Reducer("r", func(in state.AssignCtx[svcCtx]) svcCtx { view("reducer", in.Ctx); return in.Entity })).
				Quench()
			var err error
			h, err = durable.NewRunner(machine, durable.NewMemStore(), tc.opts...).
				Start(context.Background(), "probe-1", svcCtx{}, state.WithInitialState(tc.init))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if err := tc.drive(t, h); err != nil {
					t.Errorf("drive: %v", err)
				}
			}()
			waitFor(t, done, tc.name)
			for _, name := range []string{"guard", "action", "reducer"} {
				if !errors.Is(got[name], durable.ErrReentrantStep) {
					t.Errorf("the %s's View = %v, want ErrReentrantStep", name, got[name])
				}
			}
			if cur := h.Instance().Current(); cur != "done" {
				t.Fatalf("state %q, want done", cur)
			}
		})
	}
}

// reentryKey is the key the middleware case derives its context with.
type reentryKey struct{}

// TestHandle_Drain_WaitsForAStepStillRecording proves Drain doesn't return while a
// step that held the lock when the Handle failed is still in its Append: Recover then
// loads that step, and its caller learns the step was recorded.
func TestHandle_Drain_WaitsForAStepStillRecording(t *testing.T) {
	ctx := context.Background()
	m := effectMachine()
	store := &blockingMemStore{MemStore: durable.NewMemStore(), entered: make(chan struct{}), release: make(chan struct{})}
	handlerIn, failNow := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	handler := func(context.Context, string, state.Effect) error {
		if first.CompareAndSwap(false, true) { // step 0's notify waits, then fails
			close(handlerIn)
			<-failNow
			return errors.New("mail server down")
		}
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
	drained := make(chan error, 1)
	go func() { drained <- h.Drain(ctx) }()
	select {
	case drainErr := <-drained:
		t.Fatalf("Drain returned %v while step 1 was still in its Append", drainErr)
	case <-time.After(100 * time.Millisecond):
	}
	close(store.release)
	if drainErr := <-drained; drainErr != nil {
		t.Fatalf("Drain: %v", drainErr)
	}
	got := <-secondDone
	if !errors.Is(got, durable.ErrHandleFailed) || !strings.Contains(got.Error(), "step 1 was recorded") {
		t.Fatalf("Fire(bill) = %v, want ErrHandleFailed saying step 1 was recorded", got)
	}
	r, err := durable.Recover(ctx, m, store.MemStore, "e-1", opt)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if cur := r.Instance().Current(); cur != "billed" {
		t.Fatalf("recovered state %q, want billed: Recover missed step 1", cur)
	}
	if _, err := r.Fire(ctx, "finish"); err != nil {
		t.Fatalf("Fire(finish) on the recovered Handle: %v", err)
	}
}

// checkpointSpy is a MemStore that records the steps it checkpointed through after
// Start's initial checkpoint and refuses a checkpoint whose context has ended. Once
// armed, it fails the next checkpoint (diskFull first, ahead of the context check), or
// writes it and then reports it canceled (landThenFail, also ahead of the check), or
// reports an ended context as canceled whatever ended it (canceledAs).
type checkpointSpy struct {
	*durable.MemStore
	failNext     atomic.Bool
	diskFull     atomic.Bool
	canceledAs   atomic.Bool
	landThenFail atomic.Bool
	mu           sync.Mutex
	through      []int
}

func (s *checkpointSpy) Checkpoint(ctx context.Context, id durable.InstanceID, snapshot []byte, throughStep int, opts ...durable.CheckpointOption) error {
	if s.diskFull.CompareAndSwap(true, false) {
		return errors.New("disk full")
	}
	if throughStep >= 0 && s.landThenFail.CompareAndSwap(true, false) {
		if err := s.MemStore.Checkpoint(context.Background(), id, snapshot, throughStep, opts...); err != nil {
			return err
		}
		s.record(throughStep)
		return context.Canceled
	}
	if err := ctx.Err(); err != nil {
		if s.canceledAs.CompareAndSwap(true, false) {
			return context.Canceled
		}
		return err
	}
	if s.failNext.CompareAndSwap(true, false) {
		return errors.New("disk full")
	}
	if err := s.MemStore.Checkpoint(ctx, id, snapshot, throughStep, opts...); err != nil {
		return err
	}
	if throughStep >= 0 { // not Start's initial checkpoint
		s.record(throughStep)
	}
	return nil
}

func (s *checkpointSpy) record(step int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.through = append(s.through, step)
}

func (s *checkpointSpy) steps() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.through...)
}

// TestHandle_DeferredCheckpoint_SurvivesTheFinishingCallersCancel proves a checkpoint
// deferred to the step that finishes dispatching last isn't dropped when that step's
// caller cancels, and that caller isn't told: it stays pending, and the next step
// writes it.
func TestHandle_DeferredCheckpoint_SurvivesTheFinishingCallersCancel(t *testing.T) {
	ctx := context.Background()
	store := &checkpointSpy{MemStore: durable.NewMemStore()}
	handlerIn, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	handler := func(context.Context, string, state.Effect) error {
		if first.CompareAndSwap(false, true) { // step 0's notify waits
			close(handlerIn)
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
	firstCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { _, fireErr := h.Fire(firstCtx, "go"); firstDone <- fireErr }()
	<-handlerIn
	if _, err := h.Fire(ctx, "bill"); err != nil { // step 1's checkpoint is due, and waits on step 0
		t.Fatalf("Fire(bill): %v", err)
	}
	if got := store.steps(); len(got) != 0 {
		t.Fatalf("checkpointed through %v before step 0 dispatched, want none", got)
	}
	cancel()
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("Fire(go) = %v, want nil: step 1's checkpoint isn't its caller's", err)
	}
	if got := store.steps(); len(got) != 0 {
		t.Fatalf("checkpointed through %v on a canceled context, want none yet", got)
	}
	if _, err := h.Fire(ctx, "finish"); err != nil {
		t.Fatalf("Fire(finish): %v", err)
	}
	if got := store.steps(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("checkpointed through %v, want [1]: the deferred checkpoint was dropped", got)
	}
}

// TestHandle_FailedCheckpoint_WaitsForTheNextDueStep proves a checkpoint the Store
// refuses isn't retried, or reported, on a step the policy didn't make due: the next
// due step checkpoints instead.
func TestHandle_FailedCheckpoint_WaitsForTheNextDueStep(t *testing.T) {
	ctx := context.Background()
	store := &checkpointSpy{MemStore: durable.NewMemStore()}
	loop := state.ForgeFor[*runCtx]("loop").
		State("a").State("b").Initial("a").
		Transition("a").On("t").GoTo("b").
		Transition("b").On("t").GoTo("a").
		Quench()
	h, err := durable.NewRunner(loop, store, durable.WithCheckpointEvery[string, string, *runCtx](2)).
		Start(ctx, "l-1", &runCtx{}, state.WithInitialState("a"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	fire := func(step int) error { _, err := h.Fire(ctx, "t"); return err }
	if err := fire(0); err != nil {
		t.Fatalf("step 0: %v", err)
	}
	store.failNext.Store(true)
	if err := fire(1); err == nil { // due
		t.Fatal("step 1 = nil, want its checkpoint's error")
	}
	if err := fire(2); err != nil { // not due
		t.Fatalf("step 2 = %v, want nil: it didn't make step 1's checkpoint due", err)
	}
	if err := fire(3); err != nil { // due
		t.Fatalf("step 3: %v", err)
	}
	if got := store.steps(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("checkpointed through %v, want [3]: only the next due step checkpoints", got)
	}
}

// TestHandle_StepRecordedAsTheHandleFails_SaysSo proves each step method that was in
// its Append when another goroutine failed the Handle returns ErrHandleFailed saying
// the step was recorded, a step with no domain effects included, and that Drain then
// Recover load it.
func TestHandle_StepRecordedAsTheHandleFails_SaysSo(t *testing.T) {
	type handle = *durable.Handle[string, string, svcCtx]
	type probe = *state.Builder[string, string, svcCtx]
	clock := state.NewFakeClock(epoch)
	var runs int64
	cases := []struct {
		name  string
		build func(probe) probe
		opts  []durable.Option[string, string, svcCtx]
		step  func(ctx context.Context, h handle) error
		want  string
	}{
		{
			name: "Fire with no effects",
			build: func(b probe) probe {
				return b.State("hub").State("end").Final().Transition("hub").On("poke").GoTo("end")
			},
			step: func(ctx context.Context, h handle) error { _, err := h.Fire(ctx, "poke"); return err },
		},
		{
			name: "Tick",
			build: func(b probe) probe {
				return b.State("hub").State("end").Final().Transition("hub").After(5 * time.Second).On("elapsed").GoTo("end")
			},
			opts: []durable.Option[string, string, svcCtx]{durable.WithRunnerClock[string, string, svcCtx](clock)},
			step: func(ctx context.Context, h handle) error {
				clock.Advance(6 * time.Second)
				_, err := h.Tick(ctx)
				return err
			},
		},
		{
			name: "RunService",
			build: func(b probe) probe {
				return b.Service("fetch", counterService(&runs)).
					State("hub").Invoke("fetch", state.WithInvokeOnDone("ok")).State("end").Final().
					Transition("hub").On("ok").GoTo("end")
			},
			opts: []durable.Option[string, string, svcCtx]{durable.WithServiceRegistry[string, string, svcCtx](
				state.NewRegistry[svcCtx]().Service("fetch", counterService(&runs)))},
			step: func(ctx context.Context, h handle) error {
				_, _, err := h.RunService(ctx, state.InvokeID("recorded", "hub", 0))
				return err
			},
		},
		{
			name: "DeliverToActor",
			build: func(b probe) probe {
				return b.Actor("spawnChild").
					State("hub").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone")).State("end").Final().
					Transition("hub").On("childDone").GoTo("end")
			},
			opts: []durable.Option[string, string, svcCtx]{durable.WithActorPalette[string, string, svcCtx](counterPalette(&runs))},
			step: func(ctx context.Context, h handle) error {
				ref, ok := h.ActorRef(state.ActorID("recorded", "hub", 0))
				if !ok {
					return errors.New("no actor ref for the spawned child")
				}
				_, err := h.DeliverToActor(ctx, ref, "finish")
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := &blockingMemStore{MemStore: durable.NewMemStore(), entered: make(chan struct{}), release: make(chan struct{})}
			handlerIn, failNow := make(chan struct{}), make(chan struct{})
			var first atomic.Bool
			handler := func(context.Context, string, state.Effect) error {
				if first.CompareAndSwap(false, true) { // step 0's notify waits, then fails
					close(handlerIn)
					<-failNow
					return errors.New("mail server down")
				}
				return nil
			}
			machine := tc.build(state.ForgeFor[svcCtx]("recorded").
				Action("notify", func(state.ActionCtx[svcCtx]) (state.Effect, error) {
					return notifyEffect{To: "ops@example.com"}, nil
				}).
				State("idle").Initial("idle").
				Transition("idle").On("go").GoTo("hub").Do("notify")).
				Quench()
			opts := append([]durable.Option[string, string, svcCtx]{durable.WithEffectHandler[string, string, svcCtx](handler)}, tc.opts...)
			h, err := durable.NewRunner(machine, store, opts...).Start(ctx, "r-1", svcCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			firstDone := make(chan error, 1)
			go func() { _, fireErr := h.Fire(ctx, "go"); firstDone <- fireErr }()
			<-handlerIn
			store.armed.Store(true)
			stepDone := make(chan error, 1)
			go func() { stepDone <- tc.step(ctx, h) }()
			<-store.entered // step 1 holds the lock, in its Append
			close(failNow)
			if got := <-firstDone; !errors.Is(got, durable.ErrHandleFailed) {
				t.Fatalf("Fire(go) = %v, want ErrHandleFailed", got)
			}
			close(store.release)
			got := <-stepDone
			if !errors.Is(got, durable.ErrHandleFailed) || !strings.Contains(got.Error(), "step 1 was recorded") {
				t.Fatalf("%s = %v, want ErrHandleFailed saying step 1 was recorded", tc.name, got)
			}
			if drainErr := h.Drain(ctx); drainErr != nil {
				t.Fatalf("Drain: %v", drainErr)
			}
			r, err := durable.Recover(ctx, machine, store.MemStore, "r-1", opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if cur := r.Instance().Current(); cur != "end" {
				t.Fatalf("recovered state %q, want end: Recover missed step 1", cur)
			}
		})
	}
}

// TestHandle_CheckpointThatLandedDespiteAnError_IsNotRetriedForever proves a deferred
// checkpoint the Store wrote but reported canceled doesn't fail the next step: the
// retry the Store refuses as not advancing counts as written.
func TestHandle_CheckpointThatLandedDespiteAnError_IsNotRetriedForever(t *testing.T) {
	ctx := context.Background()
	store := &checkpointSpy{MemStore: durable.NewMemStore()}
	handlerIn, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	handler := func(context.Context, string, state.Effect) error {
		if first.CompareAndSwap(false, true) { // step 0's notify waits
			close(handlerIn)
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
	firstCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { _, fireErr := h.Fire(firstCtx, "go"); firstDone <- fireErr }()
	<-handlerIn
	if _, err := h.Fire(ctx, "bill"); err != nil { // step 1's checkpoint is due, and waits on step 0
		t.Fatalf("Fire(bill): %v", err)
	}
	cancel()
	store.landThenFail.Store(true)
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("Fire(go) = %v, want nil", err)
	}
	if _, err := h.Fire(ctx, "finish"); err != nil {
		t.Fatalf("Fire(finish) = %v, want nil: the checkpoint at step 1 had landed", err)
	}
	if got := store.steps(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("checkpointed through %v, want [1]", got)
	}
}

// TestHandle_DeferredCheckpointStoreError_OnAnEndedContext_IsReported proves the
// finishing caller still hears a deferred checkpoint's Store error when its context
// has ended: only the cancellation itself is kept from it.
func TestHandle_DeferredCheckpointStoreError_OnAnEndedContext_IsReported(t *testing.T) {
	ctx := context.Background()
	store := &checkpointSpy{MemStore: durable.NewMemStore()}
	handlerIn, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	handler := func(context.Context, string, state.Effect) error {
		if first.CompareAndSwap(false, true) { // step 0's notify waits
			close(handlerIn)
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
	firstCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { _, fireErr := h.Fire(firstCtx, "go"); firstDone <- fireErr }()
	<-handlerIn
	if _, err := h.Fire(ctx, "bill"); err != nil {
		t.Fatalf("Fire(bill): %v", err)
	}
	cancel()
	store.diskFull.Store(true)
	close(release)
	if err := <-firstDone; err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("Fire(go) = %v, want the Store's disk-full error", err)
	}
}

// TestHandle_DeferredCheckpoint_StoreReportsAnotherContextError_IsNotReported proves a
// deferred checkpoint the Store refuses with a different context error than the one
// that ended the finishing caller's context still isn't reported to that caller.
func TestHandle_DeferredCheckpoint_StoreReportsAnotherContextError_IsNotReported(t *testing.T) {
	ctx := context.Background()
	store := &checkpointSpy{MemStore: durable.NewMemStore()}
	handlerIn, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	handler := func(context.Context, string, state.Effect) error {
		if first.CompareAndSwap(false, true) { // step 0's notify waits
			close(handlerIn)
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
	firstCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { _, fireErr := h.Fire(firstCtx, "go"); firstDone <- fireErr }()
	<-handlerIn
	if _, err := h.Fire(ctx, "bill"); err != nil { // step 1's checkpoint is due, and waits on step 0
		t.Fatalf("Fire(bill): %v", err)
	}
	<-firstCtx.Done() // ended by its deadline
	store.canceledAs.Store(true)
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("Fire(go) = %v, want nil: the Store's context.Canceled for a deadline is still a context error", err)
	}
	if _, err := h.Fire(ctx, "finish"); err != nil {
		t.Fatalf("Fire(finish): %v", err)
	}
	if got := store.steps(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("checkpointed through %v, want [1]", got)
	}
}
