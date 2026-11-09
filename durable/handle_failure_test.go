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

// fetchID is the invocation id of the fetch service the single-service machine runs.
var fetchID = state.InvokeID("fetch", "loading", 0)

// armedAppendStore counts Appends and, once armed, fails the next one.
type armedAppendStore struct {
	durable.Store
	armed   bool
	appends int
}

func (s *armedAppendStore) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	if s.armed {
		s.armed = false
		return 0, errors.New("store unavailable")
	}
	s.appends++
	return s.Store.Append(ctx, id, rec, opts...)
}

// startFetch starts the single-service machine over store and moves it into the
// state that invokes the service.
func startFetch(t *testing.T, store durable.Store, calls *int64) *durable.Handle[string, string, svcCtx] {
	t.Helper()
	ctx := context.Background()
	fn := counterService(calls)
	h, err := durable.NewRunner(fetchMachine(fn), store,
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

// startTwin starts the two-actor machine over store, with childA running.
func startTwin(t *testing.T, store durable.Store, runs *int64) (*durable.Handle[string, string, actorCtx], state.ActorRef) {
	t.Helper()
	palette := map[string]state.ActorBehavior{"childA": counterChild(runs), "childB": counterChild(runs)}
	h, err := durable.NewRunner(twoActorMachine(), store,
		durable.WithActorPalette[string, string, actorCtx](palette)).
		Start(context.Background(), "twin-1", actorCtx{}, state.WithInitialState("first"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("twin", "first", 0))
	if !ok {
		t.Fatal("no ref for childA")
	}
	return h, ref
}

// TestHandle_RunService_FailedAppend_FailsHandle proves a failed Append of a service
// outcome fails the Handle: RunService reports the settle with ErrHandleFailed, every
// later step is refused, and a recovered Handle runs the service again.
func TestHandle_RunService_FailedAppend_FailsHandle(t *testing.T) {
	ctx := context.Background()
	var calls int64
	store := &armedAppendStore{Store: durable.NewMemStore()}
	h := startFetch(t, store, &calls)

	store.armed = true
	_, ok, err := h.RunService(ctx, fetchID)
	if !ok || !errors.Is(err, durable.ErrHandleFailed) || !strings.Contains(err.Error(), "store unavailable") {
		t.Fatalf("RunService = (%v, %v), want true and ErrHandleFailed naming the cause", ok, err)
	}
	if _, ok, err = h.RunService(ctx, fetchID); ok || !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("RunService after the failure = (%v, %v), want false and ErrHandleFailed", ok, err)
	}
	if _, err = h.Fire(ctx, "start"); !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("Fire after the failure = %v, want ErrHandleFailed", err)
	}

	fn := counterService(&calls)
	rh, err := durable.Recover(ctx, fetchMachine(fn), store, "fetch-1",
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := rh.Instance().Current(); got != "loading" {
		t.Fatalf("recovered state = %q, want loading (the outcome wasn't recorded)", got)
	}
	mustRunService(t, rh, fetchID)
	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Fatalf("service ran %d times, want 2 (once live, once after recovery)", got)
	}
}

// TestHandle_DeliverToActor_FailedAppend_FailsHandle proves a failed Append of an
// actor delivery fails the Handle: DeliverToActor reports the delivery with
// ErrHandleFailed, and every later step is refused.
func TestHandle_DeliverToActor_FailedAppend_FailsHandle(t *testing.T) {
	ctx := context.Background()
	var runs int64
	store := &armedAppendStore{Store: durable.NewMemStore()}
	h, ref := startTwin(t, store, &runs)

	store.armed = true
	delivered, err := h.DeliverToActor(ctx, ref, "finish")
	if !delivered || !errors.Is(err, durable.ErrHandleFailed) || !strings.Contains(err.Error(), "store unavailable") {
		t.Fatalf("DeliverToActor = (%v, %v), want true and ErrHandleFailed naming the cause", delivered, err)
	}
	if delivered, err = h.DeliverToActor(ctx, ref, "finish"); delivered || !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("DeliverToActor after the failure = (%v, %v), want false and ErrHandleFailed", delivered, err)
	}
	if _, err = h.Tick(ctx); !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("Tick after the failure = %v, want ErrHandleFailed", err)
	}
}

// TestHandle_RunService_ContextEnded_ChangesNothing proves an ended context stops
// RunService before the service runs or anything is journaled.
func TestHandle_RunService_ContextEnded_ChangesNothing(t *testing.T) {
	var calls int64
	store := &armedAppendStore{Store: durable.NewMemStore()}
	h := startFetch(t, store, &calls)
	before := store.appends

	ended, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok, err := h.RunService(ended, fetchID); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("RunService = (%v, %v), want false and context.Canceled", ok, err)
	}
	if got := atomic.LoadInt64(&calls); got != 0 || store.appends != before {
		t.Fatalf("after an ended context: %d service runs and %d new appends, want 0 and 0", got, store.appends-before)
	}
	mustRunService(t, h, fetchID)
}

// TestHandle_DeliverToActor_ContextEnded_ChangesNothing proves an ended context stops
// DeliverToActor before the actor sees the event or anything is journaled.
func TestHandle_DeliverToActor_ContextEnded_ChangesNothing(t *testing.T) {
	var runs int64
	store := &armedAppendStore{Store: durable.NewMemStore()}
	h, ref := startTwin(t, store, &runs)
	before, state0 := store.appends, h.Instance().Current()

	ended, cancel := context.WithCancel(context.Background())
	cancel()
	if delivered, err := h.DeliverToActor(ended, ref, "finish"); delivered || !errors.Is(err, context.Canceled) {
		t.Fatalf("DeliverToActor = (%v, %v), want false and context.Canceled", delivered, err)
	}
	if store.appends != before || h.Instance().Current() != state0 {
		t.Fatalf("after an ended context: %d new appends, state %q, want 0 and %q", store.appends-before, h.Instance().Current(), state0)
	}
	if delivered, err := h.DeliverToActor(context.Background(), ref, "finish"); !delivered || err != nil {
		t.Fatalf("DeliverToActor after = (%v, %v), want the actor still running", delivered, err)
	}
}
