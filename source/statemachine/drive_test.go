// SPDX-License-Identifier: Apache-2.0

package statemachine_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/statemachine"
	"github.com/stablekernel/crucible/state"
)

// keyOf is the instance key the turnstile is driven under; one turnstile in
// these tests, so a constant key.
const keyOf turnstileState = locked

// routeFunded routes every message to the single turnstile instance and fires
// coin (the funded-guarded transition).
func routeFunded(m source.Message) (turnstileState, turnstileEvent, error) {
	return keyOf, coin, nil
}

// routePush routes to push (the unguarded unlock→lock transition).
func routePush(m source.Message) (turnstileState, turnstileEvent, error) {
	return keyOf, push, nil
}

func TestDrive_AckAfterDurableCommit(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	// Seed a funded instance so the coin guard passes.
	seedFunded(t, m, store)

	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](m, store, routeFunded)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionAck {
		t.Fatalf("action = %v, want ack (err=%v)", res.Action, res.Err)
	}

	rec, ok, err := store.Load(context.Background(), keyOf)
	if err != nil || !ok {
		t.Fatalf("load after ack: ok=%v err=%v", ok, err)
	}
	if rec.Version != 2 {
		t.Fatalf("version = %d, want 2 (seed 1 + 1 transition)", rec.Version)
	}
	if rec.Snapshot.Current != unlocked {
		t.Fatalf("state = %v, want unlocked", rec.Snapshot.Current)
	}
	if rec.LastEventID != "evt-1" {
		t.Fatalf("lastEventID = %q, want evt-1", rec.LastEventID)
	}
}

func TestDrive_VersionIdempotency_RedeliveryIsNoOpAck(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store)
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](m, store, routeFunded)

	first := h(context.Background(), msg("evt-1", "c1"))
	if first.Action != source.ActionAck || first.Class == source.Drop {
		t.Fatalf("first delivery = %v/%v, want plain ack", first.Action, first.Class)
	}
	before, _, _ := store.Load(context.Background(), keyOf)

	// Redeliver the SAME event id: must be skipped (acked, not re-applied).
	redo := h(context.Background(), msg("evt-1", "c1"))
	if redo.Action != source.ActionAck {
		t.Fatalf("redelivery action = %v, want ack", redo.Action)
	}
	if redo.Class != source.Drop {
		t.Fatalf("redelivery class = %v, want drop (skip)", redo.Class)
	}

	after, _, _ := store.Load(context.Background(), keyOf)
	if after.Version != before.Version {
		t.Fatalf("version advanced on redelivery: %d -> %d", before.Version, after.Version)
	}
}

func TestDrive_StateAwareReject_GuardRejection(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	// Seed an UNFUNDED instance: the coin guard fails.
	seed(t, m, store, &turnstile{Funded: false})
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](m, store, routeFunded)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionTerm {
		t.Fatalf("action = %v, want term", res.Action)
	}
	if res.Class != source.InvalidForState {
		t.Fatalf("class = %v, want invalid_for_state", res.Class)
	}
	if !errors.Is(res.Err, source.ErrInvalidForState) {
		t.Fatalf("err %v does not match ErrInvalidForState", res.Err)
	}
	var gr *source.GuardRejection
	if !errors.As(res.Err, &gr) {
		t.Fatalf("err %v is not a *source.GuardRejection", res.Err)
	}
	if gr.Event != "coin" {
		t.Fatalf("rejection event = %q, want coin", gr.Event)
	}
}

func TestDrive_IllegalEvent_NoTransition_IsReject(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store) // instance is in locked; push has no transition from locked
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](m, store, routePush)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionTerm || res.Class != source.InvalidForState {
		t.Fatalf("got %v/%v, want term/invalid_for_state", res.Action, res.Class)
	}
	if !errors.Is(res.Err, source.ErrInvalidForState) {
		t.Fatalf("err %v does not match ErrInvalidForState", res.Err)
	}
}

func TestDrive_TransientErrors_AreNak(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	boom := errors.New("backend down")

	tests := []struct {
		name  string
		store statemachine.Store[turnstileState, turnstileState, turnstileEvent, *turnstile]
	}{
		{
			name:  "load failure",
			store: &flakyStore{loadErr: boom},
		},
		{
			name:  "save failure",
			store: &flakyStore{saveErr: boom},
		},
		{
			name:  "save conflict",
			store: &flakyStore{saveErr: statemachine.ErrConflict},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](m, tc.store, routeFunded)
			res := h(context.Background(), msg("evt-1", "c1"))
			if res.Action != source.ActionNak {
				t.Fatalf("action = %v, want nak", res.Action)
			}
			if res.Class != source.Retryable {
				t.Fatalf("class = %v, want retryable", res.Class)
			}
		})
	}
}

func TestDrive_RouteFailure_IsTerm(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	bad := errors.New("undecodable")
	router := func(source.Message) (turnstileState, turnstileEvent, error) {
		return 0, 0, bad
	}
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](m, store, router)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionTerm || res.Class != source.Poison {
		t.Fatalf("got %v/%v, want term/poison", res.Action, res.Class)
	}
	if !errors.Is(res.Err, bad) {
		t.Fatalf("err %v does not wrap route error", res.Err)
	}
}

func TestDrive_EmitHandoff(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store)

	var emitted []any
	sink := statemachine.SinkFunc(func(_ context.Context, eff any) error {
		emitted = append(emitted, eff)
		return nil
	})
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, statemachine.WithSink(sink),
	)

	if res := h(context.Background(), msg("evt-1", "c1")); res.Action != source.ActionAck {
		t.Fatalf("action = %v, want ack", res.Action)
	}
	if len(emitted) != 1 {
		t.Fatalf("emitted %d effects, want 1", len(emitted))
	}
	if _, ok := emitted[0].(openedEffect); !ok {
		t.Fatalf("emitted effect = %T, want openedEffect", emitted[0])
	}
}

func TestDrive_EmitFailure_IsNak_NotPersisted(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store)
	before, _, _ := store.Load(context.Background(), keyOf)

	sink := statemachine.SinkFunc(func(context.Context, any) error {
		return errors.New("publish failed")
	})
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, statemachine.WithSink(sink),
	)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionNak {
		t.Fatalf("action = %v, want nak on emit failure", res.Action)
	}
	after, _, _ := store.Load(context.Background(), keyOf)
	if after.Version != before.Version {
		t.Fatalf("transition persisted despite emit failure: %d -> %d", before.Version, after.Version)
	}
}

func TestDrive_FreshInstance_NoRecord(t *testing.T) {
	t.Parallel()
	// A machine whose initial state's coin guard passes on a zero entity needs
	// funding; use push from a fresh unlocked-by-initial machine instead. Here the
	// initial state is locked and a fresh (unfunded) coin is a guard rejection,
	// which proves the fresh-instance path casts from initial without a record.
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](m, store, routeFunded)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionTerm || res.Class != source.InvalidForState {
		t.Fatalf("fresh unfunded coin = %v/%v, want term/invalid_for_state", res.Action, res.Class)
	}
}

func TestDrive_ConcurrentDistinctKeys_RaceClean(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()

	// Seed N independent keys, each funded, and drive each from its own goroutine.
	const n = 16
	router := func(k turnstileState) statemachine.Router[turnstileState, turnstileEvent] {
		return func(source.Message) (turnstileState, turnstileEvent, error) { return k, coin, nil }
	}
	for k := turnstileState(0); k < n; k++ {
		seedKey(t, m, store, k, &turnstile{Funded: true})
	}

	var wg sync.WaitGroup
	for k := turnstileState(0); k < n; k++ {
		wg.Add(1)
		go func(k turnstileState) {
			defer wg.Done()
			h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](m, store, router(k))
			if res := h(context.Background(), msg("evt", "c")); res.Action != source.ActionAck {
				t.Errorf("key %v: action = %v, want ack", k, res.Action)
			}
		}(k)
	}
	wg.Wait()
}

// --- helpers ---

func seedFunded(t *testing.T, m *state.Machine[turnstileState, turnstileEvent, *turnstile], store *statemachine.MemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]) {
	t.Helper()
	seed(t, m, store, &turnstile{Funded: true})
}

func seed(t *testing.T, m *state.Machine[turnstileState, turnstileEvent, *turnstile], store *statemachine.MemStore[turnstileState, turnstileState, turnstileEvent, *turnstile], ent *turnstile) {
	t.Helper()
	seedKey(t, m, store, keyOf, ent)
}

// seedKey persists a version-1 record for key in the locked state with entity ent.
func seedKey(t *testing.T, m *state.Machine[turnstileState, turnstileEvent, *turnstile], store *statemachine.MemStore[turnstileState, turnstileState, turnstileEvent, *turnstile], key turnstileState, ent *turnstile) {
	t.Helper()
	inst := m.Cast(ent, state.WithInitialState[turnstileState](locked))
	rec := statemachine.Record[turnstileState, turnstileEvent, *turnstile]{
		Snapshot: inst.Snapshot(),
		Version:  1,
	}
	if err := store.Save(context.Background(), key, rec, 0); err != nil {
		t.Fatalf("seed key %v: %v", key, err)
	}
}

// flakyStore is a Store that returns configured errors, for the transient-error
// table. It loads a fresh funded instance so the fire path is reached before the
// save fails.
type flakyStore struct {
	loadErr error
	saveErr error
}

func (s *flakyStore) Load(_ context.Context, key turnstileState) (statemachine.Record[turnstileState, turnstileEvent, *turnstile], bool, error) {
	if s.loadErr != nil {
		return statemachine.Record[turnstileState, turnstileEvent, *turnstile]{}, false, s.loadErr
	}
	m := buildTurnstile()
	inst := m.Cast(&turnstile{Funded: true}, state.WithInitialState[turnstileState](locked))
	return statemachine.Record[turnstileState, turnstileEvent, *turnstile]{Snapshot: inst.Snapshot(), Version: 1}, true, nil
}

func (s *flakyStore) Save(context.Context, turnstileState, statemachine.Record[turnstileState, turnstileEvent, *turnstile], int64) error {
	return s.saveErr
}
