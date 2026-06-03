// SPDX-License-Identifier: Apache-2.0

package statemachine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/statemachine"
)

// fakeTx is an in-memory [source.Tx] that records every produced record so a test
// can assert what the transactional emit produced. It also fails on demand to
// exercise the abort path.
type fakeTx struct {
	produced []source.ProducedRecord
	failOn   string // topic to fail Produce on; "" never fails
}

func (t *fakeTx) Produce(_ context.Context, records ...source.ProducedRecord) error {
	for _, r := range records {
		if t.failOn != "" && r.Topic == t.failOn {
			return errors.New("produce rejected")
		}
		t.produced = append(t.produced, r)
	}
	return nil
}

// fakeTransactional is an in-memory [source.Transactional] that drives the
// begin → fn → commit/abort choreography against a fakeTx, recording the call
// sequence and whether the work function committed. committed=false simulates a
// broker abort (a rebalance fence) despite a successful work function.
type fakeTransactional struct {
	tx        *fakeTx
	committed bool // whether End reports a commit on a successful fn
	calls     []string
}

func (s *fakeTransactional) Begin(ctx context.Context, _ source.Message, fn func(context.Context, source.Tx) error) error {
	s.calls = append(s.calls, "begin")
	err := fn(ctx, s.tx)
	if err != nil {
		s.calls = append(s.calls, "abort")
		return err
	}
	s.calls = append(s.calls, "commit")
	if !s.committed {
		return errors.New("source: transaction aborted by broker")
	}
	return nil
}

// emitOpenedToTopic is a TxSink that produces the openedEffect onto "turnstile.out".
func emitOpenedToTopic(_ context.Context, tx source.Tx, effect any) error {
	if oe, ok := effect.(openedEffect); ok {
		return tx.Produce(context.Background(), source.ProducedRecord{
			Topic:   "turnstile.out",
			Key:     []byte("turnstile"),
			Value:   []byte("opened by " + oe.By),
			Headers: source.Headers{{Key: "kind", Value: "opened"}},
		})
	}
	return errors.New("unknown effect")
}

func TestDriveTx_CommitsEmitAndOffsetAtomically(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store)

	tx := &fakeTx{}
	sub := &fakeTransactional{tx: tx, committed: true}
	h := statemachine.DriveTx[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, sub, statemachine.TxSinkFunc(emitOpenedToTopic),
	)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionManual {
		t.Fatalf("action = %v, want manual (the transaction committed the offset; err=%v)", res.Action, res.Err)
	}
	// begin → (emit produced + persist) → commit, in order.
	if got, want := sub.calls, []string{"begin", "commit"}; !equalCalls(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
	if len(tx.produced) != 1 {
		t.Fatalf("produced = %#v, want one emitted record", tx.produced)
	}
	if tx.produced[0].Topic != "turnstile.out" || string(tx.produced[0].Value) != "opened by coin" {
		t.Errorf("produced = %+v, want turnstile.out/'opened by coin'", tx.produced[0])
	}
	// The transition was persisted inside the transaction.
	rec, ok, _ := store.Load(context.Background(), keyOf)
	if !ok || rec.Version != 2 || rec.Snapshot.Current != unlocked || rec.LastEventID != "evt-1" {
		t.Fatalf("persisted record = %+v, want version 2 / unlocked / evt-1", rec)
	}
}

func TestDriveTx_BrokerAbortNaksAndDoesNotAdvanceOffset(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store)

	tx := &fakeTx{}
	// committed=false: the work function succeeds but End reports no commit (a
	// rebalance fenced the producer). DriveTx must nak so the input is redelivered.
	sub := &fakeTransactional{tx: tx, committed: false}
	h := statemachine.DriveTx[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, sub, statemachine.TxSinkFunc(emitOpenedToTopic),
	)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionNak {
		t.Fatalf("action = %v, want nak on broker abort", res.Action)
	}
	if got, want := sub.calls, []string{"begin", "commit"}; !equalCalls(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
}

func TestDriveTx_EmitFailureAbortsTransaction(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store)

	// The Tx fails to produce to turnstile.out, so EmitTx errors and the
	// transaction aborts before the offset is committed.
	tx := &fakeTx{failOn: "turnstile.out"}
	sub := &fakeTransactional{tx: tx, committed: true}
	h := statemachine.DriveTx[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, sub, statemachine.TxSinkFunc(emitOpenedToTopic),
	)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionNak {
		t.Fatalf("action = %v, want nak on emit failure", res.Action)
	}
	if got, want := sub.calls, []string{"begin", "abort"}; !equalCalls(got, want) {
		t.Fatalf("call order = %v, want %v (abort on emit failure)", got, want)
	}
	// The transaction aborted, so nothing was committed; the seed version stands.
	rec, _, _ := store.Load(context.Background(), keyOf)
	if rec.Version != 1 {
		t.Fatalf("version = %d, want 1 (abort persisted nothing observable; the store save was rolled back logically)", rec.Version)
	}
}

func TestDriveTx_RedeliveryIsSkip_NotTransactional(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store)

	tx := &fakeTx{}
	sub := &fakeTransactional{tx: tx, committed: true}
	h := statemachine.DriveTx[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, sub, statemachine.TxSinkFunc(emitOpenedToTopic),
	)

	if first := h(context.Background(), msg("evt-1", "c1")); first.Action != source.ActionManual {
		t.Fatalf("first delivery action = %v, want manual", first.Action)
	}
	// Redeliver the same id: deduped by version, acked as a Skip, NOT a new
	// transaction.
	callsBefore := len(sub.calls)
	redo := h(context.Background(), msg("evt-1", "c1"))
	if redo.Action != source.ActionAck || redo.Class != source.Drop {
		t.Fatalf("redelivery = %v/%v, want skip (ack/drop)", redo.Action, redo.Class)
	}
	if len(sub.calls) != callsBefore {
		t.Fatalf("redelivery opened a transaction (%v), want none", sub.calls[callsBefore:])
	}
}

func TestDriveTx_RouteFailureIsTerm_NoTransaction(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	tx := &fakeTx{}
	sub := &fakeTransactional{tx: tx, committed: true}

	boom := errors.New("cannot route")
	router := func(source.Message) (turnstileState, turnstileEvent, error) { return locked, coin, boom }
	h := statemachine.DriveTx[turnstileState, turnstileEvent, *turnstile](
		m, store, router, sub, statemachine.TxSinkFunc(emitOpenedToTopic),
	)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionTerm {
		t.Fatalf("action = %v, want term on route failure", res.Action)
	}
	if len(sub.calls) != 0 {
		t.Fatalf("a transaction was opened on a route failure: %v", sub.calls)
	}
}

func TestDriveTx_GuardRejectionIsReject_NoTransaction(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seed(t, m, store, &turnstile{Funded: false}) // unfunded: coin guard fails
	tx := &fakeTx{}
	sub := &fakeTransactional{tx: tx, committed: true}
	h := statemachine.DriveTx[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, sub, statemachine.TxSinkFunc(emitOpenedToTopic),
	)

	res := h(context.Background(), msg("evt-1", "c1"))
	if res.Action != source.ActionTerm || res.Class != source.InvalidForState {
		t.Fatalf("result = %v/%v, want reject (term/invalid_for_state)", res.Action, res.Class)
	}
	if len(sub.calls) != 0 {
		t.Fatalf("a transaction was opened on a guard rejection: %v", sub.calls)
	}
}

func TestDriveTx_NilWiringNaks(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()

	h := statemachine.DriveTx[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, nil, statemachine.TxSinkFunc(emitOpenedToTopic),
	)
	res := h(context.Background(), msg("evt-1", "c1"))
	if !errors.Is(res.Err, statemachine.ErrNotTransactional) {
		t.Fatalf("nil Transactional: err = %v, want ErrNotTransactional", res.Err)
	}

	h2 := statemachine.DriveTx[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, sub2(), nil,
	)
	if res := h2(context.Background(), msg("evt-1", "c1")); !errors.Is(res.Err, statemachine.ErrNotTransactional) {
		t.Fatalf("nil TxSink: err = %v, want ErrNotTransactional", res.Err)
	}
}

func sub2() source.Transactional { return &fakeTransactional{tx: &fakeTx{}, committed: true} }

func equalCalls(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
