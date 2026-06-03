// SPDX-License-Identifier: Apache-2.0

package statemachine_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/statemachine"
	"github.com/stablekernel/crucible/state"
)

// ExampleDrive binds a turnstile statechart to a source.Handler: consuming a
// "coin" message fires the unlock transition, hands the emitted effect to a sink,
// persists the new state, and acks only after that durable commit. A redelivery
// of the same message is a no-op ack — exactly-once into the machine.
func ExampleDrive() {
	machine := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()

	// Seed a funded turnstile in the locked state at version 1.
	seeded := machine.Cast(&turnstile{Funded: true}, state.WithInitialState[turnstileState](locked))
	_ = store.Save(context.Background(), locked,
		statemachine.Record[turnstileState, turnstileEvent, *turnstile]{Snapshot: seeded.Snapshot(), Version: 1}, 0)

	// Route every message to the one instance, firing coin.
	router := func(source.Message) (turnstileState, turnstileEvent, error) {
		return locked, coin, nil
	}
	sink := statemachine.SinkFunc(func(_ context.Context, eff any) error {
		fmt.Printf("emit: %v\n", eff)
		return nil
	})

	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](
		machine, store, router, statemachine.WithSink(sink),
	)

	first := h(context.Background(), msg("coin-1", "cursor-1"))
	fmt.Println("first:", first.Action)

	// Redeliver the same message id: skipped, acked, not re-applied.
	again := h(context.Background(), msg("coin-1", "cursor-1"))
	fmt.Println("redelivery:", again.Action, again.Class)

	rec, _, _ := store.Load(context.Background(), locked)
	fmt.Println("state:", rec.Snapshot.Current, "version:", rec.Version)

	// Output:
	// emit: {coin}
	// first: ack
	// redelivery: ack drop
	// state: unlocked version: 2
}

// exampleTx is a tiny in-memory source.Tx for the DriveTx example: it prints the
// records produced into the transaction so the output is observable without a
// broker.
type exampleTx struct{}

func (exampleTx) Produce(_ context.Context, records ...source.ProducedRecord) error {
	for _, r := range records {
		fmt.Printf("produce: %s -> %s\n", r.Topic, r.Value)
	}
	return nil
}

// exampleTransactional is a tiny in-memory source.Transactional for the example:
// it runs the work function and commits, mirroring how Kafka's EOS session would
// commit the produced records and the consumed offset atomically.
type exampleTransactional struct{}

func (exampleTransactional) Begin(ctx context.Context, _ source.Message, fn func(context.Context, source.Tx) error) error {
	return fn(ctx, exampleTx{})
}

// ExampleDriveTx binds a turnstile statechart to a source.Handler with
// exactly-once consume-process-produce: firing the unlock transition produces the
// emitted effect AND commits the consumed offset in one transaction, so the
// handler returns Manual (the engine takes no further settle action).
func ExampleDriveTx() {
	machine := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()

	seeded := machine.Cast(&turnstile{Funded: true}, state.WithInitialState[turnstileState](locked))
	_ = store.Save(context.Background(), locked,
		statemachine.Record[turnstileState, turnstileEvent, *turnstile]{Snapshot: seeded.Snapshot(), Version: 1}, 0)

	router := func(source.Message) (turnstileState, turnstileEvent, error) {
		return locked, coin, nil
	}
	// The TxSink turns each emitted effect into a record produced inside the
	// transaction.
	txSink := statemachine.TxSinkFunc(func(ctx context.Context, tx source.Tx, eff any) error {
		if oe, ok := eff.(openedEffect); ok {
			return tx.Produce(ctx, source.ProducedRecord{Topic: "turnstile.out", Value: []byte("opened by " + oe.By)})
		}
		return nil
	})

	h := statemachine.DriveTx[turnstileState, turnstileEvent, *turnstile](
		machine, store, router, exampleTransactional{}, txSink,
	)

	res := h(context.Background(), msg("coin-1", "cursor-1"))
	fmt.Println("action:", res.Action)

	rec, _, _ := store.Load(context.Background(), locked)
	fmt.Println("state:", rec.Snapshot.Current, "version:", rec.Version)

	// Output:
	// produce: turnstile.out -> opened by coin
	// action: manual
	// state: unlocked version: 2
}

// ExampleCheckEvents validates that a consumer's accepted event union is
// exhaustive against the machine's event alphabet, reporting an event the machine
// can never handle (which would always be rejected as invalid-for-state).
func ExampleCheckEvents() {
	machine := buildTurnstile()

	c := statemachine.CheckEvents(machine, []turnstileEvent{coin, push, maintenance})
	fmt.Println("exhaustive:", c.Exhaustive())
	fmt.Println("unreachable:", c.Unreachable)

	// Output:
	// exhaustive: false
	// unreachable: [maintenance]
}
