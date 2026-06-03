// SPDX-License-Identifier: Apache-2.0

package statemachine_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/cdc"
	"github.com/stablekernel/crucible/source/statemachine"
	"github.com/stablekernel/crucible/state"
)

// ExampleDrive_cdc shows the intended change-data-capture pattern: a Debezium
// topic carries row changes for a turnstile table, a cdc.Codec decodes each
// change event, and a Router projects the decoded row into the instance key and
// the event to fire. The Hopper then drives the statechart per primary key,
// acking only after the transition is durably persisted.
//
// Here a single update row (the turnstile becomes funded) decodes to a coin
// event, unlocking the machine. No broker is involved; the message stands in for
// one Debezium record off the topic.
func ExampleDrive_cdc() {
	machine := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()

	// Seed a funded turnstile in the locked state at version 1.
	seeded := machine.Cast(&turnstile{Funded: true}, state.WithInitialState[turnstileState](locked))
	_ = store.Save(context.Background(), locked,
		statemachine.Record[turnstileState, turnstileEvent, *turnstile]{Snapshot: seeded.Snapshot(), Version: 1}, 0)

	// One codec, registered as the registry default for the CDC topic.
	registry := source.NewRegistry().SetDefault(cdc.New())

	// The Router decodes the change event and projects its after-image into a
	// (key, event): a create or update on a funded row drives the coin event.
	router := func(m source.Message) (turnstileState, turnstileEvent, error) {
		event, err := cdc.DecodeEvent(registry, m)
		if err != nil {
			return 0, 0, err
		}
		row, err := cdc.AfterAs[turnstile](event)
		if err != nil {
			return 0, 0, fmt.Errorf("cdc example: project after-image: %w", err)
		}
		if !row.Funded {
			return 0, 0, fmt.Errorf("cdc example: row not funded")
		}
		return locked, coin, nil
	}

	sink := statemachine.SinkFunc(func(_ context.Context, eff any) error {
		fmt.Printf("emit: %v\n", eff)
		return nil
	})
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](
		machine, store, router, statemachine.WithSink(sink),
	)

	// A Debezium update envelope: the row's funded column flips true.
	change := cdcMessage(`{
		"op":"u",
		"before":{"funded":false},
		"after":{"funded":true},
		"source":{"connector":"postgresql","db":"gate","schema":"public","table":"turnstile"},
		"ts_ms":1700000000000
	}`, "lsn-100")

	res := h(context.Background(), change)
	fmt.Println("result:", res.Action)

	rec, _, _ := store.Load(context.Background(), locked)
	fmt.Println("state:", rec.Snapshot.Current, "version:", rec.Version)

	// Output:
	// emit: {coin}
	// result: ack
	// state: unlocked version: 2
}

// cdcMessage builds a fakeMessage carrying a Debezium JSON change event on a
// CDC topic, with the cursor standing in for the source log position.
func cdcMessage(payload, cursor string) fakeMessage {
	return fakeMessage{
		value:   []byte(payload),
		subject: "gate.public.turnstile",
		cursor:  fakeCursor(cursor),
	}
}
