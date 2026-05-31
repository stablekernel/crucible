package durable_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// Example shows the Store round-trip a durable instance relies on: append the
// per-step journal as an instance advances, checkpoint a full Snapshot to
// compact the tail, then load the checkpoint plus the post-checkpoint tail to
// reconstruct the instance.
func Example() {
	ctx := context.Background()
	store := durable.NewMemStore()
	const id = durable.InstanceID("order-42")

	// Record two steps of nondeterministic results as the instance fires.
	for step := range 2 {
		_, err := store.Append(ctx, id, durable.Record{
			Step: step,
			Entries: []state.JournalEntry{{
				Step:          step,
				Kind:          state.JournalServiceResult,
				CorrelationID: fmt.Sprintf("charge-%d", step),
				Payload:       json.RawMessage(`{"ok":true}`),
			}},
		})
		if err != nil {
			panic(err)
		}
	}

	// Checkpoint a full snapshot through step 0, compacting that step's tail.
	snap, err := state.MarshalSnapshot(state.Snapshot[string, string, map[string]any]{
		Machine:       "order",
		Current:       "charging",
		Configuration: []string{"charging"},
		Context:       map[string]any{"orderID": "42"},
	})
	if err != nil {
		panic(err)
	}
	if cpErr := store.Checkpoint(ctx, id, snap, 0); cpErr != nil {
		panic(cpErr)
	}

	// Load reconstructs from the checkpoint plus the post-checkpoint tail.
	loaded, tail, err := store.Load(ctx, id)
	if err != nil {
		panic(err)
	}
	restored, err := state.UnmarshalSnapshot[string, string, map[string]any](loaded)
	if err != nil {
		panic(err)
	}

	fmt.Println("checkpoint state:", restored.Current)
	fmt.Println("tail steps:", len(tail))
	fmt.Println("first tail step:", tail[0].Step)
	// Output:
	// checkpoint state: charging
	// tail steps: 1
	// first tail step: 1
}
