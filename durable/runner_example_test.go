package durable_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// orderCtx is a small JSON-marshalable context for the Runner example.
type orderCtx struct {
	Charges int `json:"charges"`
}

// orderMachine is a flat event-driven order machine: pending -> charged -> done.
func orderMachine() *state.Machine[string, string, *orderCtx] {
	return state.Forge[string, string, *orderCtx]("order").
		Action("charge", func(c state.ActionCtx[*orderCtx]) (state.Effect, error) {
			c.Entity.Charges++
			return nil, nil
		}).
		State("pending").
		State("charged").
		State("done").Final().
		Initial("pending").
		Transition("pending").On("pay").GoTo("charged").Do("charge").
		Transition("charged").On("ship").GoTo("done").
		Quench()
}

// ExampleRunner shows the durable record/replay loop: start an instance, fire a
// sequence of events through the Runner (each step recorded write-ahead), then
// recover a fresh instance purely from the Store and observe it reaches the same
// state and context the live run did.
func ExampleRunner() {
	ctx := context.Background()
	m := orderMachine()
	store := durable.NewMemStore()
	const id = durable.InstanceID("order-7")

	// Start records a baseline checkpoint; each Fire appends the driving event,
	// and the checkpoint policy compacts the tail every two steps.
	runner := durable.NewRunner(m, store, durable.WithCheckpointEvery[string, string, *orderCtx](2))
	if _, err := runner.Start(ctx, id, &orderCtx{}, state.WithInitialState("pending")); err != nil {
		panic(err)
	}
	for _, ev := range []string{"pay", "ship"} {
		if _, err := runner.Fire(ctx, id, ev); err != nil {
			panic(err)
		}
	}

	// Recover reconstructs the instance from the Store alone — Load the checkpoint,
	// Restore it, replay the recorded tail.
	recovered, err := durable.Recover(ctx, m, store, id)
	if err != nil {
		panic(err)
	}
	snap := recovered.Instance().Snapshot()

	fmt.Println("recovered state:", snap.Current)
	fmt.Println("recovered charges:", snap.Context.Charges)
	// Output:
	// recovered state: done
	// recovered charges: 1
}
