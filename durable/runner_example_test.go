package durable_test

import (
	"context"
	"fmt"
	"time"

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

// timerOrderCtx is a small JSON-marshalable context for the durable-timer example.
type timerOrderCtx struct {
	Reminded bool `json:"reminded"`
}

// reminderMachine sends a reminder a fixed delay after an order is placed, driven
// by a delayed (`after`) transition.
func reminderMachine() *state.Machine[string, string, *timerOrderCtx] {
	return state.Forge[string, string, *timerOrderCtx]("reminder").
		Action("remind", func(c state.ActionCtx[*timerOrderCtx]) (state.Effect, error) {
			c.Entity.Reminded = true
			return nil, nil
		}).
		State("new").
		State("waiting").
		State("reminded").Final().
		Initial("new").
		Transition("new").On("place").GoTo("waiting").
		Transition("waiting").After(time.Hour).On("due").GoTo("reminded").Do("remind").
		Quench()
}

// ExampleRunner_durableTimer shows a time-dependent machine recorded and replayed
// through the clock seam: the live run arms a one-hour reminder and fires it by
// advancing a fake clock; recovery on a different wall-clock baseline replays the
// recorded clock readings, so the reminder fires at its recorded instant and the
// recovered instance matches the live one — wall-clock-independent.
func ExampleRunner_durableTimer() {
	ctx := context.Background()
	m := reminderMachine()
	store := durable.NewMemStore()
	const id = durable.InstanceID("order-9")

	start := time.Date(2025, 6, 1, 9, 0, 0, 0, time.UTC)
	clk := state.NewFakeClock(start)
	runner := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, *timerOrderCtx](clk))
	h, err := runner.Start(ctx, id, &timerOrderCtx{}, state.WithInitialState("new"))
	if err != nil {
		panic(err)
	}
	if _, err = h.Fire(ctx, "place"); err != nil {
		panic(err)
	}
	clk.Advance(2 * time.Hour) // past the one-hour reminder deadline
	if _, err = h.Tick(ctx); err != nil {
		panic(err)
	}

	// Recover on a wall clock days later: the reminder still fired at its recorded
	// instant, so the recovered instance reaches the same state.
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerOrderCtx](state.NewFakeClock(start.Add(72*time.Hour))))
	if err != nil {
		panic(err)
	}
	snap := recovered.Instance().Snapshot()
	fmt.Println("recovered state:", snap.Current)
	fmt.Println("reminded:", snap.Context.Reminded)
	// Output:
	// recovered state: reminded
	// reminded: true
}
