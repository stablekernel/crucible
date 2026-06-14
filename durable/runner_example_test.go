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
	return state.ForgeFor[*orderCtx]("order").
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
	return state.ForgeFor[*timerOrderCtx]("reminder").
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

// quoteCtx is a small JSON-marshalable context for the invoked-service example.
type quoteCtx struct {
	Quote string `json:"quote"`
}

// quoteMachine invokes a pricing service on entering quoting, folding the returned
// quote into context on its onDone.
func quoteMachine(fn state.ServiceFn[*quoteCtx]) *state.Machine[string, string, *quoteCtx] {
	return state.ForgeFor[*quoteCtx]("quote").
		Service("price", fn).
		Reducer("save", func(in state.AssignCtx[*quoteCtx]) *quoteCtx {
			c := in.Entity
			if q, ok := in.Event.(string); ok {
				c.Quote = q
			}
			return c
		}).
		State("cart").
		State("quoting").Invoke("price", state.WithInvokeOnDone("priced"), state.WithInvokeOnError("failed")).
		State("quoted").Final().
		State("rejected").Final().
		Initial("cart").
		Transition("cart").On("checkout").GoTo("quoting").
		Transition("quoting").On("priced").GoTo("quoted").Assign("save").
		Transition("quoting").On("failed").GoTo("rejected").
		Quench()
}

// ExampleHandle_RunService shows the invoked-service record/replay seam: a pricing
// service runs exactly once on the live path (its returned quote recorded), and on
// recovery the recorded quote is replayed back through the same settle seam without
// re-invoking the service — so the recovered instance reaches the same state and
// context, and the service's call count never advances past one.
func ExampleHandle_RunService() {
	ctx := context.Background()
	var calls int
	fn := func(context.Context, state.ServiceCtx[*quoteCtx]) (any, error) {
		calls++
		return fmt.Sprintf("quote-%d", calls), nil
	}
	reg := state.NewRegistry[*quoteCtx]().Service("price", fn)
	m := quoteMachine(fn)
	store := durable.NewMemStore()
	const id = durable.InstanceID("quote-1")

	runner := durable.NewRunner(m, store, durable.WithServiceRegistry[string, string, *quoteCtx](reg))
	h, err := runner.Start(ctx, id, &quoteCtx{}, state.WithInitialState("cart"))
	if err != nil {
		panic(err)
	}
	if _, err = h.Fire(ctx, "checkout"); err != nil {
		panic(err)
	}
	// Run the pricing service: it executes once and routes its result through onDone.
	if _, _, err = h.RunService(ctx, state.InvokeID("quote", "quoting", 0)); err != nil {
		panic(err)
	}

	// Recover: the recorded quote is replayed; the service is not run again.
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithServiceRegistry[string, string, *quoteCtx](reg))
	if err != nil {
		panic(err)
	}
	snap := recovered.Instance().Snapshot()
	fmt.Println("recovered state:", snap.Current)
	fmt.Println("recovered quote:", snap.Context.Quote)
	fmt.Println("service calls:", calls)
	// Output:
	// recovered state: quoted
	// recovered quote: quote-1
	// service calls: 1
}

// fulfillmentCtx is a small JSON-marshalable context for the actor example.
type fulfillmentCtx struct {
	Tracking string `json:"tracking"`
}

// fulfillmentMachine supervises a child shipping actor: it spawns the actor on
// entering supervising and folds the actor's done-data (a tracking number) into
// context on its onDone.
func fulfillmentMachine() *state.Machine[string, string, *fulfillmentCtx] {
	return state.ForgeFor[*fulfillmentCtx]("fulfillment").
		Reducer("track", func(in state.AssignCtx[*fulfillmentCtx]) *fulfillmentCtx {
			c := in.Entity
			if tr, ok := in.Event.(string); ok {
				c.Tracking = tr
			}
			return c
		}).
		Actor("ship").
		State("supervising").InvokeActor("ship", state.WithInvokeOnDone("shipped"), state.WithInvokeOnError("failed")).
		State("complete").Final().
		State("aborted").Final().
		Initial("supervising").
		Transition("supervising").On("shipped").GoTo("complete").Assign("track").
		Transition("supervising").On("failed").GoTo("aborted").
		Quench()
}

// ExampleHandle_DeliverToActor shows the actor record/replay seam: a child shipping
// actor runs exactly once on the live path (its done-data recorded), and on recovery
// the recorded done-data is replayed back through the same parent onDone without
// re-running the actor — so the recovered instance reaches the same state and
// context, and the actor's run count never advances past one.
func ExampleHandle_DeliverToActor() {
	ctx := context.Background()
	var runs int
	palette := map[string]state.ActorBehavior{
		"ship": func(map[string]any) (state.ActorInstance, error) {
			child := state.ForgeFor[*fulfillmentCtx]("shipper").
				State("packing").
				State("shipped").Final().
				Initial("packing").
				Transition("packing").On("dispatch").GoTo("shipped").
				Quench()
			inst := child.Cast(&fulfillmentCtx{}, state.WithInitialState[string]("packing"))
			return state.NewActor(inst, func(*state.Instance[string, string, *fulfillmentCtx]) any {
				runs++
				return fmt.Sprintf("TRK-%d", runs)
			}), nil
		},
	}
	m := fulfillmentMachine()
	store := durable.NewMemStore()
	const id = durable.InstanceID("ship-1")

	runner := durable.NewRunner(m, store, durable.WithActorPalette[string, string, *fulfillmentCtx](palette))
	h, err := runner.Start(ctx, id, &fulfillmentCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		panic(err)
	}
	// Drive the shipping actor to completion: it runs once and routes its tracking
	// number through the parent onDone.
	ref, ok := h.ActorRef(state.ActorID("fulfillment", "supervising", 0))
	if !ok {
		panic("no actor ref")
	}
	if _, err = h.DeliverToActor(ctx, ref, "dispatch"); err != nil {
		panic(err)
	}

	// Recover: the recorded done-data is replayed; the actor is not run again.
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithActorPalette[string, string, *fulfillmentCtx](palette))
	if err != nil {
		panic(err)
	}
	snap := recovered.Instance().Snapshot()
	fmt.Println("recovered state:", snap.Current)
	fmt.Println("recovered tracking:", snap.Context.Tracking)
	fmt.Println("actor runs:", runs)
	// Output:
	// recovered state: complete
	// recovered tracking: TRK-1
	// actor runs: 1
}
