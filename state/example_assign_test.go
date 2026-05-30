package state_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// basket is a value-semantics context: an Assign returns a new basket, and guards and
// actions receive a copy they cannot use to mutate the instance.
type basket struct {
	Total int
}

// ExampleBuilder_Assign demonstrates the assign reducer — the sole context writer.
// Under value-semantics context, a guard or action receives a copy of the context
// and cannot change the instance; only an Assign, a pure reducer returning the next
// context, updates it. The reducer reads the triggering event from AssignCtx.Event
// and its static configuration from AssignCtx.Params.
func ExampleBuilder_Assign() {
	m := state.Forge[string, string, basket]("checkout").
		Reducer("addItem", func(in state.AssignCtx[basket]) basket {
			c := in.Entity
			if price, ok := in.Params["price"].(int); ok {
				c.Total += price
			}
			return c
		}).
		State("shopping").
		State("paid").
		Initial("shopping").
		Transition("shopping").On("add").GoTo("shopping").
		Assign("addItem", map[string]any{"price": 300}).
		Transition("shopping").On("checkout").GoTo("paid").
		Quench()

	inst := m.Cast(basket{}, state.WithInitialState[string]("shopping"))
	inst.Fire(context.Background(), "add")
	inst.Fire(context.Background(), "add")

	fmt.Println(inst.Entity().Total)
	// Output: 600
}
