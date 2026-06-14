package state_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// access is the entity the combinator example guards against.
type access struct {
	admin   bool
	auditor bool
}

// ExampleAnd composes named-ref guards and the stateIn built-in into a single
// boolean guard expression on a transition with And/Or/Not, exercising the
// guard combinators. The transition fires only when the composite passes; And
// short-circuits at the first false and Or at the first true.
func ExampleAnd() {
	m := state.ForgeFor[access]("door").
		Guard("admin", func(c state.GuardCtx[access]) bool { return c.Entity.admin }).
		Guard("auditor", func(c state.GuardCtx[access]) bool { return c.Entity.auditor }).
		State("locked").
		Transition("locked").On("open").GoTo("open").
		// Enabled while in "locked" AND (admin OR auditor).
		WhenExpr(state.And(
			state.StateIn("locked"),
			state.Or(state.Guard[string]("admin"), state.Guard[string]("auditor")),
		)).
		State("open").
		Initial("locked").
		Quench()

	denied := m.Cast(access{}, state.WithInitialState("locked"))
	denied.Fire(context.Background(), "open")
	fmt.Println("no role:", denied.Current())

	allowed := m.Cast(access{auditor: true}, state.WithInitialState("locked"))
	allowed.Fire(context.Background(), "open")
	fmt.Println("auditor:", allowed.Current())
	// Output:
	// no role: locked
	// auditor: open
}
