package durable_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// ExampleStateAt reconstructs an instance's state as of an earlier recorded step,
// read-only, without re-running any work or mutating the live instance. A
// history-retaining MemStore (WithHistory) keeps every Record so any step is
// reachable; Steps enumerates the recorded ordinals to read across.
func ExampleStateAt() {
	ctx := context.Background()
	m := state.Forge[string, string, *auditCtx]("audit").
		Action("bump", func(c state.ActionCtx[*auditCtx]) (state.Effect, error) {
			c.Entity.Count++
			return nil, nil
		}).
		State("open").
		State("closed").Final().
		Initial("open").
		Transition("open").On("tick").GoTo("open").Do("bump").
		Transition("open").On("close").GoTo("closed").Do("bump").
		Quench()

	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("ledger-1")
	runner := durable.NewRunner(m, store)
	h, err := runner.Start(ctx, id, &auditCtx{}, state.WithInitialState("open"))
	if err != nil {
		panic(err)
	}
	for _, ev := range []string{"tick", "tick", "close"} {
		if _, ferr := h.Fire(ctx, ev); ferr != nil {
			panic(ferr)
		}
	}

	// Enumerate the recorded steps, then reconstruct the count at each one.
	steps, err := durable.Steps(ctx, store, id)
	if err != nil {
		panic(err)
	}
	for _, step := range steps {
		view, verr := durable.StateAt(ctx, m, store, id, step)
		if verr != nil {
			panic(verr)
		}
		fmt.Printf("step %d: state=%s count=%d\n",
			step, view.Snapshot().Current, view.Snapshot().Context.Count)
	}
	// Output:
	// step 0: state=open count=1
	// step 1: state=open count=2
	// step 2: state=closed count=3
}

// auditCtx is a JSON-marshalable context counting the bumps a time-travel read
// reconstructs at each step.
type auditCtx struct {
	Count int `json:"count"`
}
