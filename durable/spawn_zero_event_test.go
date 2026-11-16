package durable_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestRecover_RetriesANoOnErrorSpawnEvenWhenTheZeroEventHasATransition pins that a
// failed spawn with no onError declared escalates rather than firing the zero event, so
// a transition the parent declares on that event never runs, live or after Recover,
// and Recover retries the spawn: nothing declared for the failure caught it.
func TestRecover_RetriesANoOnErrorSpawnEvenWhenTheZeroEventHasATransition(t *testing.T) {
	t.Skip("an InvokeActor with no onError fires the zero event, so durable tells none declared from refused by the reason string; #250")
	ctx := context.Background()
	m := state.ForgeFor[stepCtx]("zeroOnError").
		Reducer("note", func(in state.AssignCtx[stepCtx]) stepCtx {
			c := in.Entity
			c.Notes = append(c.Notes, "caught")
			return c
		}).
		Actor("child").
		State("idle").
		SuperState("spawning").InvokeActor("child").
		SubState("waiting").
		SubState("caught").
		Initial("waiting").
		EndSuperState().
		Initial("idle").
		Transition("idle").On("start").GoTo("waiting").
		// The zero event moves between children, so spawning, and its actor, stay.
		Transition("waiting").On("").GoTo("caught").Assign("note").
		Transition("caught").On("poke").GoTo("caught").
		Quench()
	store, id := durable.NewMemStore(), durable.InstanceID("zero-onerror")
	var calls int
	opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
		"child": failingSpawn(func() { calls++ }),
	})}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	live := fmt.Sprint(h.Instance().Entity().Notes)
	if live != "[]" {
		t.Fatalf("live notes = %s, want none: the failure escalates, firing no zero event", live)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := fmt.Sprint(recovered.Instance().Entity().Notes); got != live || calls != 2 {
		t.Fatalf("recovered notes = %s after %d behavior runs, want the live %s and the retry's second run", got, calls, live)
	}
}

// TestRecover_RetriesANoOnErrorSpawnAWildcardSwallowed pins that a spawn with no onError
// declared is retried at Recover when the parent's state swallows its zero event with a
// catch-all or a forbid: nothing declared for the failure caught it.
func TestRecover_RetriesANoOnErrorSpawnAWildcardSwallowed(t *testing.T) {
	t.Skip("an InvokeActor with no onError fires the zero event, so durable tells none declared from refused by the reason string; #250")
	for _, how := range []string{"forbid any", "on any"} {
		t.Run(how, func(t *testing.T) {
			b := state.ForgeFor[stepCtx]("wildcard").
				Actor("child").
				State("idle").
				State("spawning").InvokeActor("child").
				Initial("idle").
				Transition("idle").On("start").GoTo("spawning")
			if how == "forbid any" {
				b = b.State("spawning").ForbidAny()
			} else {
				b = b.Transition("spawning").OnAny().GoTo("spawning")
			}
			m := b.Quench()
			if got := spawnRuleCalls(t, m); got != 2 {
				t.Fatalf("behavior ran %d times, want Recover to retry it once", got)
			}
		})
	}
}
