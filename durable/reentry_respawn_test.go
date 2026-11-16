package durable_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// reentryGreeter is an actor that sends its parent "hello" as it starts.
type reentryGreeter struct{ sent bool }

func (g *reentryGreeter) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (g *reentryGreeter) ChildEffects() []state.Effect {
	if g.sent {
		return nil
	}
	g.sent = true
	return []state.Effect{state.SendParent{Event: "hello"}}
}
func (g *reentryGreeter) Output() any { return nil }

// TestRecover_ARetriedReentrySpawnDoesNotRepeatTheFirstStartsMessage pins that when an
// actor started, sent its parent a message, and then failed to start again on a
// re-entry with no onError, Recover's retry doesn't apply that first message again: the
// journal already holds it, and a retry that still fails sends nothing.
func TestRecover_ARetriedReentrySpawnDoesNotRepeatTheFirstStartsMessage(t *testing.T) {
	ctx := context.Background()
	note := func(in state.AssignCtx[stepCtx]) stepCtx {
		c := in.Entity
		c.Notes = append(c.Notes, fmt.Sprint(in.Event))
		return c
	}
	m := state.ForgeFor[stepCtx]("reentryRespawn").
		Reducer("note", note).
		Actor("greeter").
		State("greeting").InvokeActor("greeter").
		Initial("greeting").
		Transition("greeting").On("hello").GoTo("greeting").Assign("note").
		Transition("greeting").On("re").GoTo("greeting").Reenter().Assign("note").
		Transition("greeting").On("poke").GoTo("greeting").Assign("note").
		Quench()
	var calls int
	opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
		"greeter": func(map[string]any) (state.ActorInstance, error) {
			calls++
			if calls > 1 {
				return nil, errors.New("the greeter can't start again")
			}
			return &reentryGreeter{}, nil
		},
	})}
	store, id := durable.NewMemStore(), durable.InstanceID("reentry-respawn")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("greeting"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"poke", "re", "poke"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%s): %v", ev, err)
		}
	}
	live := fmt.Sprint(h.Instance().Entity().Notes)
	if live != "[hello poke re poke]" {
		t.Fatalf("live notes = %s, want the first start's hello, then poke, re and poke", live)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := fmt.Sprint(recovered.Instance().Entity().Notes); got != live {
		t.Fatalf("recovered notes = %s, want the live %s (behavior ran %d times)", got, live, calls)
	}
}
