package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// cancelingActor wraps an actor and ends the step's context just before the actor
// handles a delivered event, as a deadline passing mid-delivery would.
type cancelingActor struct {
	inner  state.ActorInstance
	cancel context.CancelFunc
}

func (a *cancelingActor) DeliverFire(ctx context.Context, ev any) (bool, any) {
	a.cancel()
	return a.inner.DeliverFire(ctx, ev)
}
func (a *cancelingActor) ChildEffects() []state.Effect { return a.inner.ChildEffects() }
func (a *cancelingActor) Output() any                  { return a.inner.Output() }

// TestDeliverToActor_AContextThatEndsBeforeTheActorHandlesItFails pins that a delivery
// whose context ends before the actor handles the event, so the actor's own Fire is
// refused and the event is lost, fails rather than reporting a delivery that never
// happened.
func TestDeliverToActor_AContextThatEndsBeforeTheActorHandlesItFails(t *testing.T) {
	ctx := context.Background()
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	child := state.ForgeFor[stepCtx]("kid").
		State("working").
		State("noted").
		Initial("working").
		Transition("working").On("note").GoTo("noted").
		Quench()
	m := state.ForgeFor[stepCtx]("deliverCtx").
		Actor("kid").
		State("working").InvokeActor("kid", state.WithInvokeID("kid")).
		Initial("working").
		Transition("working").On("poke").GoTo("working").
		Quench()
	opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
		"kid": func(map[string]any) (state.ActorInstance, error) {
			inner := state.NewActor(child.Cast(stepCtx{}, state.WithInitialState("working")), nil)
			return &cancelingActor{inner: inner, cancel: cancel}, nil
		},
	})}
	h, err := durable.NewRunner(m, durable.NewMemStore(), opts...).Start(ctx, "deliver-ctx", stepCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	found, err := h.DeliverToActorByID(turn, "kid", "note")
	if !found {
		t.Fatal("the kid isn't running")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DeliverToActor = %v, want an error wrapping context.Canceled: the kid never handled the event", err)
	}
}
