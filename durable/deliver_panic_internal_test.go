package durable

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// messenger is an actor that sends its parent "boom" when it's delivered an event.
type messenger struct{ out []state.Effect }

func (a *messenger) DeliverFire(context.Context, any) (bool, any) {
	a.out = []state.Effect{state.SendParent{Event: "boom"}}
	return false, nil
}

func (a *messenger) ChildEffects() []state.Effect {
	out := a.out
	a.out = nil
	return out
}
func (a *messenger) Output() any { return nil }

// TestDeliverToActor_APanicLeavesNoStepContextBehind pins that a panic during a
// delivery, here in the parent's middleware for a message the actor sent, doesn't leave the actor collector holding the dead step's context, which a
// later parent Fire on the system would arm its timers and services under.
func TestDeliverToActor_APanicLeavesNoStepContextBehind(t *testing.T) {
	ctx := context.Background()
	type cx struct{}
	m := state.ForgeFor[cx]("panicky").
		Use(func(next state.FireFunc[string, string, cx]) state.FireFunc[string, string, cx] {
			return func(ctx context.Context, ev string) state.FireResult[string] {
				if ev == "boom" {
					panic("the parent's middleware panicked")
				}
				return next(ctx, ev)
			}
		}).
		Actor("kid").
		State("working").InvokeActor("kid", state.WithInvokeID("kid")).
		Initial("working").
		Transition("working").On("boom").GoTo("working").
		Quench()
	h, err := NewRunner(m, NewMemStore(), WithActorPalette[string, string, cx](map[string]state.ActorBehavior{
		"kid": func(map[string]any) (state.ActorInstance, error) { return &messenger{}, nil },
	})).Start(ctx, "panicky", cx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_, _ = h.DeliverToActorByID(ctx, "kid", "go")
	}()
	if !panicked {
		t.Skip("no host panic reaches DeliverToActor: the kernel and the actor system recover them")
	}
	h.actorFires.mu.Lock()
	left := h.actorFires.ctx
	h.actorFires.mu.Unlock()
	if left != nil {
		t.Fatal("the actor collector still holds the panicked step's context")
	}
}
