package durable_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestFire_AStepThatTouchesNoActorCostsLittleExtra pins that a machine declaring actors
// pays little extra on a step that touches no actor: a Fire that only moves within the
// state allocates about as much with an actor palette as without one.
func TestFire_AStepThatTouchesNoActorCostsLittleExtra(t *testing.T) {
	ctx := context.Background()
	allocs := func(withActors bool) float64 {
		b := state.ForgeFor[stepCtx]("cost")
		if withActors {
			b = b.Actor("a").Actor("b").Actor("c")
		}
		b = b.State("working")
		if withActors {
			b = b.InvokeActor("a").InvokeActor("b").InvokeActor("c")
		}
		m := b.Initial("working").
			Transition("working").On("poke").GoTo("working").
			Quench()
		var opts []stepOption
		if withActors {
			beh := func(map[string]any) (state.ActorInstance, error) { return &reentryGreeter{sent: true}, nil }
			opts = append(opts, durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"a": beh, "b": beh, "c": beh}))
		}
		h, err := durable.NewRunner(m, durable.NewMemStore(), opts...).Start(ctx, "cost", stepCtx{}, state.WithInitialState("working"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		return testing.AllocsPerRun(50, func() {
			if _, err := h.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire: %v", err)
			}
		})
	}
	without, with := allocs(false), allocs(true)
	t.Logf("allocations per Fire: %.0f without actors, %.0f with three", without, with)
	if with > without+10 {
		t.Fatalf("a Fire that touches no actor allocates %.0f with three declared actors, %.0f without: the actor sets are recomputed every step", with, without)
	}
}
