package durable

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

type setsCtx struct {
	Notes []string `json:"notes"`
}

// TestActorSets_SkipsAStepThatTouchedNoActor proves a step that spawned nothing and
// changed neither the configuration nor the running actors gets the last actor sets
// back without recomputing them, while a step that moves the configuration recomputes.
func TestActorSets_SkipsAStepThatTouchedNoActor(t *testing.T) {
	ctx := context.Background()
	child := state.ForgeFor[setsCtx]("child").
		State("working").
		State("done").Final().
		Initial("working").
		Transition("working").On("finish").GoTo("done").
		Quench()
	m := state.ForgeFor[setsCtx]("sets").
		Actor("child").
		State("working").InvokeActor("child").
		State("idle").
		Initial("working").
		Transition("working").On("poke").GoTo("working").
		Transition("working").On("leave").GoTo("idle").
		Quench()
	palette := map[string]state.ActorBehavior{"child": func(map[string]any) (state.ActorInstance, error) {
		return state.NewActor(child.Cast(setsCtx{}, state.WithInitialState("working")), nil), nil
	}}
	h, err := NewRunner(m, NewMemStore(), WithActorPalette[string, string, setsCtx](palette)).
		Start(ctx, "sets", setsCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	ended, unstarted := h.actorSets()
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	again, againUnstarted := h.actorSets()
	if again != ended || againUnstarted != unstarted {
		t.Fatal("a step that touched no actor recomputed the actor sets")
	}
	if _, err = h.Fire(ctx, "leave"); err != nil {
		t.Fatalf("Fire(leave): %v", err)
	}
	if moved, _ := h.actorSets(); moved == ended {
		t.Fatal("a step that left the actor's state reused the old actor sets")
	}
}
