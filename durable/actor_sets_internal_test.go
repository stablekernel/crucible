package durable

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

type setsCtx struct {
	Notes []string `json:"notes"`
}

// TestActorSets_FollowTheActorsState proves a step that touched no actor leaves both
// actor sets as they were, while a step that leaves the actor's state ends it.
func TestActorSets_FollowTheActorsState(t *testing.T) {
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
	if len(ended.IDs) != 0 || len(unstarted.IDs) != 0 {
		t.Fatalf("sets = %v, %v, want none: the actor is running", ended.IDs, unstarted.IDs)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	if ended, unstarted = h.actorSets(); len(ended.IDs) != 0 || len(unstarted.IDs) != 0 {
		t.Fatalf("sets = %v, %v after a step that touched no actor, want none", ended.IDs, unstarted.IDs)
	}
	if _, err = h.Fire(ctx, "leave"); err != nil {
		t.Fatalf("Fire(leave): %v", err)
	}
	if ended, unstarted = h.actorSets(); len(ended.IDs) != 0 || len(unstarted.IDs) != 0 {
		t.Fatalf("sets = %v, %v after leaving the actor's state, want none: idle declares no actor", ended.IDs, unstarted.IDs)
	}
}

// TestActorSets_ListsAnEndedActorOnce proves an actor id two active states declare is
// listed once when the actor has ended.
func TestActorSets_ListsAnEndedActorOnce(t *testing.T) {
	ctx := context.Background()
	child := state.ForgeFor[setsCtx]("child").
		State("working").
		State("done").Final().
		Initial("working").
		Transition("working").On("finish").GoTo("done").
		Quench()
	m := state.ForgeFor[setsCtx]("twice").
		Actor("child").
		State("off").
		SuperState("par").
		Region("left").
		Initial("p").
		SubState("p").InvokeActor("child", state.WithInvokeID("v")).
		EndRegion().
		Region("right").
		Initial("q").
		SubState("q").InvokeActor("child", state.WithInvokeID("v")).
		EndRegion().
		EndSuperState().
		Initial("off").
		Transition("off").On("go").GoTo("par").
		Quench()
	palette := map[string]state.ActorBehavior{"child": func(map[string]any) (state.ActorInstance, error) {
		return state.NewActor(child.Cast(setsCtx{}, state.WithInitialState("working")), nil), nil
	}}
	h, err := NewRunner(m, NewMemStore(), WithActorPalette[string, string, setsCtx](palette)).
		Start(ctx, "twice", setsCtx{}, state.WithInitialState("off"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	if found, deliverErr := h.DeliverToActorByID(ctx, "v", "finish"); !found || deliverErr != nil {
		t.Fatalf("DeliverToActorByID = %v, %v", found, deliverErr)
	}
	if ended, _ := h.actorSets(); len(ended.IDs) != 1 || ended.IDs[0] != "v" {
		t.Fatalf("ended = %v, want [v]", ended.IDs)
	}
}
