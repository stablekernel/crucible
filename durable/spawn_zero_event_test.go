package durable_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestRecover_DoesNotReapplyAnAppliedZeroEventOnError pins a failed spawn with no
// onError declared whose zero event the parent handles: the zero event applied live,
// and the journal holds it, so Recover must not retry the spawn and apply it again.
func TestRecover_DoesNotReapplyAnAppliedZeroEventOnError(t *testing.T) {
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
	if live != "[caught]" {
		t.Fatalf("live notes = %s, want the zero event to apply once", live)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := fmt.Sprint(recovered.Instance().Entity().Notes); got != live || calls != 1 {
		t.Fatalf("recovered notes = %s after %d behavior runs, want the live %s and one run", got, calls, live)
	}
}
