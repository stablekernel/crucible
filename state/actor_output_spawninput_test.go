package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file pins two previously-uncovered exported actor seams:
//
//   - ActorInstance.Output (actor.go:381, via the *actorAdapter the exported
//     NewActor returns): it must return nil before the child reaches its final
//     state and the extracted completion output once it does.
//   - WithSpawnInput (options.go:125): the input map declared on a dynamic Spawn
//     must reach the spawned actor's ActorBehavior verbatim, so a child can be
//     seeded from it.

// TestOutput_NilBeforeFinal_ValueAfterFinal pins ActorInstance.Output across the
// completion boundary. We hold the ActorInstance the behavior returns so we can
// call Output() directly: before the child reaches "done" it must report nil, and
// once "finish" steps it to its final state the output extractor's value surfaces.
func TestOutput_NilBeforeFinal_ValueAfterFinal(t *testing.T) {
	cm := state.Forge[string, string, *childEntity]("outchild").
		Action("record", func(c state.ActionCtx[*childEntity]) (state.Effect, error) {
			c.Entity.result = "done-output"
			return nil, nil
		}).
		State("working").
		State("done").Final().OnEntry("record").
		Initial("working").
		Transition("working").On("finish").GoTo("done").
		Quench()

	// Capture the ActorInstance returned by NewActor so Output() can be called on
	// it directly across the completion boundary.
	var captured state.ActorInstance
	behavior := func(input map[string]any) (state.ActorInstance, error) {
		inst := cm.Cast(&childEntity{}, state.WithInitialState("working"))
		ai := state.NewActor(inst, func(i *state.Instance[string, string, *childEntity]) any {
			return i.Entity().result
		})
		captured = ai
		return ai, nil
	}

	parent := state.Forge[string, string, *trec]("outparent").
		State("idle").
		State("supervising").InvokeActor("outchild",
		state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childErr")).
		State("complete").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childDone").GoTo("complete").
		Quench()

	p := parent.Cast(&trec{}, state.WithInitialState("idle"))
	sys := state.NewActorSystem(p).Register("outchild", behavior)
	ctx := context.Background()

	res := p.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if captured == nil {
		t.Fatal("ActorBehavior was not invoked; no ActorInstance captured")
	}

	// Before the child reaches its final state, Output() must be nil.
	if got := captured.Output(); got != nil {
		t.Fatalf("Output() before final = %v, want nil", got)
	}

	id := state.ActorID(parent.Name(), "supervising", 0)
	ref, ok := sys.Ref(id)
	if !ok {
		t.Fatalf("no actor ref for id %q", id)
	}
	if !sys.Deliver(ctx, ref, "finish") {
		t.Fatal("Deliver(finish) returned false; actor not running")
	}

	// After the child reached "done" (final), Output() returns the extracted value.
	if got := captured.Output(); got != "done-output" {
		t.Fatalf("Output() after final = %v, want %q", got, "done-output")
	}
	if p.Current() != "complete" {
		t.Fatalf("parent state = %q, want complete", p.Current())
	}
}

// TestWithSpawnInput_ReachesSpawnedActor pins WithSpawnInput: the input map
// declared on a dynamic Spawn must arrive verbatim at the spawned actor's
// ActorBehavior. We capture the input the behavior receives and assert each key
// round-trips. The child is then driven to completion as an end-to-end sanity
// check that the spawn produced a live, addressable actor.
func TestWithSpawnInput_ReachesSpawnedActor(t *testing.T) {
	cm := state.Forge[string, string, *childEntity]("inworker").
		State("working").
		State("done").Final().
		Initial("working").
		Transition("working").On("finish").GoTo("done").
		Quench()

	var received map[string]any
	behavior := func(input map[string]any) (state.ActorInstance, error) {
		received = input
		inst := cm.Cast(&childEntity{}, state.WithInitialState("working"))
		return state.NewActor(inst, nil), nil
	}

	m := state.Forge[string, string, *trec]("inspawner").
		State("idle").
		State("active").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("go").GoTo("active").
		Spawn("inworker", "worker-1",
			state.WithSpawnInput(map[string]any{"greeting": "hello", "count": float64(42)}),
			state.WithSpawnOnDone("workerDone"), state.WithSpawnOnError("workerErr")).
		Transition("active").On("workerDone").GoTo("idle").
		Quench()

	parent := m.Cast(&trec{}, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).Register("inworker", behavior)
	ctx := context.Background()

	res := parent.Fire(ctx, "go")
	sys.Absorb(ctx, res.Effects)

	if received == nil {
		t.Fatal("spawned ActorBehavior received nil input; WithSpawnInput did not reach the actor")
	}
	if got := received["greeting"]; got != "hello" {
		t.Fatalf("input[greeting] = %v, want %q", got, "hello")
	}
	if got := received["count"]; got != float64(42) {
		t.Fatalf("input[count] = %v, want %v", got, float64(42))
	}
	if len(received) != 2 {
		t.Fatalf("input has %d keys (%v), want exactly 2", len(received), received)
	}

	// The spawn produced a live, addressable actor under the explicit id.
	ref, ok := sys.Ref("worker-1")
	if !ok {
		t.Fatal("spawned actor worker-1 is not running/addressable")
	}
	if !sys.Deliver(ctx, ref, "finish") {
		t.Fatal("Deliver(finish) returned false; spawned actor not running")
	}
	if sys.Running() != 0 {
		t.Fatalf("running actors after completion = %d, want 0", sys.Running())
	}
}
