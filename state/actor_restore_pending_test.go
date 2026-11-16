package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestRestoreActors_DoesNotRespawnANestedActorThatSettled pins that an actor restored
// from a snapshot doesn't spawn a nested actor its earlier start spawned, once that
// nested actor settled and moved the actor on: the restored actor resumes, so the
// spawn its fresh Cast buffered is stale.
func TestRestoreActors_DoesNotRespawnANestedActorThatSettled(t *testing.T) {
	for _, compound := range []bool{false} {
		ctx := context.Background()
		nestedRuns := 0
		cb := state.ForgeFor[*childEntity]("child").Actor("nested")
		if compound {
			cb = cb.SuperState("p").InvokeActor("nested", state.WithInvokeOnDone("nestedDone"), state.WithInvokeID("nested")).
				SubState("a").
				SubState("b").
				Initial("a").
				EndSuperState().
				Initial("p")
		} else {
			cb = cb.State("a").InvokeActor("nested", state.WithInvokeOnDone("nestedDone"), state.WithInvokeID("nested")).
				State("b").
				Initial("a")
		}
		child := cb.Transition("a").On("nestedDone").GoTo("b").
			Transition("b").On("poke").GoTo("b").
			Quench()
		parent := state.ForgeFor[*trec]("parent").
			Actor("child").
			State("supervising").InvokeActor("child", state.WithInvokeID("child")).
			Initial("supervising").
			Quench()
		register := func(sys *state.ActorSystem[string, string, *trec]) {
			sys.Register("child", func(map[string]any) (state.ActorInstance, error) {
				return state.NewActor(child.Cast(&childEntity{}, state.WithInitialState("a")), nil), nil
			})
			sys.Register("nested", func(in map[string]any) (state.ActorInstance, error) {
				nestedRuns++
				return childBehavior()(in)
			})
		}
		inst := parent.Cast(&trec{}, state.WithInitialState("supervising"))
		sys := state.NewActorSystem(inst)
		register(sys)
		sys.Absorb(ctx, inst.StartEffects())
		if nestedRuns != 1 {
			t.Fatalf("compound=%v: nested ran %d times starting, want once", compound, nestedRuns)
		}
		sys.DeliverByID(ctx, "nested", "finish")
		snaps, err := sys.SnapshotActors()
		if err != nil {
			t.Fatalf("SnapshotActors: %v", err)
		}
		inst2 := parent.Cast(&trec{}, state.WithInitialState("supervising"))
		sys2 := state.NewActorSystem(inst2)
		register(sys2)
		if err = sys2.RestoreActors(ctx, snaps); err != nil {
			t.Fatalf("RestoreActors: %v", err)
		}
		sys2.DeliverByID(ctx, "child", "poke")
		if nestedRuns != 1 {
			t.Fatalf("compound=%v: nested ran %d times, want once: the restored child spawned it again", compound, nestedRuns)
		}
	}
}
