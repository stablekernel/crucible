package durable_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// starting is an actor that emits start from its initial entry, then keeps running.
type starting struct{ start []state.Effect }

func (a *starting) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (a *starting) ChildEffects() []state.Effect {
	out := a.start
	a.start = nil
	return out
}
func (a *starting) Output() any { return nil }

// TestRecover_ANestedChildsStartFireAppliesOnce proves a parent Fire that a running
// actor's start drives through a nested child it spawns, the onError of a nested spawn
// that can't start or the onDone of one the actor ends at once, is journaled live and
// applied exactly once across repeated Recovers: Recover spawns the running actor again,
// and its nested child fails or settles again, but that parent Fire is part of the
// actor's start, which the journal already holds.
func TestRecover_ANestedChildsStartFireAppliesOnce(t *testing.T) {
	ctx := context.Background()
	note := func(s string) func(state.AssignCtx[actorCtx]) actorCtx {
		return func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			c.Notes = append(c.Notes, s)
			return c
		}
	}
	m := state.ForgeFor[actorCtx]("nested").
		Reducer("noteNested", note("nested")).
		Reducer("notePoke", note("poke")).
		Actor("host").
		State("hosting").InvokeActor("host").
		Initial("hosting").
		Transition("hosting").On("nestedFailed").GoTo("hosting").Assign("noteNested").
		Transition("hosting").On("nestedDone").GoTo("hosting").Assign("noteNested").
		Transition("hosting").On("poke").GoTo("hosting").Assign("notePoke").
		Quench()
	starts := map[string][]state.Effect{
		"fails": {state.SpawnActor{ID: "child", Src: state.Ref{Name: "broken"}, OnError: "nestedFailed"}},
		"settles": {
			state.SpawnActor{ID: "child", Src: state.Ref{Name: "finisher"}, OnDone: "nestedDone"},
			state.SendTo{TargetID: "child", Event: "finish"},
		},
	}
	for _, how := range []string{"fails", "settles"} {
		t.Run(how, func(t *testing.T) {
			opts := []greetOption{durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
				"host": func(map[string]any) (state.ActorInstance, error) {
					return &starting{start: starts[how]}, nil
				},
				"broken":   failingSpawn(nil),
				"finisher": finishingChild(),
			})}
			store, id := durable.NewMemStore(), durable.InstanceID("nested")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("hosting"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke): %v", err)
			}
			want := fmt.Sprint([]string{"nested", "poke"})
			if got := fmt.Sprint(h.Instance().Entity().Notes); got != want {
				t.Fatalf("live notes = %s, want %s", got, want)
			}
			for round := 1; round <= 2; round++ {
				recovered, err := durable.Recover(ctx, m, store, id, opts...)
				if err != nil {
					t.Fatalf("Recover %d: %v", round, err)
				}
				if got := fmt.Sprint(recovered.Instance().Entity().Notes); got != want {
					t.Fatalf("Recover %d notes = %s, want %s: the nested child's parent Fire is part of the host's start", round, got, want)
				}
			}
		})
	}
}
