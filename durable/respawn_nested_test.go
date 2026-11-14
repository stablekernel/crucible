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

// relay is an actor that sends its parent "pong" each time it's delivered "ping", and
// keeps running.
type relay struct{ out []state.Effect }

func (r *relay) DeliverFire(_ context.Context, ev any) (bool, any) {
	if ev == "ping" {
		r.out = append(r.out, state.SendParent{Event: "pong"})
	}
	return false, nil
}

func (r *relay) ChildEffects() []state.Effect {
	out := r.out
	r.out = nil
	return out
}
func (r *relay) Output() any { return nil }

// TestRecover_DropsAPongLiveNeverSent proves Recover's respawn of the running actors,
// which follows the order the configuration declares them in, not the order they
// spawned live, can't apply a parent Fire live never did. Live, the pinger in the right
// region spawned first and pinged the relay as it started, but the relay, in the left
// region, wasn't running yet, so the ping went nowhere; the relay spawned later. Recover
// spawns the relay first, so the pinger's ping reaches it, and its pong is part of a
// running actor's start, which the parent doesn't hear.
func TestRecover_DropsAPongLiveNeverSent(t *testing.T) {
	ctx := context.Background()
	note := func(s string) func(state.AssignCtx[actorCtx]) actorCtx {
		return func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			c.Notes = append(c.Notes, s)
			return c
		}
	}
	m := state.ForgeFor[actorCtx]("order").
		Reducer("notePong", note("pong")).
		Actor("relay").
		Actor("pinger").
		SuperState("par").
		Region("left").
		Initial("l1").
		SubState("l1").
		SubState("l2").InvokeActor("relay", state.WithInvokeID("relay")).
		EndRegion().
		Region("right").
		Initial("r1").
		SubState("r1").InvokeActor("pinger").
		EndRegion().
		EndSuperState().
		Initial("par").
		Transition("l1").On("go").GoTo("l2").
		Transition("par").On("pong").GoTo("par").Assign("notePong").
		Quench()
	opts := []greetOption{durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
		"relay": func(map[string]any) (state.ActorInstance, error) { return &relay{}, nil },
		"pinger": func(map[string]any) (state.ActorInstance, error) {
			return &starting{start: []state.Effect{state.SendTo{TargetID: "relay", Event: "ping"}}}, nil
		},
	})}
	store, id := durable.NewMemStore(), durable.InstanceID("order")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("par"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	if notes := h.Instance().Entity().Notes; len(notes) != 0 {
		t.Fatalf("live notes = %v, want none: the relay wasn't running when the pinger pinged", notes)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if notes := recovered.Instance().Entity().Notes; len(notes) != 0 {
		t.Fatalf("recovered notes = %v, want none: live never heard a pong", notes)
	}
}
