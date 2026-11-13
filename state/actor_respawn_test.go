package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// startGreeter is an actor that sends its parent "hello" from its initial entry, then
// keeps running.
type startGreeter struct{ sent bool }

func (g *startGreeter) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (g *startGreeter) ChildEffects() []state.Effect {
	if g.sent {
		return nil
	}
	g.sent = true
	return []state.Effect{state.SendParent{Event: "hello"}}
}
func (g *startGreeter) Output() any { return nil }

// TestActorSystem_AbsorbRespawn_DropsStartMessages proves an actor AbsorbRespawn spawns
// runs, but the message it sends the parent as it starts doesn't reach the parent, as it
// does under Absorb, while a spawn that can't start still routes its onError.
func TestActorSystem_AbsorbRespawn_DropsStartMessages(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("respawn").
		Actor("greeter").
		Actor("broken").
		State("greeting").InvokeActor("greeter").InvokeActor("broken", state.WithInvokeOnError("brokenFailed")).
		State("hellod").
		State("failed").
		Initial("greeting").
		Transition("greeting").On("hello").GoTo("hellod").
		Transition("greeting").On("brokenFailed").GoTo("failed").
		Quench()
	behaviors := func(sys *state.ActorSystem[string, string, *trec], broken bool) {
		sys.Register("greeter", func(map[string]any) (state.ActorInstance, error) { return &startGreeter{}, nil })
		sys.Register("broken", func(map[string]any) (state.ActorInstance, error) {
			if broken {
				return nil, errors.New("can't start")
			}
			return &startGreeter{sent: true}, nil
		})
	}

	live := m.Cast(&trec{}, state.WithInitialState("greeting"))
	liveSys := state.NewActorSystem(live)
	behaviors(liveSys, false)
	liveSys.Absorb(ctx, live.StartEffects())
	if got := live.Current(); got != "hellod" {
		t.Fatalf("Absorb left the parent in %q, want hellod from the greeter's start", got)
	}

	resumed := m.Cast(&trec{}, state.WithInitialState("greeting"))
	var seen []state.ParentFire[string]
	sys := state.NewActorSystem(resumed).
		WithParentFireObserver(func(pf state.ParentFire[string]) { seen = append(seen, pf) })
	behaviors(sys, false)
	sys.AbsorbRespawn(ctx, resumed.StartEffects())
	if got := resumed.Current(); got != "greeting" {
		t.Fatalf("AbsorbRespawn left the parent in %q, want greeting: the start message should be dropped", got)
	}
	if len(seen) != 0 {
		t.Fatalf("observed %d parent Fires, want none", len(seen))
	}
	if !sys.IsRunning(state.ActorID("respawn", "greeting", 0)) {
		t.Fatal("AbsorbRespawn didn't spawn the greeter")
	}

	failing := m.Cast(&trec{}, state.WithInitialState("greeting"))
	failSys := state.NewActorSystem(failing)
	behaviors(failSys, true)
	failSys.AbsorbRespawn(ctx, []state.Effect{failing.StartEffects()[1]})
	if got := failing.Current(); got != "failed" {
		t.Fatalf("AbsorbRespawn of a spawn that can't start left the parent in %q, want failed by its onError", got)
	}
}
