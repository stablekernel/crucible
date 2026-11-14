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

// starter is an actor that sends its first effects from its initial entry, then keeps
// running; finish ends it with no output.
type starter struct{ start []state.Effect }

func (a *starter) DeliverFire(_ context.Context, ev any) (bool, any) { return ev == "finish", nil }
func (a *starter) ChildEffects() []state.Effect {
	out := a.start
	a.start = nil
	return out
}
func (a *starter) Output() any { return nil }

// TestActorSystem_AbsorbRespawn_ParentFiresItDrivesAreAudible proves the quiet of an
// AbsorbRespawn spawn covers only the spawned actor's own start: the onError of a spawn
// that can't start, and the onDone of an actor a respawned one ends as it starts, are
// parent Fires the earlier start didn't send, so the greeter their state spawns reaches
// the parent with its hello, as under Absorb.
func TestActorSystem_AbsorbRespawn_ParentFiresItDrivesAreAudible(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("audible").
		Actor("greeter").
		Actor("broken").
		Actor("finisher").
		Actor("poker").
		State("failing").InvokeActor("broken", state.WithInvokeOnError("brokenFailed")).
		State("pair").InvokeActor("finisher", state.WithInvokeOnDone("finished")).InvokeActor("poker").
		State("greeting").InvokeActor("greeter").
		State("hellod").
		Initial("failing").
		Transition("failing").On("brokenFailed").GoTo("greeting").
		Transition("pair").On("finished").GoTo("greeting").
		Transition("greeting").On("hello").GoTo("hellod").
		Quench()
	finisher := state.ActorID("audible", "pair", 0)
	for _, from := range []string{"failing", "pair"} {
		t.Run(from, func(t *testing.T) {
			inst := m.Cast(&trec{}, state.WithInitialState(from))
			sys := state.NewActorSystem(inst)
			sys.Register("greeter", func(map[string]any) (state.ActorInstance, error) { return &startGreeter{}, nil })
			sys.Register("broken", func(map[string]any) (state.ActorInstance, error) { return nil, errors.New("can't start") })
			sys.Register("finisher", func(map[string]any) (state.ActorInstance, error) { return &starter{}, nil })
			sys.Register("poker", func(map[string]any) (state.ActorInstance, error) {
				return &starter{start: []state.Effect{state.SendTo{TargetID: finisher, Event: "finish"}}}, nil
			})
			sys.AbsorbRespawn(ctx, inst.StartEffects())
			if got := inst.Current(); got != "hellod" {
				t.Fatalf("AbsorbRespawn from %s left the parent in %q, want hellod: the greeter its onError or onDone spawned should reach the parent", from, got)
			}
		})
	}
}
