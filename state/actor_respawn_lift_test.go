package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestActorSystem_AbsorbRespawn_ALiftedFireReenteringAStateStartsItsActorOnce pins
// what a lifted parent Fire does to an actor AbsorbRespawn hasn't respawned yet. outer's
// actor fails to start now, which it didn't before, so its onError is a new Fire and is
// heard; that Fire re-enters inner, whose greeter starts there as a new start, and its
// hello reaches the parent once. AbsorbRespawn's own spawn of the greeter then finds it
// running, and doesn't start it, or send its hello, a second time.
func TestActorSystem_AbsorbRespawn_ALiftedFireReenteringAStateStartsItsActorOnce(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("lift").
		Actor("broken").
		Actor("greeter").
		SuperState("outer").InvokeActor("broken", state.WithInvokeOnError("brokenFailed")).
		SubState("inner").InvokeActor("greeter").
		Initial("inner").
		EndSuperState().
		Initial("outer").
		Transition("inner").On("brokenFailed").GoTo("inner").Reenter().
		Transition("inner").On("hello").GoTo("inner").
		Quench()
	inst := m.Cast(&trec{}, state.WithInitialState("inner"))
	var fired []string
	sys := state.NewActorSystem(inst).
		WithParentFireObserver(func(pf state.ParentFire[string]) { fired = append(fired, pf.Result.Trace.Event) })
	sys.Register("broken", func(map[string]any) (state.ActorInstance, error) { return nil, errors.New("can't start now") })
	starts := 0
	sys.Register("greeter", func(map[string]any) (state.ActorInstance, error) {
		starts++
		return &startGreeter{}, nil
	})
	sys.AbsorbRespawn(ctx, inst.StartEffects())
	hellos := 0
	for _, ev := range fired {
		if ev == "hello" {
			hellos++
		}
	}
	if starts != 1 || hellos != 1 {
		t.Fatalf("greeter started %d times and its hello reached the parent %d times (parent Fires %q), want once each", starts, hellos, fired)
	}
}

// TestActorSystem_AbsorbRespawn_DoesNotMuteAnotherActorsDelivery pins that the quiet of
// an AbsorbRespawn covers only the actors it spawns: a delivery to another actor that
// was already running, made while AbsorbRespawn is under way (as another goroutine
// can), reaches the parent with the message it drives.
func TestActorSystem_AbsorbRespawn_DoesNotMuteAnotherActorsDelivery(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("mute").
		Actor("pinger").
		Actor("late").
		State("waiting").InvokeActor("pinger").InvokeActor("late").
		State("hellod").
		Initial("waiting").
		Transition("waiting").On("hello").GoTo("hellod").
		Quench()
	inst := m.Cast(&trec{}, state.WithInitialState("waiting"))
	sys := state.NewActorSystem(inst)
	sys.Register("pinger", func(map[string]any) (state.ActorInstance, error) { return &pinger{}, nil })
	pingerID := state.ActorID("mute", "waiting", 0)
	sys.Register("late", func(map[string]any) (state.ActorInstance, error) {
		// Another goroutine's delivery, landing while AbsorbRespawn runs.
		sys.DeliverByID(ctx, pingerID, "ping")
		return &startGreeter{sent: true}, nil
	})
	effects := inst.StartEffects()
	// The pinger was already running, started as usual.
	sys.Absorb(ctx, effects[:1])
	sys.AbsorbRespawn(ctx, effects[1:])
	if got := inst.Current(); got != "hellod" {
		t.Fatalf("the pinger's hello, delivered during AbsorbRespawn, left the parent in %q, want hellod", got)
	}
}
