package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

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
