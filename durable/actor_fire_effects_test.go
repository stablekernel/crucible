package durable_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestFire_DispatchesTheEffectsOfAParentFireAnActorDrove pins that a domain effect a
// parent Fire emits, where an actor drove that Fire, is dispatched like the event
// Fire's own: here a failed spawn's onError transition emits a notify.
func TestFire_DispatchesTheEffectsOfAParentFireAnActorDrove(t *testing.T) {
	t.Skip("a parent Fire an actor drove has its timers and services armed, but its domain effects aren't dispatched; #235 fixes it")
	ctx := context.Background()
	type cx struct{}
	m := state.ForgeFor[cx]("actorEffects").
		Action("notify", func(state.ActionCtx[cx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		Actor("child").
		State("idle").
		State("spawning").InvokeActor("child", state.WithInvokeOnError("spawnFailed")).
		State("failed").
		Initial("idle").
		Transition("idle").On("start").GoTo("spawning").
		Transition("spawning").On("spawnFailed").GoTo("failed").Do("notify").
		Quench()
	rec := newEffectRecorder()
	h, err := durable.NewRunner(m, durable.NewMemStore(),
		durable.WithEffectHandler[string, string, cx](rec.handler()),
		durable.WithActorPalette[string, string, cx](map[string]state.ActorBehavior{"child": failingSpawn(nil)}),
	).Start(ctx, "actor-effects", cx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	if got := h.Instance().Current(); got != "failed" {
		t.Fatalf("state = %q, want failed by the onError", got)
	}
	if got := len(rec.values); got != 1 {
		t.Fatalf("dispatched %d effects, want the onError transition's notify", got)
	}
}
