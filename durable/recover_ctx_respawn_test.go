package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestRecover_AContextThatEndsWhileARespawnRunsWithNoFireRefused pins that Recover, like
// Start, doesn't fail when its context ends while it spawns an actor and no parent Fire
// was refused: the actor's behavior already ran, and failing would make the caller's
// retry run it again.
func TestRecover_AContextThatEndsWhileARespawnRunsWithNoFireRefused(t *testing.T) {
	ctx := context.Background()
	m := spawnRuleMachine(false, nil)
	store, id := durable.NewMemStore(), durable.InstanceID("recover-ctx")
	calls := 0
	var cancel context.CancelFunc
	opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
		"child": func(in map[string]any) (state.ActorInstance, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("the child can't start yet")
			}
			cancel() // the deadline passes as the retried spawn runs
			return finishingChild()(in)
		},
	})}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	turn, c := context.WithCancel(ctx)
	cancel = c
	if _, err = durable.Recover(turn, m, store, id, opts...); err != nil {
		t.Fatalf("Recover = %v after the retried spawn ran (behavior ran %d times); want it to succeed, as Start does, so a retry doesn't run the behavior again", err, calls)
	}
}
