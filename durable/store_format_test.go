package durable_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// formatDroppingStore is a Store that keeps every Record field but Format, as a
// custom backend with a schema from before that field would.
type formatDroppingStore struct{ *durable.MemStore }

func (s formatDroppingStore) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	rec.Format = 0
	return s.MemStore.Append(ctx, id, rec, opts...)
}

// TestRecover_FromAStoreThatDropsFormat pins that Recover fails when a Store loses
// Record.Format, rather than replay the Records as an older format's and silently
// skip the timer an actor-driven Fire armed live.
func TestRecover_FromAStoreThatDropsFormat(t *testing.T) {
	ctx := context.Background()
	m := armedMachine()
	clk := state.NewFakeClock(epoch)
	opts := armedOptions(clk, true)
	store, id := formatDroppingStore{durable.NewMemStore()}, durable.InstanceID("armed")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	if found, deliverErr := h.DeliverToActor(ctx, state.ActorRef{ID: state.ActorID("armed", "working", 0)}, "finish"); !found || deliverErr != nil {
		t.Fatalf("DeliverToActor = %v, %v", found, deliverErr)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	clk.Advance(10 * time.Second)
	if _, err := durable.Recover(ctx, m, store, id, opts...); err == nil {
		t.Fatal("Recover from a Store that dropped Format succeeded; want it to fail rather than recover without the timer the live run armed")
	}
}
