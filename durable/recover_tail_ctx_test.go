package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestRecover_FailsWhenItsContextEndsDuringTheLastRecord pins that Recover fails when its
// context ends while it replays the journal's last Record, here a Tick that fires two
// timers whose first transition ends the context, so the kernel refuses the second: the
// recovered instance would otherwise be short of the journal with no error.
func TestRecover_FailsWhenItsContextEndsDuringTheLastRecord(t *testing.T) {
	ctx := context.Background()
	var cancel context.CancelFunc
	m := state.ForgeFor[stepCtx]("tailCtx").
		Action("end", func(state.ActionCtx[stepCtx]) (state.Effect, error) {
			if cancel != nil {
				cancel()
			}
			return nil, nil
		}).
		State("idle").
		SuperState("both").
		Region("left").
		Initial("l1").
		SubState("l1").
		SubState("l2").
		EndRegion().
		Region("right").
		Initial("r1").
		SubState("r1").
		SubState("r2").
		EndRegion().
		EndSuperState().
		Initial("idle").
		Transition("idle").On("go").GoTo("both").
		Transition("l1").After(time.Second).On("toL2").GoTo("l2").Do("end").
		Transition("r1").After(time.Second).On("toR2").GoTo("r2").
		Quench()
	clk := state.NewFakeClock(epoch)
	opts := []stepOption{durable.WithRunnerClock[string, string, stepCtx](clk)}
	store, id := durable.NewMemStore(), durable.InstanceID("tail-ctx")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	clk.Advance(2 * time.Second)
	if fired, tickErr := h.Tick(ctx); tickErr != nil || len(fired) != 2 {
		t.Fatalf("Tick fired %d, %v; want both timers", len(fired), tickErr)
	}
	turn, c := context.WithCancel(ctx)
	cancel = c
	recovered, err := durable.Recover(turn, m, store, id, opts...)
	if !errors.Is(err, context.Canceled) {
		got := "no Handle"
		if recovered != nil {
			got = recovered.Instance().Snapshot().Current
		}
		t.Fatalf("Recover = %v (state %v), want context.Canceled: the last Record's second timer was refused", err, got)
	}
}
