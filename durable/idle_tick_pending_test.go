package durable_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestIdleTick_KeepsAPendingParentFiresRead proves an idle Tick, the first step after
// Start or Recover, keeps the clock read a pending parent Fire took arming its timer: the
// goer's go, sent as it spawned, which the next real step journals. A Recover after that
// step replays the go's arming at its read, so the timer is due 10s after it.
func TestIdleTick_KeepsAPendingParentFiresRead(t *testing.T) {
	ctx := context.Background()
	for _, from := range []string{"Start", "Recover"} {
		t.Run(from, func(t *testing.T) {
			m := armedMachine()
			clk := state.NewFakeClock(epoch)
			store, id := durable.NewMemStore(), durable.InstanceID("pending")
			h, err := durable.NewRunner(m, store, armedOptions(clk, from == "Start")...).Start(ctx, id, actorCtx{}, state.WithInitialState("going"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if from == "Recover" {
				clk.Advance(time.Hour)
				if h, err = durable.Recover(ctx, m, store, id, armedOptions(clk, true)...); err != nil {
					t.Fatalf("Recover: %v", err)
				}
			}
			if got := h.Instance().Current(); got != "armed" {
				t.Fatalf("state %q, want armed by the goer's go", got)
			}
			fired, err := h.Tick(ctx)
			if err != nil || len(fired) != 0 {
				t.Fatalf("idle Tick = %d fired, %v, want none", len(fired), err)
			}
			if _, err = h.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke): %v", err)
			}
			clk.Advance(5 * time.Second)
			again, err := durable.Recover(ctx, m, store, id, armedOptions(clk, true)...)
			if err != nil {
				t.Fatalf("second Recover: %v", err)
			}
			clk.Advance(5 * time.Second)
			if _, err = again.Tick(ctx); err != nil {
				t.Fatalf("Tick: %v", err)
			}
			if got := again.Instance().Current(); got != "timedOut" {
				t.Fatalf("state = %q, want timedOut 10s after the go's read", got)
			}
		})
	}
}
