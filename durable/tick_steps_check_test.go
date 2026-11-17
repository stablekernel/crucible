package durable_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestRecover_ATickThatFiresFewerTimersThanRecorded_Fails proves Recover refuses a
// journal whose tick Record says it fired a timer replay can't fire. An older version's
// recovery could leave one: it missed a checkpointed timer that a tail tick fired, re-armed
// it after the walk, and a later live tick fired it again. Replay, which re-arms the
// timer before the tail, fires it at the first tick, so the second finds nothing due.
func TestRecover_ATickThatFiresFewerTimersThanRecorded_Fails(t *testing.T) {
	ctx := context.Background()
	m := pendingTimerMachine()
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](3),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("timer-1")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"arm", "poke", "poke"} { // the checkpoint carries the timer
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%s): %v", ev, err)
		}
	}
	clk.Advance(11 * time.Second)
	if fired, tickErr := h.Tick(ctx); tickErr != nil || len(fired) != 1 {
		t.Fatalf("Tick = %d fired, %v, want 1", len(fired), tickErr)
	}

	older := asFormatOne(t, store, id)
	_, tail, err := older.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	read := clockReadAt(12 * time.Second)
	again := durable.Record{Step: tail[len(tail)-1].Step + 1, Tick: true, TickSteps: 1, Format: 1, Entries: []state.JournalEntry{read, read, read}}
	if _, err = older.Append(ctx, id, again); err != nil {
		t.Fatalf("Append: %v", err)
	}

	_, err = durable.Recover(ctx, m, older, id, opts...)
	if err == nil || !strings.Contains(err.Error(), "fired 0 timers, but the journal records 1") {
		t.Fatalf("Recover: %v, want the tick's fired count refused", err)
	}
}
