package durable_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// leavableTimerMachine is pendingTimerMachine with a "leave" from waiting back to idle,
// which cancels the timer.
func leavableTimerMachine() *state.Machine[string, string, *timerCtx] {
	return state.ForgeFor[*timerCtx]("leavableTimer").
		State("idle").
		State("waiting").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("arm").GoTo("waiting").
		Transition("waiting").On("poke").GoTo("waiting").
		Transition("waiting").On("leave").GoTo("idle").
		Transition("waiting").After(10 * time.Second).On("elapsed").GoTo("done").
		Quench()
}

// checkpointedTimer starts m in idle, arms its timer and pokes twice, so a checkpoint
// every 3 steps carries the pending timer, then fires each of then.
func checkpointedTimer(t *testing.T, m *state.Machine[string, string, *timerCtx], opts []durable.Option[string, string, *timerCtx], then ...string) (*durable.MemStore, durable.InstanceID) {
	t.Helper()
	ctx := context.Background()
	store, id := durable.NewMemStore(), durable.InstanceID("timer-1")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range append([]string{"arm", "poke", "poke"}, then...) {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%s): %v", ev, err)
		}
	}
	return store, id
}

// TestRecover_ATickThatFiresFewerTimersThanRecorded_Fails proves Recover and StateAt
// refuse a Record this version wrote whose tick replay can't reproduce: it records a
// fired timer, but nothing is due at its readings.
func TestRecover_ATickThatFiresFewerTimersThanRecorded_Fails(t *testing.T) {
	ctx := context.Background()
	m := leavableTimerMachine()
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch)),
		durable.WithCheckpointEvery[string, string, *timerCtx](3),
	}
	store, id := checkpointedTimer(t, m, opts, "leave")
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	last := tail[len(tail)-1]
	read := clockReadAt(11 * time.Second)
	bad := durable.Record{Step: last.Step + 1, Tick: true, TickSteps: 1, Format: last.Format, Entries: []state.JournalEntry{read, read, read}}
	if _, err = store.Append(ctx, id, bad); err != nil {
		t.Fatalf("Append: %v", err)
	}
	want := "fired 0 timer(s), but the journal records 1"
	if _, err = durable.Recover(ctx, m, store, id, opts...); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Recover: %v, want the tick's fired count refused", err)
	}
}

// TestRecover_AnOlderRecoverysRevivedTimer_Recovers proves an older journal whose tick
// fired a timer the tail had canceled still recovers, to the state the older version
// reached. That version re-armed a checkpoint's timers after the tail, so its recovery
// revived the canceled timer, and a later tick fired it into a refusal, recorded as one
// fired timer. Replay keeps it canceled and fires nothing, which changes no state.
func TestRecover_AnOlderRecoverysRevivedTimer_Recovers(t *testing.T) {
	ctx := context.Background()
	m := leavableTimerMachine()
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch)),
		durable.WithCheckpointEvery[string, string, *timerCtx](3),
	}
	store, id := checkpointedTimer(t, m, opts, "leave")
	older := asFormat(t, store, id, 2)
	_, tail, err := older.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	read := clockReadAt(11 * time.Second)
	revived := durable.Record{Step: tail[len(tail)-1].Step + 1, Tick: true, TickSteps: 1, Format: 2, Entries: []state.JournalEntry{read, read, read}}
	if _, err = older.Append(ctx, id, revived); err != nil {
		t.Fatalf("Append: %v", err)
	}
	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "idle" {
		t.Fatalf("recovered in %q, want idle: the revived timer's Fire was refused", got)
	}
}
