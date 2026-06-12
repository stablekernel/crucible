package state_test

// This file pins that Snapshot/Restore preserves an instance's bounded-history
// retention. WithHistory(n) caps trace retention to the last n settled traces in
// a ring buffer; a restore that dropped the bound silently turned a bounded
// instance into one with no live retention (it froze on the snapshot traces) or an
// unbounded one. These tests lock the bound — and that retention keeps recording —
// across a snapshot round-trip, using a self-loop machine whose every event label
// is distinct so a frozen window is observably different from a live one.

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// hloopState is the single state of the history-loop machine.
type hloopState int

const hloopS hloopState = 0

// buildHistoryLoopMachine returns a one-state machine with a wildcard internal
// self-transition: it accepts ANY integer event and stays in place, so each Fire
// produces a settled trace whose Event label is the (distinct) integer fired. That
// distinctness lets a test tell a live bounded-history window (carrying the latest
// fire labels) apart from a frozen one (carrying the original snapshot labels).
func buildHistoryLoopMachine() *state.Machine[hloopState, int, any] {
	return state.Forge[hloopState, int, any]("historyLoop").
		State(hloopS).
		Transition(hloopS).OnAny().GoTo(hloopS).
		Initial(hloopS).
		CurrentStateFn(func(any) hloopState { return hloopS }).
		Quench()
}

// fireRange fires the half-open integer range [from, to) into inst, failing the
// test on the first error.
func fireRange(t *testing.T, inst *state.Instance[hloopState, int, any], from, to int) {
	t.Helper()
	ctx := context.Background()
	for ev := from; ev < to; ev++ {
		if res := inst.Fire(ctx, ev); res.Err != nil {
			t.Fatalf("Fire(%d) err = %v", ev, res.Err)
		}
	}
}

// TestSnapshotRestore_PreservesBoundedHistory casts a WithHistory(n) instance,
// fires past n so the ring wraps, snapshots and restores it, then fires more
// distinct events and asserts the restored instance still retains exactly n traces
// AND keeps recording — its window advances to the latest fires rather than
// freezing on the snapshot traces or growing unbounded.
func TestSnapshotRestore_PreservesBoundedHistory(t *testing.T) {
	const limit = 3
	m := buildHistoryLoopMachine()

	inst := m.Cast(nil,
		state.WithInitialState[hloopState](hloopS),
		state.WithHistory[hloopState](limit),
	)
	// Fire events 0..4 (5 > limit) so the ring wraps before the snapshot. The
	// retained window is now {2,3,4}.
	fireRange(t, inst, 0, 5)
	if got := len(inst.History()); got != limit {
		t.Fatalf("pre-snapshot History len = %d, want %d (ring should be full)", got, limit)
	}

	snap := inst.Snapshot()
	restored, err := m.Restore(snap)
	if err != nil {
		t.Fatalf("Restore err = %v", err)
	}

	if got := len(restored.History()); got != limit {
		t.Fatalf("post-restore History len = %d, want %d (retention bound dropped)", got, limit)
	}

	// Fire events 100..110 (10 > limit) into the restored instance. A live bounded
	// instance ends with window {107,108,109}; a frozen one (bound dropped) still
	// reports {2,3,4}; an unbounded one grows past limit.
	fireRange(t, restored, 100, 110)

	h := restored.History()
	if len(h) != limit {
		t.Fatalf("post-restore History len = %d after 10 fires, want %d (bound not preserved)", len(h), limit)
	}
	wantEvents := []string{"107", "108", "109"}
	for i, tr := range h {
		if tr.Event != wantEvents[i] {
			t.Fatalf("retained History events = %s, want %v (window did not advance — retention frozen)",
				historyEvents(h), wantEvents)
		}
	}
}

// TestSnapshotRestore_PreservesHistoryHeadOrder asserts the restored ring's head
// is preserved so a post-restore fire overwrites the OLDEST entry and the window
// stays in chronological order — not corrupted into an out-of-order or unbounded
// slice.
func TestSnapshotRestore_PreservesHistoryHeadOrder(t *testing.T) {
	const limit = 2
	m := buildHistoryLoopMachine()

	inst := m.Cast(nil,
		state.WithInitialState[hloopState](hloopS),
		state.WithHistory[hloopState](limit),
	)
	// Fire 0,1,2 to wrap the 2-entry ring once. Window: {1,2}.
	fireRange(t, inst, 0, 3)

	snap := inst.Snapshot()
	restored, err := m.Restore(snap)
	if err != nil {
		t.Fatalf("Restore err = %v", err)
	}

	// Fire event 3; the ring must drop its oldest entry (1) and keep {2,3} in order.
	fireRange(t, restored, 3, 4)

	h := restored.History()
	want := []string{"2", "3"}
	if len(h) != len(want) {
		t.Fatalf("post-restore History len = %d, want %d", len(h), len(want))
	}
	for i, tr := range h {
		if tr.Event != want[i] {
			t.Fatalf("post-restore History events = %s, want %v (ring head not preserved)",
				historyEvents(h), want)
		}
	}
}

// historyEvents renders the Event labels of a trace slice for failure messages.
func historyEvents(h []state.Trace) string {
	out := make([]string, len(h))
	for i, tr := range h {
		out[i] = tr.Event
	}
	return "[" + joinComma(out) + "]"
}

func joinComma(ss []string) string {
	s := ""
	for i, v := range ss {
		if i > 0 {
			s += ","
		}
		s += v
	}
	return s
}
