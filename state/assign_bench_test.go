package state_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestAssign_ValueSemanticsSnapshotRoundTrip asserts a value-C instance whose
// context was folded by an Assign snapshots and restores to the same context.
func TestAssign_ValueSemanticsSnapshotRoundTrip(t *testing.T) {
	m := state.ForgeFor[acct]("snaproundtrip").
		Reducer("credit", func(in state.AssignCtx[acct]) acct {
			c := in.Entity
			c.Balance += 250
			c.Notes = append(c.Notes, "credited")
			return c
		}).
		State("idle").
		State("active").
		Initial("idle").
		Transition("idle").On("go").GoTo("active").Assign("credit").
		Quench()

	inst := m.Cast(acct{Balance: 50}, state.WithInitialState[string]("idle"))
	if res := inst.Fire(context.Background(), "go"); res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}

	snap := inst.Snapshot()
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	var restoredSnap state.Snapshot[string, string, acct]
	if err = json.Unmarshal(raw, &restoredSnap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	restored, err := m.Restore(restoredSnap)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}

	got := restored.Entity()
	if got.Balance != 300 {
		t.Fatalf("restored balance = %d, want 300", got.Balance)
	}
	if len(got.Notes) != 1 || got.Notes[0] != "credited" {
		t.Fatalf("restored notes = %v, want [credited]", got.Notes)
	}
	if restored.Current() != "active" {
		t.Fatalf("restored state = %q, want active", restored.Current())
	}
}

// BenchmarkAssign_ContextCopyPerStep measures the per-step cost of the
// value-semantic context fold: a single transition Assign that copies and updates
// the context on every Fire. The number quantifies the copy cost the pointer
// escape hatch trades away; no optimization is applied (deltas are deferred).
func BenchmarkAssign_ContextCopyPerStep(b *testing.B) {
	m := state.ForgeFor[acct]("benchcopy").
		Reducer("bump", func(in state.AssignCtx[acct]) acct {
			c := in.Entity
			c.Balance++
			return c
		}).
		State("a").
		State("b").
		Initial("a").
		Transition("a").On("go").GoTo("b").Assign("bump").
		Transition("b").On("back").GoTo("a").Assign("bump").
		Quench()

	inst := m.Cast(acct{}, state.WithInitialState[string]("a"))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			inst.Fire(ctx, "go")
		} else {
			inst.Fire(ctx, "back")
		}
	}
}
