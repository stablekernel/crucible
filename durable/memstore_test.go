package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// recOf builds a Record at step with a single service-result journal entry and a
// single effect, so round-trips have non-trivial Entries/Effects to compare.
func recOf(step int, corr string) durable.Record {
	return durable.Record{
		Step: step,
		Entries: []state.JournalEntry{{
			Step:          step,
			Kind:          state.JournalServiceResult,
			CorrelationID: corr,
			Payload:       json.RawMessage(fmt.Sprintf(`{"v":%d}`, step)),
		}},
		Effects: []state.EffectEnvelope{{
			Kind:     "scheduleAfter",
			EffectID: fmt.Sprintf("%s#%d#scheduleAfter", corr, step),
		}},
	}
}

// marshaledSnapshot returns a real marshaled state.Snapshot so checkpoint
// round-trips exercise the actual serialization seam, not a synthetic byte blob.
func marshaledSnapshot(t *testing.T, current string) []byte {
	t.Helper()
	snap := state.Snapshot[string, string, map[string]any]{
		Machine:       "test-machine",
		Current:       current,
		Configuration: []string{current},
		Context:       map[string]any{"k": current},
		Status:        state.StatusRunning,
	}
	b, err := state.MarshalSnapshot(snap)
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	return b
}

func TestMemStore_AppendLoad_RoundTripsSnapshotAndJournal(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst-1")

	want := []durable.Record{recOf(0, "svc-a"), recOf(1, "svc-b"), recOf(2, "svc-c")}
	for _, rec := range want {
		if _, err := st.Append(ctx, id, rec); err != nil {
			t.Fatalf("Append(step %d): %v", rec.Step, err)
		}
	}

	snap, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if snap != nil {
		t.Fatalf("Load snapshot = %q, want nil (never checkpointed)", snap)
	}
	assertRecordsEqual(t, tail, want)
}

func TestMemStore_Append_PerStepIdempotency(t *testing.T) {
	tests := []struct {
		name    string
		appends []durable.Record
		wantLen int // distinct records persisted
	}{
		{
			name:    "double append same step is no-op",
			appends: []durable.Record{recOf(0, "a"), recOf(0, "a")},
			wantLen: 1,
		},
		{
			name:    "triple append same step is no-op",
			appends: []durable.Record{recOf(0, "a"), recOf(0, "a"), recOf(0, "a")},
			wantLen: 1,
		},
		{
			name:    "distinct steps all persist",
			appends: []durable.Record{recOf(0, "a"), recOf(1, "b")},
			wantLen: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := durable.NewMemStore()
			const id = durable.InstanceID("inst")

			var firstSeq int64
			for i, rec := range tt.appends {
				seq, err := st.Append(ctx, id, rec)
				if err != nil {
					t.Fatalf("Append %d: %v", i, err)
				}
				if i == 0 {
					firstSeq = seq
				}
			}

			// A re-append of step 0 must always return the original sequence.
			if tt.appends[len(tt.appends)-1].Step == tt.appends[0].Step {
				reSeq, err := st.Append(ctx, id, tt.appends[0])
				if err != nil {
					t.Fatalf("re-Append: %v", err)
				}
				if reSeq != firstSeq {
					t.Fatalf("idempotent re-append seq = %d, want original %d", reSeq, firstSeq)
				}
			}

			_, tail, err := st.Load(ctx, id)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if len(tail) != tt.wantLen {
				t.Fatalf("persisted records = %d, want %d", len(tail), tt.wantLen)
			}
		})
	}
}

func TestMemStore_Append_FirstWriterWins(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst")

	first := recOf(0, "original")
	if _, err := st.Append(ctx, id, first); err != nil {
		t.Fatalf("Append first: %v", err)
	}
	// A second append at the same step with different content must not overwrite.
	if _, err := st.Append(ctx, id, recOf(0, "usurper")); err != nil {
		t.Fatalf("Append second: %v", err)
	}

	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertRecordsEqual(t, tail, []durable.Record{first})
}

func TestMemStore_Append_WithIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst")

	// Same explicit key on two appends at the SAME step collapses to one.
	seq1, err := st.Append(ctx, id, recOf(0, "a"), durable.WithIdempotencyKey("k1"))
	if err != nil {
		t.Fatalf("Append k1: %v", err)
	}
	seq2, err := st.Append(ctx, id, recOf(0, "a"), durable.WithIdempotencyKey("k1"))
	if err != nil {
		t.Fatalf("Append k1 again: %v", err)
	}
	if seq1 != seq2 {
		t.Fatalf("same-key seqs differ: %d vs %d", seq1, seq2)
	}

	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 1 {
		t.Fatalf("records = %d, want 1", len(tail))
	}
}

func TestMemStore_Append_OutOfOrderRejected(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst")

	if _, err := st.Append(ctx, id, recOf(5, "a")); err != nil {
		t.Fatalf("Append step 5: %v", err)
	}
	_, err := st.Append(ctx, id, recOf(3, "b"))
	if !errors.Is(err, durable.ErrStepOutOfOrder) {
		t.Fatalf("Append step 3 err = %v, want ErrStepOutOfOrder", err)
	}
}

func TestMemStore_Load_UnknownInstance(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()

	snap, tail, err := st.Load(ctx, "nope")
	if !errors.Is(err, durable.ErrInstanceNotFound) {
		t.Fatalf("Load err = %v, want ErrInstanceNotFound", err)
	}
	if snap != nil || tail != nil {
		t.Fatalf("Load returned snap=%v tail=%v, want nils", snap, tail)
	}
}

func TestMemStore_Checkpoint_AdvancesSnapshotAndCompactsTail(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst")

	for i := range 4 {
		if _, err := st.Append(ctx, id, recOf(i, fmt.Sprintf("svc-%d", i))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	cp := marshaledSnapshot(t, "checkpointed")
	if err := st.Checkpoint(ctx, id, cp, 2); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	snap, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(snap) != string(cp) {
		t.Fatalf("Load snapshot mismatch:\n got %s\nwant %s", snap, cp)
	}
	// Only steps strictly greater than throughStep (2) remain in the tail.
	assertRecordsEqual(t, tail, []durable.Record{recOf(3, "svc-3")})

	// The checkpoint round-trips back into a real Snapshot.
	got, err := state.UnmarshalSnapshot[string, string, map[string]any](snap)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot: %v", err)
	}
	if got.Current != "checkpointed" {
		t.Fatalf("restored Current = %q, want checkpointed", got.Current)
	}
}

func TestMemStore_Checkpoint_NotAdvancingRejected(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst")

	if _, err := st.Append(ctx, id, recOf(0, "a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "c1"), 0); err != nil {
		t.Fatalf("Checkpoint 0: %v", err)
	}
	// Re-checkpointing at the same throughStep must be rejected.
	err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "c2"), 0)
	if !errors.Is(err, durable.ErrCheckpointNotAdvancing) {
		t.Fatalf("Checkpoint 0 again err = %v, want ErrCheckpointNotAdvancing", err)
	}
}

func TestMemStore_Checkpoint_AppendContinuesAfterCheckpoint(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst")

	for i := range 3 {
		if _, err := st.Append(ctx, id, recOf(i, "a")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "cp"), 2); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	// A post-checkpoint append at step 3 is accepted and ordered after the
	// checkpoint. A re-append of an already-recorded step (2, now compacted into
	// the checkpoint) stays an idempotent no-op — its dedup key survives the
	// checkpoint, so a crash-retry never re-applies or errors.
	if _, err := st.Append(ctx, id, recOf(3, "post")); err != nil {
		t.Fatalf("Append step 3: %v", err)
	}
	if _, err := st.Append(ctx, id, recOf(2, "a")); err != nil {
		t.Fatalf("idempotent re-append of compacted step 2: %v", err)
	}

	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertRecordsEqual(t, tail, []durable.Record{recOf(3, "post")})
}

func TestMemStore_JournalOrderingPreserved(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst")

	// A single record carrying several entries must preserve their relative order.
	rec := durable.Record{
		Step: 0,
		Entries: []state.JournalEntry{
			{Step: 0, Kind: state.JournalClockRead, ClockUnixNano: 100},
			{Step: 0, Kind: state.JournalServiceResult, CorrelationID: "svc", Payload: json.RawMessage(`1`)},
			{Step: 0, Kind: state.JournalRandom, Payload: json.RawMessage(`42`)},
		},
	}
	if _, err := st.Append(ctx, id, rec); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 1 || len(tail[0].Entries) != 3 {
		t.Fatalf("unexpected tail shape: %+v", tail)
	}
	wantKinds := []state.JournalKind{state.JournalClockRead, state.JournalServiceResult, state.JournalRandom}
	for i, e := range tail[0].Entries {
		if e.Kind != wantKinds[i] {
			t.Fatalf("entry %d kind = %q, want %q", i, e.Kind, wantKinds[i])
		}
	}
}

func TestMemStore_Load_IsolatedFromCallerMutation(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst")

	rec := recOf(0, "a")
	if _, err := st.Append(ctx, id, rec); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Mutating the caller's record after append must not affect stored state.
	rec.Entries[0].CorrelationID = "mutated"

	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if tail[0].Entries[0].CorrelationID != "a" {
		t.Fatalf("stored record was mutated by caller: %q", tail[0].Entries[0].CorrelationID)
	}
	// Mutating the loaded copy must not affect a subsequent Load either.
	tail[0].Entries[0].CorrelationID = "again"
	_, tail2, _ := st.Load(ctx, id)
	if tail2[0].Entries[0].CorrelationID != "a" {
		t.Fatalf("stored record was mutated via loaded copy: %q", tail2[0].Entries[0].CorrelationID)
	}
}

// TestMemStore_Checkpoint_CompactsTail verifies a Checkpoint compacts the journal
// through the checkpointed step, so Load returns only the post-checkpoint tail.
// History retention is a store-level capability (NewMemStore WithHistory), not a
// per-checkpoint flag.
func TestMemStore_Checkpoint_CompactsTail(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	const id = durable.InstanceID("inst")

	for i := range 3 {
		if _, err := st.Append(ctx, id, recOf(i, "a")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "cp"), 1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertRecordsEqual(t, tail, []durable.Record{recOf(2, "a")})
}

func TestMemStore_NewMemStore_WithInitialCapacity(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore(durable.WithInitialCapacity(8))
	const id = durable.InstanceID("inst")

	if _, err := st.Append(ctx, id, recOf(0, "a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 1 {
		t.Fatalf("records = %d, want 1", len(tail))
	}
}

// TestMemStore_Concurrent_RaceSafe drives concurrent appends (distinct instances
// and idempotent retries of the same step) plus concurrent loads and a
// checkpoint, so `go test -race` can detect any unsynchronized access.
func TestMemStore_Concurrent_RaceSafe(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()

	const instances = 8
	const steps = 50

	var wg sync.WaitGroup
	for inst := range instances {
		id := durable.InstanceID(fmt.Sprintf("inst-%d", inst))
		wg.Add(3)

		// Writer: append steps in order.
		go func() {
			defer wg.Done()
			for s := range steps {
				_, _ = st.Append(ctx, id, recOf(s, "svc"))
			}
		}()

		// Idempotent retrier: re-append step 0 repeatedly (must stay a no-op).
		go func() {
			defer wg.Done()
			for range steps {
				_, _ = st.Append(ctx, id, recOf(0, "svc"))
			}
		}()

		// Reader: concurrent loads.
		go func() {
			defer wg.Done()
			for range steps {
				_, _, _ = st.Load(ctx, id)
			}
		}()
	}
	wg.Wait()

	// After the dust settles, every instance holds exactly `steps` records.
	for inst := range instances {
		id := durable.InstanceID(fmt.Sprintf("inst-%d", inst))
		_, tail, err := st.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load %s: %v", id, err)
		}
		if len(tail) != steps {
			t.Fatalf("instance %s has %d records, want %d", id, len(tail), steps)
		}
		for i, rec := range tail {
			if rec.Step != i {
				t.Fatalf("instance %s record %d has step %d, want %d", id, i, rec.Step, i)
			}
		}
	}
}

// assertRecordsEqual compares two Record slices by value, marshaling each side
// to JSON so json.RawMessage payloads compare by content.
func assertRecordsEqual(t *testing.T, got, want []durable.Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("record count = %d, want %d\n got: %+v\nwant: %+v", len(got), len(want), got, want)
	}
	for i := range want {
		gb, err := json.Marshal(got[i])
		if err != nil {
			t.Fatalf("marshal got[%d]: %v", i, err)
		}
		wb, err := json.Marshal(want[i])
		if err != nil {
			t.Fatalf("marshal want[%d]: %v", i, err)
		}
		if string(gb) != string(wb) {
			t.Fatalf("record %d mismatch:\n got %s\nwant %s", i, gb, wb)
		}
	}
}

// staticStoreCheck asserts MemStore satisfies Store at compile time.
var _ durable.Store = (*durable.MemStore)(nil)
