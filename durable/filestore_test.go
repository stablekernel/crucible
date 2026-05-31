package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// newFileStore builds a FileStore over a fresh temp directory cleaned up after
// the test, with optional construction options.
func newFileStore(t *testing.T, opts ...durable.FileStoreOption) *durable.FileStore {
	t.Helper()
	st, err := durable.NewFileStore(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	return st
}

func TestFileStore_AppendLoad_RoundTripsSnapshotAndJournal(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
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

func TestFileStore_Append_PerStepIdempotency(t *testing.T) {
	tests := []struct {
		name    string
		appends []durable.Record
		wantLen int
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
			st := newFileStore(t)
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

func TestFileStore_Append_FirstWriterWins(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
	const id = durable.InstanceID("inst")

	first := recOf(0, "original")
	if _, err := st.Append(ctx, id, first); err != nil {
		t.Fatalf("Append first: %v", err)
	}
	if _, err := st.Append(ctx, id, recOf(0, "usurper")); err != nil {
		t.Fatalf("Append second: %v", err)
	}

	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertRecordsEqual(t, tail, []durable.Record{first})
}

func TestFileStore_Append_WithIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
	const id = durable.InstanceID("inst")

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

func TestFileStore_Append_OutOfOrderRejected(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
	const id = durable.InstanceID("inst")

	if _, err := st.Append(ctx, id, recOf(5, "a")); err != nil {
		t.Fatalf("Append step 5: %v", err)
	}
	_, err := st.Append(ctx, id, recOf(3, "b"))
	if !errors.Is(err, durable.ErrStepOutOfOrder) {
		t.Fatalf("Append step 3 err = %v, want ErrStepOutOfOrder", err)
	}
}

func TestFileStore_Load_UnknownInstance(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)

	snap, tail, err := st.Load(ctx, "nope")
	if !errors.Is(err, durable.ErrInstanceNotFound) {
		t.Fatalf("Load err = %v, want ErrInstanceNotFound", err)
	}
	if snap != nil || tail != nil {
		t.Fatalf("Load returned snap=%v tail=%v, want nils", snap, tail)
	}
}

func TestFileStore_Checkpoint_AdvancesSnapshotAndCompactsTail(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
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
	assertRecordsEqual(t, tail, []durable.Record{recOf(3, "svc-3")})

	got, err := state.UnmarshalSnapshot[string, string, map[string]any](snap)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot: %v", err)
	}
	if got.Current != "checkpointed" {
		t.Fatalf("restored Current = %q, want checkpointed", got.Current)
	}
}

func TestFileStore_Checkpoint_NotAdvancingRejected(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
	const id = durable.InstanceID("inst")

	if _, err := st.Append(ctx, id, recOf(0, "a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "c1"), 0); err != nil {
		t.Fatalf("Checkpoint 0: %v", err)
	}
	err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "c2"), 0)
	if !errors.Is(err, durable.ErrCheckpointNotAdvancing) {
		t.Fatalf("Checkpoint 0 again err = %v, want ErrCheckpointNotAdvancing", err)
	}
}

func TestFileStore_Checkpoint_AppendContinuesAfterCheckpoint(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
	const id = durable.InstanceID("inst")

	for i := range 3 {
		if _, err := st.Append(ctx, id, recOf(i, "a")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "cp"), 2); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if _, err := st.Append(ctx, id, recOf(3, "post")); err != nil {
		t.Fatalf("Append step 3: %v", err)
	}
	// A re-append of an already-recorded step compacted into the checkpoint stays
	// an idempotent no-op: its dedup key survives compaction.
	if _, err := st.Append(ctx, id, recOf(2, "a")); err != nil {
		t.Fatalf("idempotent re-append of compacted step 2: %v", err)
	}

	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertRecordsEqual(t, tail, []durable.Record{recOf(3, "post")})
}

func TestFileStore_JournalOrderingPreserved(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
	const id = durable.InstanceID("inst")

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

func TestFileStore_Checkpoint_RetainTail(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
	const id = durable.InstanceID("inst")

	for i := range 3 {
		if _, err := st.Append(ctx, id, recOf(i, "a")); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "cp"), 1, durable.WithRetainTail()); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	_, tail, err := st.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	assertRecordsEqual(t, tail, []durable.Record{recOf(2, "a")})
}

// TestFileStore_Dispatched_RoundTrip exercises the DispatchStore seam: marking
// effect ids and reading the membership set back, idempotently.
func TestFileStore_Dispatched_RoundTrip(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
	const id = durable.InstanceID("inst")

	// An instance with nothing dispatched reports an empty (non-nil) set.
	got, err := st.Dispatched(ctx, id)
	if err != nil {
		t.Fatalf("Dispatched (empty): %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("empty Dispatched = %v, want empty non-nil map", got)
	}

	if err = st.MarkDispatched(ctx, id, "e1", "e2"); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}
	// Re-marking an already-marked id is a no-op.
	if err = st.MarkDispatched(ctx, id, "e1"); err != nil {
		t.Fatalf("re-MarkDispatched: %v", err)
	}
	if err = st.MarkDispatched(ctx, id); err != nil {
		t.Fatalf("MarkDispatched empty: %v", err)
	}

	got, err = st.Dispatched(ctx, id)
	if err != nil {
		t.Fatalf("Dispatched: %v", err)
	}
	if !got["e1"] || !got["e2"] || len(got) != 2 {
		t.Fatalf("Dispatched = %v, want {e1,e2}", got)
	}
}

// TestFileStore_StaticInterfaces asserts FileStore satisfies both seams.
func TestFileStore_StaticInterfaces(t *testing.T) {
	var _ durable.Store = (*durable.FileStore)(nil)
	var _ durable.DispatchStore = (*durable.FileStore)(nil)
}

// TestNewFileStore_EmptyDirRejected guards the constructor's required-argument
// validation.
func TestNewFileStore_EmptyDirRejected(t *testing.T) {
	if _, err := durable.NewFileStore(""); err == nil {
		t.Fatal("NewFileStore(\"\") = nil error, want an error")
	}
}

// TestFileStore_UnsafeInstanceID round-trips an InstanceID containing path
// separators and non-safe bytes through both an Append and a fresh-store reopen,
// proving arbitrary ids map to a single collision-free on-disk directory and
// reconstruct intact.
func TestFileStore_UnsafeInstanceID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// Slashes, spaces, and a leading underscore all force per-byte escaping.
	ids := []durable.InstanceID{
		"tenant/abc-123",
		"with space",
		"_leading-underscore",
		"unsafe\x00byte",
		"emoji-🚀",
	}

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for _, id := range ids {
		if _, err = st.Append(ctx, id, recOf(0, "a")); err != nil {
			t.Fatalf("Append %q: %v", id, err)
		}
	}

	// A fresh store over the same dir reconstructs each escaped instance.
	st2, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore reopen: %v", err)
	}
	for _, id := range ids {
		_, tail, err := st2.Load(ctx, id)
		if err != nil {
			t.Fatalf("reopen Load %q: %v", id, err)
		}
		assertRecordsEqual(t, tail, []durable.Record{recOf(0, "a")})
	}
}

// TestFileStore_NewFileStore_WithNoOptions confirms construction with an explicit
// (currently no-op) option resolves to a ready store.
func TestFileStore_NewFileStore_WithNoOptions(t *testing.T) {
	if _, err := durable.NewFileStore(t.TempDir()); err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
}
