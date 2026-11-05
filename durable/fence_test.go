package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// storeBackends runs a contract test against each in-tree Store.
func storeBackends(t *testing.T, test func(t *testing.T, st durable.Store)) {
	t.Helper()
	t.Run("MemStore", func(t *testing.T) { test(t, durable.NewMemStore()) })
	t.Run("FileStore", func(t *testing.T) { test(t, newFileStore(t)) })
}

func tailLen(t *testing.T, st durable.Store, id durable.InstanceID) int {
	t.Helper()
	_, tail, err := st.Load(context.Background(), id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return len(tail)
}

func TestStore_Fence_LowerTokenWritesNothing(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		if _, err := st.Append(ctx, id, recOf(0, "a"), durable.WithFence(2)); err != nil {
			t.Fatalf("Append(2): %v", err)
		}
		if _, err := st.Append(ctx, id, recOf(1, "b"), durable.WithFence(1)); !errors.Is(err, durable.ErrFenced) {
			t.Fatalf("Append(1): err = %v, want ErrFenced", err)
		}
		// The fence comes first: even a retry of a recorded step is refused.
		if _, err := st.Append(ctx, id, recOf(0, "a"), durable.WithFence(1)); !errors.Is(err, durable.ErrFenced) {
			t.Fatalf("retry at token 1: err = %v, want ErrFenced", err)
		}
		if err := st.Checkpoint(ctx, id, []byte("snap"), 0, durable.WithCheckpointFence(1)); !errors.Is(err, durable.ErrFenced) {
			t.Fatalf("Checkpoint(1): err = %v, want ErrFenced", err)
		}
		snap, tail, err := st.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if snap != nil || len(tail) != 1 {
			t.Fatalf("fenced writes changed the store: snapshot %q, %d records", snap, len(tail))
		}
	})
}

func TestStore_Fence_EqualTokenPasses(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		seq, err := st.Append(ctx, id, recOf(0, "a"), durable.WithFence(2))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		again, err := st.Append(ctx, id, recOf(0, "a"), durable.WithFence(2))
		if err != nil {
			t.Fatalf("retry: %v", err)
		}
		if again != seq {
			t.Fatalf("retry seq = %d, want %d", again, seq)
		}
		if _, err = st.Append(ctx, id, recOf(1, "b"), durable.WithFence(2)); err != nil {
			t.Fatalf("next step: %v", err)
		}
		if err = st.Checkpoint(ctx, id, []byte("snap"), 1, durable.WithCheckpointFence(2)); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
	})
}

func TestStore_Fence_CheckpointRaisesIt(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		if err := st.Checkpoint(ctx, id, []byte("snap"), -1, durable.WithCheckpointFence(3)); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if _, err := st.Append(ctx, id, recOf(0, "a"), durable.WithFence(2)); !errors.Is(err, durable.ErrFenced) {
			t.Fatalf("Append(2): err = %v, want ErrFenced", err)
		}
	})
}

func TestStore_Fence_NoTokenCountsAsZero(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		if _, err := st.Append(ctx, id, recOf(0, "a")); err != nil {
			t.Fatalf("unfenced Append on an unfenced instance: %v", err)
		}
		if _, err := st.Append(ctx, id, recOf(1, "b"), durable.WithFence(1)); err != nil {
			t.Fatalf("Append(1): %v", err)
		}
		if _, err := st.Append(ctx, id, recOf(2, "c")); !errors.Is(err, durable.ErrFenced) {
			t.Fatalf("unfenced Append on a fenced instance: err = %v, want ErrFenced", err)
		}
	})
}

func TestStore_Append_SameRecordIsANoOpAfterCompaction(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		seq, err := st.Append(ctx, id, recOf(0, "a"))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err = st.Checkpoint(ctx, id, []byte("snap"), 0); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		again, err := st.Append(ctx, id, recOf(0, "a"))
		if err != nil {
			t.Fatalf("retry after compaction: %v", err)
		}
		if again != seq {
			t.Fatalf("retry seq = %d, want %d", again, seq)
		}
	})
}

func TestStore_Append_DifferentRecordConflictsAfterCompaction(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		if _, err := st.Append(ctx, id, recOf(0, "a")); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if err := st.Checkpoint(ctx, id, []byte("snap"), 0); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if _, err := st.Append(ctx, id, recOf(0, "b")); !errors.Is(err, durable.ErrStepConflict) {
			t.Fatalf("err = %v, want ErrStepConflict", err)
		}
	})
}

func TestStore_Append_EmptyAndNilFieldsAreTheSameRecord(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		if _, err := st.Append(ctx, id, durable.Record{Step: 0}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		same := durable.Record{Step: 0, Event: []byte{}, Snapshot: []byte{}, Entries: []state.JournalEntry{}}
		if _, err := st.Append(ctx, id, same); err != nil {
			t.Fatalf("retry: %v", err)
		}
	})
}

func TestStore_Append_ExplicitKeyVouchesForTheRecord(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		if _, err := st.Append(ctx, id, recOf(0, "a"), durable.WithIdempotencyKey("k")); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if _, err := st.Append(ctx, id, recOf(0, "b"), durable.WithIdempotencyKey("k")); err != nil {
			t.Fatalf("same key, different record: %v", err)
		}
		if n := tailLen(t, st, id); n != 1 {
			t.Fatalf("records = %d, want 1", n)
		}
	})
}

func TestFileStore_FenceAndDigestSurviveReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst")
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, err = st.Append(ctx, id, recOf(0, "a"), durable.WithFence(5)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err = st.Checkpoint(ctx, id, []byte("snap"), 0, durable.WithCheckpointFence(5)); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	reopened, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err = reopened.Append(ctx, id, recOf(1, "b"), durable.WithFence(4)); !errors.Is(err, durable.ErrFenced) {
		t.Fatalf("Append(4) after reopen: err = %v, want ErrFenced", err)
	}
	if _, err = reopened.Append(ctx, id, recOf(0, "other"), durable.WithFence(5)); !errors.Is(err, durable.ErrStepConflict) {
		t.Fatalf("different record after reopen: err = %v, want ErrStepConflict", err)
	}
}

// A ledger written before digests existed can't be compared, so a re-append stays
// the no-op it always was.
func TestFileStore_LedgerWithoutDigest_StaysANoOp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst")
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, err = st.Append(ctx, id, recOf(0, "a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	ledger := filepath.Join(filepath.Dir(durable.JournalPath(dir, id)), "applied.log")
	if err = os.WriteFile(ledger, []byte(`{"key":"step:0","seq":1}`+"\n"), 0o600); err != nil {
		t.Fatalf("rewrite ledger: %v", err)
	}

	reopened, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err = reopened.Append(ctx, id, recOf(0, "other")); err != nil {
		t.Fatalf("re-append over an old ledger: %v", err)
	}
}

func TestFileStore_CorruptFence_FailsLoudly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst")
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, err = st.Append(ctx, id, recOf(0, "a"), durable.WithFence(5)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	fence := filepath.Join(filepath.Dir(durable.JournalPath(dir, id)), "fence")
	if err = os.WriteFile(fence, []byte("not a number"), 0o600); err != nil {
		t.Fatalf("corrupt fence: %v", err)
	}

	reopened, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, _, err = reopened.Load(ctx, id); err == nil {
		t.Fatal("Load over a corrupt fence succeeded")
	}
}

// A Start that crashed after raising the fence but before its baseline landed never
// wrote the instance, so the id must stay usable.
func TestFileStore_FenceOnlyDirectory_IsNotAnInstance(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst")
	instDir := filepath.Dir(durable.JournalPath(dir, id))
	if err := os.MkdirAll(instDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(instDir, "fence"), []byte("5"), 0o600); err != nil {
		t.Fatalf("write fence: %v", err)
	}
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, _, err = st.Load(ctx, id); !errors.Is(err, durable.ErrInstanceNotFound) {
		t.Fatalf("Load: err = %v, want ErrInstanceNotFound", err)
	}
	if err = st.Checkpoint(ctx, id, []byte("snap"), -1, durable.WithCheckpointFence(4)); !errors.Is(err, durable.ErrFenced) {
		t.Fatalf("lower token: err = %v, want ErrFenced", err)
	}
	if err = st.Checkpoint(ctx, id, []byte("snap"), -1, durable.WithCheckpointFence(5)); err != nil {
		t.Fatalf("retried Start's checkpoint: %v", err)
	}
}

func TestStore_Fence_ComesBeforeRecordEncoding(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		if _, err := st.Append(ctx, id, recOf(0, "a"), durable.WithFence(2)); err != nil {
			t.Fatalf("Append: %v", err)
		}
		bad := durable.Record{Step: 1, Entries: []state.JournalEntry{{Payload: json.RawMessage("{")}}}
		if _, err := st.Append(ctx, id, bad, durable.WithFence(1)); !errors.Is(err, durable.ErrFenced) {
			t.Fatalf("err = %v, want ErrFenced", err)
		}
	})
}

func TestFenceAccessors(t *testing.T) {
	tests := []struct {
		name  string
		token int64
		want  int64
	}{
		{"a token", 7, 7},
		{"zero is ignored", 0, 0},
		{"negative is ignored", -1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := durable.AppendFence(durable.WithFence(tt.token)); got != tt.want {
				t.Fatalf("AppendFence = %d, want %d", got, tt.want)
			}
			if got := durable.CheckpointFence(durable.WithCheckpointFence(tt.token)); got != tt.want {
				t.Fatalf("CheckpointFence = %d, want %d", got, tt.want)
			}
		})
	}
	if got := durable.AppendFence(); got != 0 {
		t.Fatalf("AppendFence() = %d, want 0", got)
	}
	if got := durable.AppendIdempotencyKey(durable.WithIdempotencyKey("k")); got != "k" {
		t.Fatalf("AppendIdempotencyKey = %q, want k", got)
	}
	if got := durable.AppendIdempotencyKey(); got != "" {
		t.Fatalf("AppendIdempotencyKey() = %q, want empty", got)
	}
}

func ownerFence(token int64) durable.Option[string, string, *runCtx] {
	return durable.WithOwnerFence[string, string, *runCtx](token)
}

// fenceSpy records the token on every write the Runner makes.
type fenceSpy struct {
	durable.Store
	appends     []int64
	checkpoints []int64
}

func (s *fenceSpy) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	s.appends = append(s.appends, durable.AppendFence(opts...))
	return s.Store.Append(ctx, id, rec, opts...)
}

func (s *fenceSpy) Checkpoint(ctx context.Context, id durable.InstanceID, snapshot []byte, throughStep int, opts ...durable.CheckpointOption) error {
	s.checkpoints = append(s.checkpoints, durable.CheckpointFence(opts...))
	return s.Store.Checkpoint(ctx, id, snapshot, throughStep, opts...)
}

func TestRunner_OwnerFence_CarriedOnEveryWrite(t *testing.T) {
	ctx := context.Background()
	m := linearMachine()
	spy := &fenceSpy{Store: durable.NewMemStore()}
	opts := []durable.Option[string, string, *runCtx]{
		ownerFence(4),
		durable.WithCheckpointEvery[string, string, *runCtx](1),
	}
	h, err := durable.NewRunner(m, spy, opts...).Start(ctx, "inst", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if len(spy.appends) != 1 || len(spy.checkpoints) != 2 {
		t.Fatalf("appends %v, checkpoints %v: want 1 and 2 writes", spy.appends, spy.checkpoints)
	}
	for _, got := range append(spy.appends, spy.checkpoints...) {
		if got != 4 {
			t.Fatalf("write carried token %d, want 4", got)
		}
	}
}

func TestRecover_NewerOwnerFencesTheOlder(t *testing.T) {
	ctx := context.Background()
	m := branchingMachine()
	store := durable.NewMemStore()
	const id = durable.InstanceID("split-1")
	if _, err := durable.NewRunner(m, store, ownerFence(1)).Start(ctx, id, &runCtx{}, state.WithInitialState("start")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stale, err := durable.Recover(ctx, m, store, id, ownerFence(1))
	if err != nil {
		t.Fatalf("Recover(stale): %v", err)
	}
	owner, err := durable.Recover(ctx, m, store, id, ownerFence(2))
	if err != nil {
		t.Fatalf("Recover(owner): %v", err)
	}
	if _, err = owner.Fire(ctx, "goLeft"); err != nil {
		t.Fatalf("owner Fire: %v", err)
	}
	if _, err = stale.Fire(ctx, "goRight"); !errors.Is(err, durable.ErrFenced) {
		t.Fatalf("stale Fire: err = %v, want ErrFenced", err)
	}
	recovered, err := durable.Recover(ctx, m, store, id, ownerFence(2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "left" {
		t.Fatalf("recovered at %q, want left", got)
	}
}

// Recovery writes no fence token, so a stale write can land before the new owner's
// first. The owner hears about it from the step check and recovers the journal as
// written, so no acknowledged step is lost.
func TestRecover_StaleWriteBeforeOwnersFirst_ConflictsThenRecovers(t *testing.T) {
	ctx := context.Background()
	m := branchingMachine()
	store := durable.NewMemStore()
	const id = durable.InstanceID("split-1")
	if _, err := durable.NewRunner(m, store, ownerFence(1)).Start(ctx, id, &runCtx{}, state.WithInitialState("start")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stale, err := durable.Recover(ctx, m, store, id, ownerFence(1))
	if err != nil {
		t.Fatalf("Recover(stale): %v", err)
	}
	owner, err := durable.Recover(ctx, m, store, id, ownerFence(2))
	if err != nil {
		t.Fatalf("Recover(owner): %v", err)
	}
	if _, err = stale.Fire(ctx, "goRight"); err != nil {
		t.Fatalf("stale Fire: %v", err)
	}
	if _, err = owner.Fire(ctx, "goLeft"); !errors.Is(err, durable.ErrStepConflict) {
		t.Fatalf("owner Fire: err = %v, want ErrStepConflict", err)
	}
	// The owner's refused write still raised the fence.
	if _, err = stale.Fire(ctx, "finish"); !errors.Is(err, durable.ErrFenced) {
		t.Fatalf("stale Fire after takeover: err = %v, want ErrFenced", err)
	}
	owner, err = durable.Recover(ctx, m, store, id, ownerFence(2))
	if err != nil {
		t.Fatalf("Recover(owner again): %v", err)
	}
	if got := owner.Instance().Current(); got != "right" {
		t.Fatalf("recovered at %q, want right", got)
	}
	if _, err = owner.Fire(ctx, "finish"); err != nil {
		t.Fatalf("owner Fire after recovering: %v", err)
	}
}

// TestStore_Fence_RacingTokens races owners with rising tokens. Once a token is
// acknowledged no lower one may be, and tokens never fall along the journal.
func TestStore_Fence_RacingTokens(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		var highest, next, violations atomic.Int64
		var wg sync.WaitGroup
		for token := int64(1); token <= 8; token++ {
			wg.Go(func() {
				for range 40 {
					step := int(next.Add(1))
					before := highest.Load()
					payload, err := json.Marshal(token)
					if err != nil {
						t.Errorf("Marshal: %v", err)
						return
					}
					_, err = st.Append(ctx, id, durable.Record{Step: step, Snapshot: payload}, durable.WithFence(token))
					switch {
					case err == nil:
						if token < before {
							violations.Add(1)
						}
						for {
							seen := highest.Load()
							if token <= seen || highest.CompareAndSwap(seen, token) {
								break
							}
						}
					case errors.Is(err, durable.ErrFenced), errors.Is(err, durable.ErrStepOutOfOrder), errors.Is(err, durable.ErrStepConflict):
					default:
						t.Errorf("Append: %v", err)
					}
				}
			})
		}
		wg.Wait()
		if n := violations.Load(); n != 0 {
			t.Fatalf("%d lower tokens acknowledged after a higher one", n)
		}
		_, tail, err := st.Load(ctx, id)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		var last int64
		for _, rec := range tail {
			var token int64
			if err := json.Unmarshal(rec.Snapshot, &token); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if token < last {
				t.Fatalf("token fell from %d to %d at step %d", last, token, rec.Step)
			}
			last = token
		}
	})
}

// TestStore_RefusedFirstWrite_IsNotAnInstance proves a first write refused after the
// fence check leaves an instance Start can still create, on every backend.
func TestStore_RefusedFirstWrite_IsNotAnInstance(t *testing.T) {
	storeBackends(t, func(t *testing.T, st durable.Store) {
		ctx := context.Background()
		const id = durable.InstanceID("inst")
		bad := durable.Record{Step: 0, Entries: []state.JournalEntry{{Payload: json.RawMessage("{")}}}
		if _, err := st.Append(ctx, id, bad, durable.WithFence(1)); err == nil {
			t.Fatal("Append of an unencodable Record succeeded")
		}
		if _, _, err := st.Load(ctx, id); !errors.Is(err, durable.ErrInstanceNotFound) {
			t.Fatalf("Load = %v, want ErrInstanceNotFound", err)
		}
	})
}

// TestFileStore_EmptyDirectoryWithNoFence_StillExists proves a directory with no fence
// keeps the meaning it had before fencing.
func TestFileStore_EmptyDirectoryWithNoFence_StillExists(t *testing.T) {
	root := t.TempDir()
	st, err := durable.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	const id = durable.InstanceID("inst")
	if err = st.MarkDispatched(context.Background(), id, "e"); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		t.Fatalf("ReadDir = (%v, %v)", entries, err)
	}
	dir := filepath.Join(root, entries[0].Name())
	if err = os.Remove(filepath.Join(dir, "dispatched.log")); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	reopened, err := durable.NewFileStore(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, _, err = reopened.Load(context.Background(), id); err != nil {
		t.Fatalf("Load of an existing directory = %v, want nil", err)
	}
}
