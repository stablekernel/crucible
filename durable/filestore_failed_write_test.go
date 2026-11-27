package durable_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stablekernel/crucible/durable"
)

// The #228 defects: FileStore can lose an acknowledged step or apply one twice
// after a failed write or a crash, contradicting the Atomicity contract in its
// docs. Each test pins one finding at the Store level, with the failure injected
// the way the issue reproduces it — a torn trailing journal line, a crash between
// the two checkpoint renames, a journal step whose ledger line never landed, and a
// failed ledger write followed by a retry. All skip pending #228's ruling.

// instanceFile returns the path of one of an instance's on-disk files, beside its
// journal.
func instanceFile(t *testing.T, dir string, id durable.InstanceID, name string) string {
	t.Helper()
	return filepath.Join(filepath.Dir(durable.JournalPath(dir, id)), name)
}

// dropLastLedgerLine rewrites the applied ledger without its final line, the disk a
// crash between the journal write and the ledger write leaves behind.
func dropLastLedgerLine(t *testing.T, path string) {
	t.Helper()
	lines, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	last := len(lines)
	if last > 0 && lines[last-1] == '\n' {
		last--
	}
	for last > 0 && lines[last-1] != '\n' {
		last--
	}
	if err = os.WriteFile(path, lines[:last], 0o600); err != nil {
		t.Fatalf("rewrite ledger: %v", err)
	}
}

// TestFileStore_AnAppendOntoATornTailIsNotSilentlyAcknowledged pins the first #228
// finding: a failed journal write leaves a torn trailing line, and nothing truncates
// it, so the next Append merges into it and returns success — an acknowledged step
// the disk cannot produce. A fresh store over the same directory must load every
// step a successful Append acknowledged.
func TestFileStore_AnAppendOntoATornTailIsNotSilentlyAcknowledged(t *testing.T) {
	t.Skip("pending #228: a torn trailing journal line makes the next Append merge into it and ack a step the disk can't produce")
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-torn-ack")

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for _, rec := range []durable.Record{recOf(0, "a"), recOf(1, "b")} {
		if _, aerr := st.Append(ctx, id, rec); aerr != nil {
			t.Fatalf("Append step %d: %v", rec.Step, aerr)
		}
	}
	// A torn trailing write, as a crash between flush and completion leaves.
	jf, err := os.OpenFile(durable.JournalPath(dir, id), os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open journal to corrupt: %v", err)
	}
	if _, err = jf.WriteString(`{"step":2,"entries":[{"kind":"serviceResul`); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	if err = jf.Close(); err != nil {
		t.Fatalf("close journal: %v", err)
	}
	// The next Append merges into the torn line. If it acknowledges, the step is
	// durable: a fresh store must load it.
	if _, err = st.Append(ctx, id, recOf(2, "c")); err != nil {
		t.Fatalf("Append onto the torn tail: %v", err)
	}
	st2 := freshFileStore(t, dir)
	_, tail, err := st2.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load after the acknowledged Append: %v", err)
	}
	assertRecordsEqual(t, tail, []durable.Record{recOf(0, "a"), recOf(1, "b"), recOf(2, "c")})
}

// TestFileStore_ACheckpointSnapshotNeverPairsWithTheTailItHolds pins the second #228
// finding: checkpoint.snap renames before checkpoint.meta, so a kill between the two
// pairs a newer snapshot with an older throughStep, and Load returns that snapshot
// with a tail of steps it already holds — replaying them, or failing an invalid
// transition. A snapshot must never load beside a tail reaching at or before the
// step it compacted through.
func TestFileStore_ACheckpointSnapshotNeverPairsWithTheTailItHolds(t *testing.T) {
	t.Skip("pending #228: checkpoint.snap and checkpoint.meta are renamed apart, so a kill between them pairs a new snapshot with an old throughStep")
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-cp-pair")

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for i := range 3 {
		if _, aerr := st.Append(ctx, id, recOf(i, "a")); aerr != nil {
			t.Fatalf("Append %d: %v", i, aerr)
		}
	}
	if err = st.Checkpoint(ctx, id, marshaledSnapshot(t, "one"), 1); err != nil {
		t.Fatalf("Checkpoint at 1: %v", err)
	}
	// A checkpoint at 2 begins: the new snapshot is renamed into place, and the
	// process dies before the meta rename and the compaction, so the journal still
	// holds the step-2 record the new snapshot already compacted through.
	snapTwo := marshaledSnapshot(t, "two")
	if err = os.WriteFile(instanceFile(t, dir, id, "checkpoint.snap"), snapTwo, 0o600); err != nil {
		t.Fatalf("write the new snapshot in: %v", err)
	}
	st2 := freshFileStore(t, dir)
	snap, tail, err := st2.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load after the split checkpoint: %v", err)
	}
	// The new snapshot compacted through step 2, so it must never load beside a tail
	// record at or before step 2: that pairing replays a step the snapshot already
	// holds.
	if string(snap) == string(snapTwo) {
		for _, s := range stepsOf(tail) {
			if s <= 2 {
				t.Fatalf("the step-2 snapshot loaded beside tail steps %v it already holds: replay double-applies", stepsOf(tail))
			}
		}
	}
}

// TestFileStore_AJournaledStepWithoutItsLedgerLineIsIdempotent pins the third #228
// finding's crash case: Append writes the journal before the applied ledger, and
// reopen seeds the ledger from the ledger file alone, so a kill between the two
// leaves a journaled step with no ledger key. Retrying that step must be the
// idempotent no-op the Store contract promises, not ErrStepOutOfOrder.
func TestFileStore_AJournaledStepWithoutItsLedgerLineIsIdempotent(t *testing.T) {
	t.Skip("pending #228: the ledger isn't seeded from the journal, so a kill between the two Append writes breaks the retry's idempotence")
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-no-ledger")

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for _, rec := range []durable.Record{recOf(0, "a"), recOf(1, "b")} {
		if _, aerr := st.Append(ctx, id, rec); aerr != nil {
			t.Fatalf("Append step %d: %v", rec.Step, aerr)
		}
	}
	// The kill between the journal write and the ledger write: the ledger file
	// loses its last line while the journal keeps the record.
	dropLastLedgerLine(t, instanceFile(t, dir, id, "applied.log"))
	st2 := freshFileStore(t, dir)
	if _, _, err = st2.Load(ctx, id); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// The retry of the journaled step: the contract's idempotent no-op.
	if _, err = st2.Append(ctx, id, recOf(1, "b")); err != nil {
		t.Fatalf("retry of the journaled step: %v, want the idempotent no-op", err)
	}
	_, tail, err := st2.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load after the retry: %v", err)
	}
	if got := stepsOf(tail); len(got) != 2 || got[len(got)-1] != 1 {
		t.Fatalf("tail steps = %v, want [0 1] with step 1 once", got)
	}
}

// TestFileStore_AFailedLedgerWriteDoesNotDuplicateAStep pins the third #228 finding's
// failed-write case and its follow-up: a ledger write that fails leaves the step out
// of the in-memory ledger, so a retry — the same record, or a different one for the
// same step — appends to the journal again. The journal must never hold two records
// for one step.
func TestFileStore_AFailedLedgerWriteDoesNotDuplicateAStep(t *testing.T) {
	t.Skip("pending #228: a failed ledger write leaves the step out of memory, so a retry appends the journal twice — the same record, or a different one for the same step")
	ctx := context.Background()
	for _, retry := range []struct {
		name string
		rec  durable.Record
	}{
		{"the same record", recOf(1, "b")},
		{"a different record for the same step", recOf(1, "c")},
	} {
		t.Run(retry.name, func(t *testing.T) {
			dir := t.TempDir()
			const id = durable.InstanceID("inst-failed-ledger")

			st, err := durable.NewFileStore(dir)
			if err != nil {
				t.Fatalf("NewFileStore: %v", err)
			}
			if _, aerr := st.Append(ctx, id, recOf(0, "a")); aerr != nil {
				t.Fatalf("Append 0: %v", aerr)
			}
			// The ledger write fails — its path is unusable — after the journal line
			// landed, leaving the step out of the in-memory ledger.
			ledger := instanceFile(t, dir, id, "applied.log")
			lines, err := os.ReadFile(ledger)
			if err != nil {
				t.Fatalf("read ledger: %v", err)
			}
			if err = os.Remove(ledger); err != nil {
				t.Fatalf("remove ledger: %v", err)
			}
			if err = os.Mkdir(ledger, 0o700); err != nil {
				t.Fatalf("block ledger: %v", err)
			}
			if _, aerr := st.Append(ctx, id, recOf(1, "b")); aerr == nil {
				t.Fatal("Append with the ledger unwritable = nil error, want the write failure")
			}
			if err = os.Remove(ledger); err != nil {
				t.Fatalf("unblock ledger: %v", err)
			}
			if err = os.WriteFile(ledger, lines, 0o600); err != nil {
				t.Fatalf("restore ledger: %v", err)
			}
			// The retry, with the journal already holding step 1.
			if _, aerr := st.Append(ctx, id, retry.rec); aerr != nil {
				t.Fatalf("retry Append: %v", aerr)
			}
			st2 := freshFileStore(t, dir)
			_, tail, err := st2.Load(ctx, id)
			if err != nil {
				t.Fatalf("Load after the retry: %v", err)
			}
			seen := map[int]int{}
			for _, s := range stepsOf(tail) {
				seen[s]++
			}
			for s, n := range seen {
				if n > 1 {
					t.Fatalf("the journal holds %d records for step %d, want at most one", n, s)
				}
			}
		})
	}
}

// stepsOf lists each Record's step, in load order, for failure messages.
func stepsOf(recs []durable.Record) []int {
	out := make([]int, len(recs))
	for i, r := range recs {
		out[i] = r.Step
	}
	return out
}
