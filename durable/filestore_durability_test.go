package durable_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// freshFileStore opens a brand-new FileStore over an existing directory,
// simulating a process restart: it carries NO in-memory state from any earlier
// store over the same dir, so everything it returns was reconstructed from disk.
func freshFileStore(t *testing.T, dir string) *durable.FileStore {
	t.Helper()
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore(reopen): %v", err)
	}
	return st
}

// TestFileStore_RestartDurability_EventMachine is the durability gate for an
// event-driven machine: run it through the durable Runner backed by a FileStore
// over a temp dir, drop that store entirely, open a FRESH FileStore over the SAME
// dir (no carried in-memory state), Recover, and assert the recovered snapshot is
// byte-identical to the live run. This proves real durability across a process
// restart, not merely across an in-memory Recover.
func TestFileStore_RestartDurability_EventMachine(t *testing.T) {
	ctx := context.Background()
	m := assignMachine()
	events := []string{"go", "again", "again", "finish"}
	live := liveSnapshotBytes(t, m, "idle", &runCtx{}, events)

	dir := t.TempDir()
	id := durable.InstanceID("inst-restart")

	// First lifetime: record the run to disk, then drop the store (the "crash").
	func() {
		st, err := durable.NewFileStore(dir)
		if err != nil {
			t.Fatalf("NewFileStore: %v", err)
		}
		runner := durable.NewRunner(m, st, durable.WithCheckpointEvery[string, string, *runCtx](2))
		if _, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle")); err != nil {
			t.Fatalf("Start: %v", err)
		}
		for _, ev := range events {
			if _, err := runner.Fire(ctx, id, ev); err != nil {
				t.Fatalf("Fire(%q): %v", ev, err)
			}
		}
	}()

	// Second lifetime: a fresh store over the same dir, nothing in memory.
	st2 := freshFileStore(t, dir)
	recovered, err := durable.Recover(ctx, m, st2, id,
		durable.WithCheckpointEvery[string, string, *runCtx](2))
	if err != nil {
		t.Fatalf("Recover from fresh store: %v", err)
	}
	got, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered: %v", err)
	}
	if string(live) != string(got) {
		t.Fatalf("restart recovery not byte-identical\n live: %s\n got:  %s", live, got)
	}
}

// TestFileStore_RestartDurability_TimerMachine is the durability gate for a
// timer-driven machine across a restart: the live run arms a durable timer and
// ticks it past its deadline; a fresh FileStore over the same dir on a completely
// different wall-clock baseline recovers to a byte-identical snapshot, proving the
// recorded clock readings (and any persisted timer deadlines) survived to disk.
func TestFileStore_RestartDurability_TimerMachine(t *testing.T) {
	ctx := context.Background()
	m := timerMachine()
	dir := t.TempDir()
	id := durable.InstanceID("timer-restart")

	var liveBytes []byte
	func() {
		st, err := durable.NewFileStore(dir)
		if err != nil {
			t.Fatalf("NewFileStore: %v", err)
		}
		clk := state.NewFakeClock(epoch)
		runner := durable.NewRunner(m, st, durable.WithRunnerClock[string, string, *timerCtx](clk))
		h, err := runner.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "arm"); err != nil {
			t.Fatalf("Fire(arm): %v", err)
		}
		clk.Advance(6 * time.Second)
		if _, err = h.Tick(ctx); err != nil {
			t.Fatalf("Tick: %v", err)
		}
		if got := h.Instance().Snapshot().Current; got != "done" {
			t.Fatalf("live did not reach done: %q", got)
		}
		liveBytes, err = state.MarshalSnapshot(h.Instance().Snapshot())
		if err != nil {
			t.Fatalf("marshal live: %v", err)
		}
	}()

	// Fresh store over the same dir, recovering on a wall clock 100h ahead: if
	// replay read the wall clock the timing would diverge. It reads the recorded
	// values from disk instead.
	st2 := freshFileStore(t, dir)
	recovered, err := durable.Recover(ctx, m, st2, id,
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch.Add(100*time.Hour))))
	if err != nil {
		t.Fatalf("Recover from fresh store: %v", err)
	}
	recBytes, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered: %v", err)
	}
	if string(liveBytes) != string(recBytes) {
		t.Fatalf("timer restart not byte-identical\n live: %s\n got:  %s", liveBytes, recBytes)
	}
	if recovered.Instance().Snapshot().Current != "done" {
		t.Fatalf("recovered did not reach done: %q", recovered.Instance().Snapshot().Current)
	}
}

// TestFileStore_TornTrailingRecord_RecoversCleanly proves a crash mid-append is
// survivable: a partially written trailing journal record (a torn line, as a
// power loss between buffer flush and completion would leave) is detected on
// reopen and skipped, leaving the prior fully written records intact and loadable.
func TestFileStore_TornTrailingRecord_RecoversCleanly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-torn")

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	want := []durable.Record{recOf(0, "a"), recOf(1, "b")}
	for _, rec := range want {
		if _, aerr := st.Append(ctx, id, rec); aerr != nil {
			t.Fatalf("Append step %d: %v", rec.Step, aerr)
		}
	}

	// Simulate a torn trailing write: append a partial, newline-less fragment of a
	// would-be third record directly to the journal file, exactly as a crash
	// between flush and durable completion would leave on disk.
	jpath := durable.JournalPath(dir, id)
	f, err := os.OpenFile(jpath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open journal to corrupt: %v", err)
	}
	if _, err = f.WriteString(`{"step":2,"entries":[{"kind":"serviceResul`); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	if err = f.Close(); err != nil {
		t.Fatalf("close journal: %v", err)
	}

	// A fresh store over the same dir must skip the torn tail and load only the
	// fully written records.
	st2 := freshFileStore(t, dir)
	_, tail, err := st2.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load after torn write: %v", err)
	}
	assertRecordsEqual(t, tail, want)

	// The store recovers to a writable state: a subsequent in-order append lands
	// after the last intact record (the torn fragment did not bump maxStep).
	if _, err = st2.Append(ctx, id, recOf(2, "c")); err != nil {
		t.Fatalf("Append after torn recovery: %v", err)
	}
	_, tail, err = st2.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load after re-append: %v", err)
	}
	assertRecordsEqual(t, tail, append(want, recOf(2, "c")))
}

// TestFileStore_CompactionShrinksJournal proves checkpoint compaction reclaims the
// on-disk journal tail: after a checkpoint compacts early steps, the journal file
// is strictly smaller than before, and a fresh store over the same dir still loads
// only the post-checkpoint tail.
func TestFileStore_CompactionShrinksJournal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-compact")

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for i := range 8 {
		if _, aerr := st.Append(ctx, id, recOf(i, fmt.Sprintf("svc-%d", i))); aerr != nil {
			t.Fatalf("Append %d: %v", i, aerr)
		}
	}
	jpath := durable.JournalPath(dir, id)
	before := fileSize(t, jpath)

	if err = st.Checkpoint(ctx, id, marshaledSnapshot(t, "cp"), 5); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	after := fileSize(t, jpath)
	if after >= before {
		t.Fatalf("journal did not shrink after compaction: before=%d after=%d", before, after)
	}

	// A fresh store over the same dir loads only the post-checkpoint tail.
	st2 := freshFileStore(t, dir)
	snap, tail, err := st2.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load after compaction: %v", err)
	}
	if snap == nil {
		t.Fatal("expected a checkpoint snapshot after compaction")
	}
	assertRecordsEqual(t, tail, []durable.Record{recOf(6, "svc-6"), recOf(7, "svc-7")})
}

// TestFileStore_Concurrent_RaceSafe drives concurrent appends, idempotent retries,
// and loads across separate instances so `go test -race` can detect unsynchronized
// access, then verifies a fresh store over the same dir loads every record.
func TestFileStore_Concurrent_RaceSafe(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	const instances = 6
	const steps = 40

	var wg sync.WaitGroup
	for inst := range instances {
		id := durable.InstanceID(fmt.Sprintf("inst-%d", inst))
		wg.Add(3)

		go func() {
			defer wg.Done()
			for s := range steps {
				_, _ = st.Append(ctx, id, recOf(s, "svc"))
			}
		}()
		go func() {
			defer wg.Done()
			for range steps {
				_, _ = st.Append(ctx, id, recOf(0, "svc"))
			}
		}()
		go func() {
			defer wg.Done()
			for range steps {
				_, _, _ = st.Load(ctx, id)
			}
		}()
	}
	wg.Wait()

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

	// A fresh store over the same dir reconstructs every instance from disk.
	st2 := freshFileStore(t, dir)
	for inst := range instances {
		id := durable.InstanceID(fmt.Sprintf("inst-%d", inst))
		_, tail, err := st2.Load(ctx, id)
		if err != nil {
			t.Fatalf("reopen Load %s: %v", id, err)
		}
		if len(tail) != steps {
			t.Fatalf("reopened instance %s has %d records, want %d", id, len(tail), steps)
		}
	}
}

// TestFileStore_DispatchedSurvivesRestart proves the dispatched-set is durable:
// marked effect ids are reconstructed by a fresh store over the same dir, so a
// delayed redispatch after a restart still dedups.
func TestFileStore_DispatchedSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-dispatch")

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	// An instance must exist before its dispatched-set is meaningful on reopen.
	if _, err = st.Append(ctx, id, recOf(0, "a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err = st.MarkDispatched(ctx, id, "0#0#email", "0#1#charge"); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}

	st2 := freshFileStore(t, dir)
	got, err := st2.Dispatched(ctx, id)
	if err != nil {
		t.Fatalf("Dispatched after restart: %v", err)
	}
	if !got["0#0#email"] || !got["0#1#charge"] || len(got) != 2 {
		t.Fatalf("dispatched set lost across restart: %v", got)
	}
}

// TestFileStore_MissingCheckpointMeta_TreatedAsNoCheckpoint proves a checkpoint
// snapshot whose throughStep meta did not survive (a crash between the snapshot
// rename and the meta rename) is treated as no checkpoint on reopen, so the full
// journal tail still loads rather than a half-applied checkpoint.
func TestFileStore_MissingCheckpointMeta_TreatedAsNoCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-meta")

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	for i := range 3 {
		if _, aerr := st.Append(ctx, id, recOf(i, "a")); aerr != nil {
			t.Fatalf("Append %d: %v", i, aerr)
		}
	}
	if err = st.Checkpoint(ctx, id, marshaledSnapshot(t, "cp"), 1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	// Remove the throughStep meta, leaving an orphan snapshot file.
	if err = os.Remove(filepath.Join(filepath.Dir(durable.JournalPath(dir, id)), "checkpoint.meta")); err != nil {
		t.Fatalf("remove meta: %v", err)
	}

	st2 := freshFileStore(t, dir)
	snap, tail, err := st2.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if snap != nil {
		t.Fatalf("orphan checkpoint surfaced: %q", snap)
	}
	// With no checkpoint recognized, the compacted-but-still-present tail (step 2)
	// loads; the journal was compacted through step 1 at checkpoint time.
	assertRecordsEqual(t, tail, []durable.Record{recOf(2, "a")})
}

// TestFileStore_WriteToBlockedInstanceDir surfaces an IO error when the instance
// directory cannot be created because a regular file already occupies its path.
func TestFileStore_WriteToBlockedInstanceDir(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const id = durable.InstanceID("blocked")

	// Place a regular file exactly where the instance directory must go, so
	// MkdirAll fails on the first write.
	instPath := filepath.Dir(durable.JournalPath(root, id))
	if err := os.WriteFile(instPath, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed blocking file: %v", err)
	}

	st, err := durable.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, err := st.Append(ctx, id, recOf(0, "a")); err == nil {
		t.Fatal("Append onto a blocked instance dir = nil error, want an IO error")
	}
	if err := st.MarkDispatched(ctx, id, "e1"); err == nil {
		t.Fatal("MarkDispatched onto a blocked instance dir = nil error, want an IO error")
	}
	if err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "cp"), 0); err == nil {
		t.Fatal("Checkpoint onto a blocked instance dir = nil error, want an IO error")
	}
}

// TestFileStore_LoadInstancePathIsFile surfaces an error when an instance's
// directory path on disk is a regular file rather than a directory.
func TestFileStore_LoadInstancePathIsFile(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const id = durable.InstanceID("notadir")

	instPath := filepath.Dir(durable.JournalPath(root, id))
	if err := os.WriteFile(instPath, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	st, err := durable.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, _, err := st.Load(ctx, id); err == nil {
		t.Fatal("Load of a file-as-instance-dir = nil error, want an error")
	}
}

// TestFileStore_TornAppliedLedger proves a torn trailing idempotency-ledger line
// is skipped on reopen, leaving earlier dedup keys recognized and the store
// writable.
func TestFileStore_TornAppliedLedger(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-ledger")

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, err = st.Append(ctx, id, recOf(0, "a")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Append a torn trailing line to the applied ledger.
	apath := filepath.Join(filepath.Dir(durable.JournalPath(dir, id)), "applied.log")
	f, err := os.OpenFile(apath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	if _, err := f.WriteString(`{"key":"step:1","se`); err != nil {
		t.Fatalf("write torn ledger: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close ledger: %v", err)
	}

	st2 := freshFileStore(t, dir)
	// Step 0's key is still recognized (idempotent re-append returns its seq).
	if _, err := st2.Append(ctx, id, recOf(0, "a")); err != nil {
		t.Fatalf("idempotent re-append after torn ledger: %v", err)
	}
	if _, err := st2.Append(ctx, id, recOf(1, "b")); err != nil {
		t.Fatalf("Append step 1 after torn ledger: %v", err)
	}
}

// TestFileStore_ReadOnlyInstanceDir surfaces IO errors from the low-level write
// helpers (appendLine's open, writeAtomic's temp create) when an existing
// instance directory is made read-only after creation, as a permissions
// regression on the data directory would.
func TestFileStore_ReadOnlyInstanceDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("windows does not enforce unix directory permission bits: a read-only mode still permits file creation")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission bits")
	}
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-ro")

	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, err := st.Append(ctx, id, recOf(0, "a")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	instPath := filepath.Dir(durable.JournalPath(dir, id))
	if err := os.Chmod(instPath, 0o500); err != nil {
		t.Fatalf("chmod read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(instPath, 0o700) })

	// appendLine cannot open a new-content append into a read-only directory for a
	// fresh dispatched.log, and writeAtomic cannot create its temp file.
	if err := st.MarkDispatched(ctx, id, "e-new"); err == nil {
		t.Fatal("MarkDispatched into read-only dir = nil error, want an IO error")
	}
	if err := st.Checkpoint(ctx, id, marshaledSnapshot(t, "cp"), 0); err == nil {
		t.Fatal("Checkpoint into read-only dir = nil error, want an IO error")
	}
}

// TestFileStore_ReopenUnreadableFiles surfaces load-time IO errors: an instance
// whose on-disk files cannot be read (a permissions regression) fails the reopen
// rather than silently returning a partial reconstruction.
func TestFileStore_ReopenUnreadableFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("windows ignores the unix read permission bit: a 0o000 file stays readable by its owner")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permission bits")
	}
	ctx := context.Background()

	for _, name := range []string{"journal.log", "applied.log", "dispatched.log", "checkpoint.snap"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			const id = durable.InstanceID("inst-unreadable")
			st, err := durable.NewFileStore(dir)
			if err != nil {
				t.Fatalf("NewFileStore: %v", err)
			}
			if _, err = st.Append(ctx, id, recOf(0, "a")); err != nil {
				t.Fatalf("Append: %v", err)
			}
			if err = st.MarkDispatched(ctx, id, "e1"); err != nil {
				t.Fatalf("MarkDispatched: %v", err)
			}
			if err = st.Checkpoint(ctx, id, marshaledSnapshot(t, "cp"), 0); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}

			instDir := filepath.Dir(durable.JournalPath(dir, id))
			target := filepath.Join(instDir, name)
			if err = os.Chmod(target, 0o000); err != nil {
				t.Fatalf("chmod unreadable: %v", err)
			}
			t.Cleanup(func() { _ = os.Chmod(target, 0o600) })

			st2, err := durable.NewFileStore(dir)
			if err != nil {
				t.Fatalf("NewFileStore reopen: %v", err)
			}
			if _, _, err := st2.Load(ctx, id); err == nil {
				t.Fatalf("Load with unreadable %s = nil error, want an IO error", name)
			}
		})
	}
}

// TestFileStore_Dispatched_UnknownInstance confirms the dedup query on a never-
// written instance returns an empty, non-nil set rather than an error.
func TestFileStore_Dispatched_UnknownInstance(t *testing.T) {
	ctx := context.Background()
	st := newFileStore(t)
	got, err := st.Dispatched(ctx, "never-written")
	if err != nil {
		t.Fatalf("Dispatched(unknown): %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("Dispatched(unknown) = %v, want empty non-nil map", got)
	}
}

// TestFileStore_DispatchedLog_SkipsBlankLines confirms a blank line in
// dispatched.log (a benign artifact) is ignored on reopen.
func TestFileStore_DispatchedLog_SkipsBlankLines(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const id = durable.InstanceID("inst-blank")
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if _, err = st.Append(ctx, id, recOf(0, "a")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err = st.MarkDispatched(ctx, id, "e1"); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}
	dpath := filepath.Join(filepath.Dir(durable.JournalPath(dir, id)), "dispatched.log")
	f, err := os.OpenFile(dpath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open dispatched: %v", err)
	}
	if _, err = f.WriteString("\n\n"); err != nil {
		t.Fatalf("write blanks: %v", err)
	}
	if err = f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st2 := freshFileStore(t, dir)
	got, err := st2.Dispatched(ctx, id)
	if err != nil {
		t.Fatalf("Dispatched: %v", err)
	}
	if len(got) != 1 || !got["e1"] {
		t.Fatalf("Dispatched = %v, want {e1}", got)
	}
}

// fileSize returns the size in bytes of the file at path.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}
