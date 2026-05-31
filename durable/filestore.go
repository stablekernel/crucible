package durable

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStore is an on-disk Store: a directory of per-instance subdirectories,
// each holding an append-only journal of Records, the instance's latest
// Snapshot checkpoint, an append-only idempotency ledger, and an append-only set
// of dispatched effect ids. It is stdlib-only and durable across process
// restarts — a fresh FileStore opened over the same directory reconstructs every
// instance from disk, with no carried in-memory state — so it is the persistent
// reference backend the in-memory MemStore is the volatile counterpart to.
//
// # On-disk layout
//
// Under the store's root directory, each instance lives in its own subdirectory
// named by a filesystem-safe encoding of its InstanceID:
//
//	<root>/<encoded-id>/journal.log     append-only, one JSON Record per line
//	<root>/<encoded-id>/checkpoint.snap latest checkpoint Snapshot bytes
//	<root>/<encoded-id>/checkpoint.meta latest checkpoint throughStep
//	<root>/<encoded-id>/applied.log     append-only idempotency ledger (key\tseq)
//	<root>/<encoded-id>/dispatched.log  append-only dispatched effect ids
//
// The journal is the source of truth for the post-checkpoint Record tail; the
// checkpoint files hold the compacted prefix's Snapshot; the applied ledger
// preserves an idempotency key even after its Record is compacted out of the
// journal; the dispatched log backs the DispatchStore dedup set.
//
// # Atomicity and crash-safety
//
// An Append writes one complete newline-terminated JSON line to the journal and
// flushes it to stable storage before returning, so a successful Append is
// durable. A crash mid-write can leave a torn (newline-less or unparseable)
// trailing line; on reopen the loader reads complete lines and stops at the first
// torn trailing record, discarding it without corrupting the intact prefix. A
// Checkpoint writes the Snapshot and its throughStep through a write-temp+rename,
// which is atomic on POSIX filesystems, then rewrites the journal to only the
// post-checkpoint tail (also via temp+rename), so a concurrent reopen never
// observes a checkpoint torn against its tail.
//
// All methods are safe for concurrent use; a per-instance mutex serializes
// writes to one instance's files while distinct instances proceed independently.
type FileStore struct {
	cfg  fileStoreConfig
	root string

	mu        sync.Mutex // guards instances map mutation
	instances map[InstanceID]*fileInstance
}

// fileInstance is the in-memory index of one instance's on-disk state, rebuilt
// from disk on first access. Its own mutex serializes that instance's file
// writes; the FileStore mutex only guards the instances map.
type fileInstance struct {
	mu sync.Mutex

	dir string

	// loaded is set once the index has been reconstructed from disk, guarding the
	// one-time load under mu so concurrent first-accessers serialize rather than
	// race the lock-free reconstruction. existed records whether that load found an
	// on-disk instance directory.
	loaded  bool
	existed bool

	// checkpoint is the latest Snapshot bytes, or nil if never checkpointed.
	checkpoint []byte
	// throughStep is the Step the checkpoint was taken through. noCheckpoint
	// means none yet.
	throughStep int
	// tail is the post-checkpoint Records, in Step order.
	tail []Record
	// maxStep is the highest Step ever appended. noRecord means none yet.
	maxStep int
	// seq is the monotonic per-instance append sequence.
	seq int64
	// applied maps an idempotency key to the seq assigned on first append. It is
	// seeded from applied.log so a compacted step's key still dedups.
	applied map[string]int64
	// dispatched is the set of effect ids already applied for the instance.
	dispatched map[string]struct{}
}

// NewFileStore opens (creating if absent) a FileStore rooted at dir. Existing
// instance subdirectories under dir are discovered lazily on first access and
// reconstructed from their files, so reopening over a populated directory carries
// every recorded instance forward. Construction is configured through functional
// options; with none supplied it returns a ready store.
func NewFileStore(dir string, opts ...FileStoreOption) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("crucible/durable: FileStore dir must not be empty")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("crucible/durable: creating FileStore root %q: %w", dir, err)
	}
	return &FileStore{
		cfg:       resolveFileStore(opts...),
		root:      dir,
		instances: make(map[InstanceID]*fileInstance),
	}, nil
}

// instanceDir returns the per-instance subdirectory path under the store root.
func instanceDir(root string, id InstanceID) string {
	return filepath.Join(root, encodeInstanceID(id))
}

// JournalPath returns the append-only journal file path for an instance under a
// FileStore rooted at root. It is exported so tests and tooling can inspect or
// fault-inject the on-disk journal directly.
func JournalPath(root string, id InstanceID) string {
	return filepath.Join(instanceDir(root, id), journalFile)
}

const (
	journalFile    = "journal.log"
	checkpointFile = "checkpoint.snap"
	metaFile       = "checkpoint.meta"
	appliedFile    = "applied.log"
	dispatchedFile = "dispatched.log"
)

// instance returns the in-memory index for id, loading it from disk on first
// access (or initializing an empty index for an instance that does not yet exist
// on disk). existed reports whether the instance has any on-disk presence.
//
// The instances map is guarded by s.mu, and each instance's one-time disk load
// is guarded by its own mu: the entry is published to the map empty, then loaded
// under inst.mu. Concurrent first-accessers serialize on inst.mu, so exactly one
// performs the reconstruction while the others wait and observe the fully loaded
// index — no caller ever reads index state that a lock-free load is still
// mutating.
func (s *FileStore) instance(id InstanceID) (*fileInstance, bool, error) {
	s.mu.Lock()
	inst, ok := s.instances[id]
	if !ok {
		inst = &fileInstance{
			dir:         instanceDir(s.root, id),
			throughStep: noCheckpoint,
			maxStep:     noRecord,
			applied:     make(map[string]int64),
			dispatched:  make(map[string]struct{}),
		}
		s.instances[id] = inst
	}
	s.mu.Unlock()

	inst.mu.Lock()
	defer inst.mu.Unlock()
	if !inst.loaded {
		existed, err := inst.load()
		if err != nil {
			return nil, false, err
		}
		inst.existed = existed
		inst.loaded = true
	}
	return inst, inst.existed, nil
}

// load reconstructs the instance index from its on-disk files. It returns
// whether the instance directory existed (an instance ever written). A torn
// trailing journal line is skipped without error.
func (fi *fileInstance) load() (bool, error) {
	info, err := os.Stat(fi.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("crucible/durable: stat instance dir %q: %w", fi.dir, err)
	}
	if !info.IsDir() {
		return false, fmt.Errorf("crucible/durable: instance path %q is not a directory", fi.dir)
	}

	if err := fi.loadCheckpoint(); err != nil {
		return false, err
	}
	if err := fi.loadApplied(); err != nil {
		return false, err
	}
	if err := fi.loadJournal(); err != nil {
		return false, err
	}
	if err := fi.loadDispatched(); err != nil {
		return false, err
	}
	return true, nil
}

// loadCheckpoint reads the latest checkpoint Snapshot bytes and its throughStep,
// if present.
func (fi *fileInstance) loadCheckpoint() error {
	snap, err := os.ReadFile(filepath.Join(fi.dir, checkpointFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("crucible/durable: reading checkpoint for %q: %w", fi.dir, err)
	}
	metaBytes, err := os.ReadFile(filepath.Join(fi.dir, metaFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// A snapshot without intact meta is treated as no checkpoint.
			return nil
		}
		return fmt.Errorf("crucible/durable: reading checkpoint meta for %q: %w", fi.dir, err)
	}
	var through int
	if err := json.Unmarshal(metaBytes, &through); err != nil {
		return fmt.Errorf("crucible/durable: decoding checkpoint meta for %q: %w", fi.dir, err)
	}
	fi.checkpoint = snap
	fi.throughStep = through
	if through > fi.maxStep {
		fi.maxStep = through
	}
	return nil
}

// loadApplied seeds the idempotency ledger from applied.log, so a compacted
// step's dedup key is still recognized. A torn trailing ledger line is ignored;
// ledger order is monotonic so only the last line can be partial.
func (fi *fileInstance) loadApplied() error {
	return scanLines(filepath.Join(fi.dir, appliedFile), "applied ledger", func(line []byte) {
		var entry appliedEntry
		if jerr := json.Unmarshal(line, &entry); jerr != nil {
			return
		}
		fi.applied[entry.Key] = entry.Seq
		if entry.Seq > fi.seq {
			fi.seq = entry.Seq
		}
	})
}

// loadJournal reads the post-checkpoint Record tail from journal.log, skipping a
// torn trailing line. Records with a Step at or below the checkpoint are ignored
// (compaction may have lagged), so the tail holds only post-checkpoint steps.
func (fi *fileInstance) loadJournal() error {
	return scanLines(filepath.Join(fi.dir, journalFile), "journal", func(line []byte) {
		var rec Record
		// A torn or partial trailing record fails to parse and is skipped without
		// corrupting the intact prefix.
		if jerr := json.Unmarshal(line, &rec); jerr != nil {
			return
		}
		if fi.throughStep != noCheckpoint && rec.Step <= fi.throughStep {
			return
		}
		fi.tail = append(fi.tail, rec)
		if rec.Step > fi.maxStep {
			fi.maxStep = rec.Step
		}
	})
}

// loadDispatched seeds the dispatched-id set from dispatched.log.
func (fi *fileInstance) loadDispatched() error {
	return scanLines(filepath.Join(fi.dir, dispatchedFile), "dispatched log", func(line []byte) {
		if len(line) == 0 {
			return
		}
		fi.dispatched[string(line)] = struct{}{}
	})
}

// scanLines opens path and invokes fn for each newline-delimited line, treating a
// missing file as empty. label names the file in any error. The file is closed
// and its read error is surfaced, so a permissions or IO fault during reopen is
// not silently swallowed.
func scanLines(path, label string, fn func(line []byte)) (err error) {
	f, oerr := os.Open(path)
	if oerr != nil {
		if errors.Is(oerr, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("crucible/durable: opening %s %q: %w", label, path, oerr)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
	for sc.Scan() {
		fn(sc.Bytes())
	}
	if serr := sc.Err(); serr != nil {
		return fmt.Errorf("crucible/durable: scanning %s %q: %w", label, path, serr)
	}
	return nil
}

// maxLineBytes bounds a single journal/ledger line so a corrupt length can't
// exhaust memory; a marshaled Record (including a checkpoint envelope) stays well
// under it in practice.
const maxLineBytes = 16 * 1024 * 1024

// appliedEntry is one line of the idempotency ledger.
type appliedEntry struct {
	Key string `json:"key"`
	Seq int64  `json:"seq"`
}

// Append implements Store. It is atomic and idempotent per (id, key), durably
// writing a complete journal line before returning.
func (s *FileStore) Append(_ context.Context, id InstanceID, rec Record, opts ...AppendOption) (int64, error) {
	cfg := resolveAppend(opts...)
	key := cfg.idempotencyKey
	if key == "" {
		key = fmt.Sprintf("step:%d", rec.Step)
	}

	inst, _, err := s.instance(id)
	if err != nil {
		return 0, err
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if existing, ok := inst.applied[key]; ok {
		return existing, nil
	}
	if rec.Step <= inst.maxStep {
		return 0, fmt.Errorf("%w: step %d is not greater than recorded step %d for instance %q",
			ErrStepOutOfOrder, rec.Step, inst.maxStep, id)
	}

	if err = inst.ensureDir(); err != nil {
		return 0, err
	}
	inst.existed = true
	line, err := json.Marshal(cloneRecord(rec))
	if err != nil {
		return 0, fmt.Errorf("crucible/durable: marshaling record step %d for %q: %w", rec.Step, id, err)
	}
	if err = appendLine(filepath.Join(inst.dir, journalFile), line); err != nil {
		return 0, fmt.Errorf("crucible/durable: appending journal for %q: %w", id, err)
	}

	seq := inst.seq + 1
	appliedLine, err := json.Marshal(appliedEntry{Key: key, Seq: seq})
	if err != nil {
		return 0, fmt.Errorf("crucible/durable: marshaling applied ledger for %q: %w", id, err)
	}
	if err = appendLine(filepath.Join(inst.dir, appliedFile), appliedLine); err != nil {
		return 0, fmt.Errorf("crucible/durable: appending applied ledger for %q: %w", id, err)
	}

	inst.tail = append(inst.tail, cloneRecord(rec))
	inst.maxStep = rec.Step
	inst.seq = seq
	inst.applied[key] = seq
	return seq, nil
}

// Load implements Store. It returns the latest checkpoint plus the post-
// checkpoint Record tail in Step order, or ErrInstanceNotFound.
func (s *FileStore) Load(_ context.Context, id InstanceID) ([]byte, []Record, error) {
	inst, existed, err := s.instance(id)
	if err != nil {
		return nil, nil, err
	}
	if !existed {
		return nil, nil, fmt.Errorf("%w: %q", ErrInstanceNotFound, id)
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()

	var snapshot []byte
	if inst.checkpoint != nil {
		snapshot = append([]byte(nil), inst.checkpoint...)
	}
	tail := make([]Record, len(inst.tail))
	for i, rec := range inst.tail {
		tail[i] = cloneRecord(rec)
	}
	return snapshot, tail, nil
}

// Checkpoint implements Store. It atomically persists snapshot at throughStep
// (temp+rename) and compacts the on-disk journal to only the post-checkpoint
// tail. throughStep must advance beyond the current checkpoint.
func (s *FileStore) Checkpoint(_ context.Context, id InstanceID, snapshot []byte, throughStep int, opts ...CheckpointOption) error {
	_ = resolveCheckpoint(opts...) // retainTail does not change Load's view; on-disk tail is always compacted

	inst, _, err := s.instance(id)
	if err != nil {
		return err
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if throughStep <= inst.throughStep {
		return fmt.Errorf("%w: throughStep %d does not advance past checkpoint step %d for instance %q",
			ErrCheckpointNotAdvancing, throughStep, inst.throughStep, id)
	}
	if err = inst.ensureDir(); err != nil {
		return err
	}
	inst.existed = true

	// Persist the snapshot and its throughStep atomically (temp+rename each).
	meta, err := json.Marshal(throughStep)
	if err != nil {
		return fmt.Errorf("crucible/durable: marshaling checkpoint meta for %q: %w", id, err)
	}
	if err = writeAtomic(filepath.Join(inst.dir, checkpointFile), snapshot); err != nil {
		return fmt.Errorf("crucible/durable: writing checkpoint for %q: %w", id, err)
	}
	if err = writeAtomic(filepath.Join(inst.dir, metaFile), meta); err != nil {
		return fmt.Errorf("crucible/durable: writing checkpoint meta for %q: %w", id, err)
	}

	// Compact the on-disk journal: keep only Records strictly after throughStep.
	kept := inst.tail[:0:0]
	for _, rec := range inst.tail {
		if rec.Step <= throughStep {
			continue
		}
		kept = append(kept, rec)
	}
	if err = inst.rewriteJournal(kept); err != nil {
		return fmt.Errorf("crucible/durable: compacting journal for %q: %w", id, err)
	}

	inst.checkpoint = append([]byte(nil), snapshot...)
	inst.throughStep = throughStep
	inst.tail = kept
	if throughStep > inst.maxStep {
		inst.maxStep = throughStep
	}
	return nil
}

// MarkDispatched records that the effects named by effectIDs have been applied
// for the instance, appending each new id durably. It is idempotent and
// satisfies the DispatchStore seam.
func (s *FileStore) MarkDispatched(_ context.Context, id InstanceID, effectIDs ...string) error {
	if len(effectIDs) == 0 {
		return nil
	}
	inst, _, err := s.instance(id)
	if err != nil {
		return err
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()

	if err = inst.ensureDir(); err != nil {
		return err
	}
	inst.existed = true
	for _, eid := range effectIDs {
		if _, ok := inst.dispatched[eid]; ok {
			continue
		}
		if err = appendLine(filepath.Join(inst.dir, dispatchedFile), []byte(eid)); err != nil {
			return fmt.Errorf("crucible/durable: appending dispatched id for %q: %w", id, err)
		}
		inst.dispatched[eid] = struct{}{}
	}
	return nil
}

// Dispatched returns the set of effect ids already applied for the instance as a
// membership map. An instance never written reports an empty (non-nil) set. It
// satisfies the DispatchStore seam.
func (s *FileStore) Dispatched(_ context.Context, id InstanceID) (map[string]bool, error) {
	inst, _, err := s.instance(id)
	if err != nil {
		return nil, err
	}
	inst.mu.Lock()
	defer inst.mu.Unlock()

	out := make(map[string]bool, len(inst.dispatched))
	for eid := range inst.dispatched {
		out[eid] = true
	}
	return out, nil
}

// ensureDir creates the instance directory on first write.
func (fi *fileInstance) ensureDir() error {
	if err := os.MkdirAll(fi.dir, 0o700); err != nil {
		return fmt.Errorf("crucible/durable: creating instance dir %q: %w", fi.dir, err)
	}
	return nil
}

// rewriteJournal replaces the journal file with exactly recs, one JSON line
// each, atomically via temp+rename so a concurrent reopen never sees a torn
// compaction.
func (fi *fileInstance) rewriteJournal(recs []Record) error {
	var buf []byte
	for _, rec := range recs {
		line, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshaling record step %d: %w", rec.Step, err)
		}
		buf = append(buf, line...)
		buf = append(buf, '\n')
	}
	return writeAtomic(filepath.Join(fi.dir, journalFile), buf)
}

// appendLine appends one newline-terminated line to path as a single write,
// flushing it to stable storage before returning so a successful append is
// durable. A crash mid-write can leave a torn trailing line, which the loader
// detects and skips. The line plus its terminator is written in one call so a
// partial write leaves a truncated (and thus unparseable) trailing record rather
// than a record split across calls.
func appendLine(path string, line []byte) (err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	buf := make([]byte, 0, len(line)+1)
	buf = append(buf, line...)
	buf = append(buf, '\n')
	if _, err = f.Write(buf); err != nil {
		return err
	}
	return f.Sync()
}

// writeAtomic writes data to path atomically: a sibling temp file is written and
// fsynced, then renamed over path (atomic on POSIX), so a reader sees either the
// old contents or the fully written new contents, never a torn mix.
func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once the rename succeeds

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// encodeInstanceID maps an InstanceID to a filesystem-safe directory name. An id
// composed only of safe characters is used verbatim for readability; any other
// id is hex-escaped per byte, so arbitrary ids (including path separators) map to
// a single, collision-free directory name.
func encodeInstanceID(id InstanceID) string {
	s := string(id)
	if s != "" && isSafeID(s) {
		return s
	}
	const hex = "0123456789abcdef"
	out := make([]byte, 0, len(s)*3+1)
	out = append(out, '_') // prefix marks an escaped name, disjoint from safe ids
	for i := 0; i < len(s); i++ {
		c := s[i]
		if isSafeByte(c) {
			out = append(out, c)
			continue
		}
		out = append(out, '%', hex[c>>4], hex[c&0x0f])
	}
	return string(out)
}

// isSafeID reports whether every byte of s is a safe, unescaped directory-name
// byte and s does not begin with the escape prefix.
func isSafeID(s string) bool {
	if s[0] == '_' {
		return false // reserved for escaped names
	}
	for i := 0; i < len(s); i++ {
		if !isSafeByte(s[i]) {
			return false
		}
	}
	return true
}

// isSafeByte reports whether c is safe to use verbatim in a directory name.
func isSafeByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z',
		c >= 'A' && c <= 'Z',
		c >= '0' && c <= '9',
		c == '-', c == '.':
		return true
	default:
		return false
	}
}
