package durable

import (
	"context"
	"fmt"
	"sync"
)

// MemStore is the in-memory reference Store: a thread-safe map of instances,
// each holding its checkpoint Snapshot and its ordered Record tail. It is the
// conformance backend for the Store contract and the backend the package's
// Example tests run against. It is stdlib-only and holds everything in memory,
// so it is for tests, examples, and single-process development — not durable
// across process restarts. Use a persistent backend for production durability.
//
// Per-instance sentinels for a MemStore's "nothing yet" bookkeeping. They sit
// below the start baseline (baselineStep == -1) so the baseline checkpoint and
// the baseline append both advance past them.
const (
	// noCheckpoint marks an instance with no checkpoint taken yet.
	noCheckpoint = -2
	// noRecord marks an instance with no Record appended yet.
	noRecord = -2
)

// MemStore satisfies Store and is safe for concurrent use by multiple
// goroutines.
type MemStore struct {
	cfg memStoreConfig

	mu        sync.RWMutex
	instances map[InstanceID]*memInstance
}

// memInstance is one instance's in-memory durable state under MemStore: its
// latest checkpoint plus the Record tail appended after it, and the bookkeeping
// the contract needs (append sequence, idempotency dedup set).
type memInstance struct {
	// checkpoint is the latest marshaled Snapshot, or nil if never checkpointed.
	checkpoint []byte
	// throughStep is the Step the checkpoint was taken through; tail holds only
	// Records with a greater Step. noCheckpoint means no checkpoint yet — set
	// below the start baseline (baselineStep) so the baseline checkpoint advances.
	throughStep int
	// tail is the post-checkpoint Records, in Step order.
	tail []Record
	// baseline is the instance's start baseline checkpoint snapshot (the
	// BaselineStep checkpoint), captured once when WithHistory is set so a
	// time-travel read can restore the pre-event cast even after later checkpoints
	// overwrote the live checkpoint. It is nil when history is disabled.
	baseline []byte
	// history holds every Record ever appended, in Step order, preserved when
	// WithHistory is set so a time-travel read reaches a step a checkpoint compacted
	// out of the live tail. It is nil when history is disabled.
	history []Record
	// maxStep is the highest Step ever appended (across checkpoint and tail), for
	// the strict-ordering check. -1 means nothing appended yet.
	maxStep int
	// seq is the monotonic per-instance append sequence; it advances on every
	// non-idempotent Append.
	seq int64
	// applied maps an append's idempotency key (the Record Step by default, or an
	// explicit WithIdempotencyKey value) to the seq assigned on first append, so
	// a re-append is a no-op returning that seq.
	applied map[string]int64
	// dispatched is the set of effect ids the Runner has applied through its
	// effect handler. It backs the exactly-once dedup: an effect whose id is
	// present is skipped on (re)dispatch. It survives checkpoint compaction so a
	// delayed redispatch of an already-applied effect stays a no-op.
	dispatched map[string]struct{}
}

// NewMemStore returns an in-memory Store. Construction is configured through
// functional options (see WithInitialCapacity); with none supplied it returns a
// ready, empty store.
func NewMemStore(opts ...MemStoreOption) *MemStore {
	return &MemStore{
		cfg:       resolveMemStore(opts...),
		instances: make(map[InstanceID]*memInstance),
	}
}

// instanceLocked returns the instance state for id, creating it if absent. The
// caller must hold the write lock.
func (s *MemStore) instanceLocked(id InstanceID) *memInstance {
	inst, ok := s.instances[id]
	if !ok {
		inst = &memInstance{
			throughStep: noCheckpoint,
			maxStep:     noRecord,
			applied:     make(map[string]int64),
			dispatched:  make(map[string]struct{}),
		}
		if s.cfg.initialCapacity > 0 {
			inst.tail = make([]Record, 0, s.cfg.initialCapacity)
		}
		s.instances[id] = inst
	}
	return inst
}

// Append implements Store. It is atomic and idempotent per (id, key), where key
// is the Record Step by default or the WithIdempotencyKey value when supplied.
func (s *MemStore) Append(_ context.Context, id InstanceID, rec Record, opts ...AppendOption) (int64, error) {
	cfg := resolveAppend(opts...)
	key := cfg.idempotencyKey
	if key == "" {
		key = fmt.Sprintf("step:%d", rec.Step)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	inst := s.instanceLocked(id)

	// Idempotency: a re-append of an already-applied key is a no-op returning the
	// original sequence. The first writer of the key wins.
	if existing, ok := inst.applied[key]; ok {
		return existing, nil
	}

	// Strict per-instance Step ordering: a new Step must exceed every recorded
	// Step. (Idempotent re-appends are handled above and never reach here.)
	if rec.Step <= inst.maxStep {
		return 0, fmt.Errorf("%w: step %d is not greater than recorded step %d for instance %q",
			ErrStepOutOfOrder, rec.Step, inst.maxStep, id)
	}

	inst.tail = append(inst.tail, cloneRecord(rec))
	if s.cfg.history {
		inst.history = append(inst.history, cloneRecord(rec))
	}
	inst.maxStep = rec.Step
	inst.seq++
	inst.applied[key] = inst.seq
	return inst.seq, nil
}

// Load implements Store. It returns the latest checkpoint plus the post-
// checkpoint Record tail in Step order, or ErrInstanceNotFound.
func (s *MemStore) Load(_ context.Context, id InstanceID) ([]byte, []Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inst, ok := s.instances[id]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %q", ErrInstanceNotFound, id)
	}

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

// History implements HistoryStore for a MemStore constructed WithHistory: it returns
// the instance's start baseline snapshot and its full ordered Record log, including
// Records a checkpoint compacted out of the live tail, so a time-travel read can
// reconstruct any recorded step. Without WithHistory the baseline is nil and the
// returned log is the live (compacted) tail, sufficient only for steps at or after
// the latest checkpoint. It reports ErrInstanceNotFound for an unknown instance.
func (s *MemStore) History(_ context.Context, id InstanceID) ([]byte, []Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inst, ok := s.instances[id]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %q", ErrInstanceNotFound, id)
	}

	var baseline []byte
	src := inst.history
	if !s.cfg.history {
		// History was not retained; fall back to the live view (latest checkpoint
		// baseline plus the compacted tail), which reaches post-checkpoint steps.
		baseline = cloneBytes(inst.checkpoint)
		src = inst.tail
	} else {
		baseline = cloneBytes(inst.baseline)
	}

	records := make([]Record, len(src))
	for i, rec := range src {
		records[i] = cloneRecord(rec)
	}
	return baseline, records, nil
}

// Checkpoint implements Store. It persists snapshot as the instance's checkpoint at
// throughStep and compacts the tail through that step. throughStep must advance
// beyond the current checkpoint. Time-travel retention is a store-level capability
// (NewMemStore with WithHistory), so the per-checkpoint CheckpointOptions do not
// change what Load returns here.
func (s *MemStore) Checkpoint(_ context.Context, id InstanceID, snapshot []byte, throughStep int, opts ...CheckpointOption) error {
	_ = resolveCheckpoint(opts...) // no per-checkpoint option defined; history is store-level (WithHistory)

	s.mu.Lock()
	defer s.mu.Unlock()

	inst := s.instanceLocked(id)

	if throughStep <= inst.throughStep {
		return fmt.Errorf("%w: throughStep %d does not advance past checkpoint step %d for instance %q",
			ErrCheckpointNotAdvancing, throughStep, inst.throughStep, id)
	}

	// Capture the start baseline snapshot once, when history is retained: it is the
	// BaselineStep checkpoint, and a later checkpoint would otherwise overwrite the
	// only copy a time-travel read needs to restore the pre-event cast from.
	if s.cfg.history && inst.baseline == nil && throughStep == baselineStep {
		inst.baseline = append([]byte(nil), snapshot...)
	}

	// Split the tail at throughStep: keep Records strictly after it. The compacted
	// prefix is discarded from the live tail; WithHistory preserves it independently
	// in the full history log for time-travel reads.
	kept := inst.tail[:0:0]
	for _, rec := range inst.tail {
		if rec.Step <= throughStep {
			continue
		}
		kept = append(kept, rec)
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
// for the instance, so a subsequent (re)dispatch skips them. It is atomic and
// idempotent: re-marking an already-marked id is a no-op, and a partially marked
// batch is completed without error. It satisfies the DispatchStore seam.
func (s *MemStore) MarkDispatched(_ context.Context, id InstanceID, effectIDs ...string) error {
	if len(effectIDs) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	inst := s.instanceLocked(id)
	for _, eid := range effectIDs {
		inst.dispatched[eid] = struct{}{}
	}
	return nil
}

// Dispatched returns the set of effect ids already applied for the instance, as
// a membership map. An instance never written reports an empty (non-nil) set
// rather than an error: the dedup query is a pure read of "what has landed",
// orthogonal to whether any Record exists yet. It satisfies the DispatchStore
// seam.
func (s *MemStore) Dispatched(_ context.Context, id InstanceID) (map[string]bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inst, ok := s.instances[id]
	if !ok {
		return map[string]bool{}, nil
	}
	out := make(map[string]bool, len(inst.dispatched))
	for eid := range inst.dispatched {
		out[eid] = true
	}
	return out, nil
}

// cloneBytes returns a copy of b, or nil for a nil input, so a returned snapshot is
// insulated from later mutation of the caller's slice (and vice versa).
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}

// cloneRecord returns a deep-enough copy of rec so a stored Record is insulated
// from later mutation of the caller's slices (and vice versa on Load). The
// json.RawMessage payloads within entries/effects are immutable by convention,
// so the slice headers are copied without cloning each payload's bytes.
func cloneRecord(rec Record) Record {
	out := Record{Step: rec.Step, Tick: rec.Tick, TickSteps: rec.TickSteps}
	if rec.Event != nil {
		out.Event = append([]byte(nil), rec.Event...)
	}
	if len(rec.Entries) > 0 {
		out.Entries = append(out.Entries, rec.Entries...)
	}
	if len(rec.Effects) > 0 {
		out.Effects = append(out.Effects, rec.Effects...)
	}
	if rec.Snapshot != nil {
		out.Snapshot = append([]byte(nil), rec.Snapshot...)
	}
	return out
}
