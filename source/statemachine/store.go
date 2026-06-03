// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"context"
	"errors"
	"sync"

	"github.com/stablekernel/crucible/state"
)

// ErrConflict reports that a [Store.Save] was rejected because the persisted
// version no longer matched the expected version the caller loaded: another
// writer advanced the same key concurrently. It is a transient,
// retry-by-reloading condition — [Drive] surfaces it as [source.Nak] so the
// message is redelivered and re-applied against the now-current state. Match it
// with errors.Is.
var ErrConflict = errors.New("statemachine: store version conflict")

// Record is the durable form of a state-machine instance the [Store] persists:
// a [state.Snapshot] (the lossless instance state), the monotonic Version that
// advances by one on every applied transition, and LastEventID, the id of the
// most recently applied message. Version and LastEventID together are the
// exactly-once dedup key — a redelivered message whose id equals LastEventID
// was already folded into Version, so it is skipped rather than re-fired.
//
// A Record round-trips through JSON whenever its [state.Snapshot] does (a
// JSON-marshalable context C, or a context codec on the store side), so a Store
// may persist it verbatim.
type Record[S comparable, E comparable, C any] struct {
	// Snapshot is the instance's persisted state, restorable with
	// [state.Machine.Restore].
	Snapshot state.Snapshot[S, E, C] `json:"snapshot"`
	// Version is the monotonic sequence advanced once per applied transition,
	// starting at zero for a never-fired instance. It is the optimistic-concurrency
	// token a [Store.Save] checks against the expected version.
	Version int64 `json:"version"`
	// LastEventID is the id of the most recently applied inbound message (empty for
	// a never-fired instance). A redelivery carrying this id is a no-op ack.
	LastEventID string `json:"lastEventId,omitempty"`
}

// Store is the durable seam [Drive] depends on to load and persist a
// state-machine instance, keyed by an instance key K. It is deliberately small
// so the bridge never hard-depends on a concrete backend: the crucible/durable
// module, a SQL store, or any key/value store can supply an adapter, and
// [NewMemStore] provides an in-memory implementation for tests.
//
// Implementations must be safe for concurrent use across keys; [Drive] serializes
// load→fire→save per key through the [source.Hopper]'s ordered lanes, but
// different keys are driven in parallel.
type Store[K comparable, S comparable, E comparable, C any] interface {
	// Load returns the persisted [Record] for key and whether one exists. A missing
	// key returns ok=false and a nil error so [Drive] can fire a fresh instance from
	// its initial state; a backend failure returns a non-nil error, which [Drive]
	// treats as transient (nak).
	Load(ctx context.Context, key K) (rec Record[S, E, C], ok bool, err error)
	// Save persists rec for key under optimistic concurrency: expectedVersion is the
	// version the caller loaded (zero for a first write). A Save whose persisted
	// version no longer matches expectedVersion must return an error matching
	// [ErrConflict]; any other failure is a transient backend error. On success the
	// persisted version becomes rec.Version.
	Save(ctx context.Context, key K, rec Record[S, E, C], expectedVersion int64) error
}

// MemStore is an in-memory [Store] for tests and single-process use. It enforces
// the same optimistic-concurrency contract as a durable backend, returning
// [ErrConflict] when a Save's expected version does not match the stored version,
// so the exactly-once and conflict paths are exercised without infrastructure. It
// is safe for concurrent use.
type MemStore[K comparable, S comparable, E comparable, C any] struct {
	mu      sync.Mutex
	records map[K]Record[S, E, C]
}

// NewMemStore returns an empty in-memory [Store].
func NewMemStore[K comparable, S comparable, E comparable, C any]() *MemStore[K, S, E, C] {
	return &MemStore[K, S, E, C]{records: make(map[K]Record[S, E, C])}
}

// Load returns the stored record for key and whether one exists.
func (s *MemStore[K, S, E, C]) Load(_ context.Context, key K) (Record[S, E, C], bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[key]
	return rec, ok, nil
}

// Save persists rec for key, rejecting a stale write with [ErrConflict] when the
// stored version no longer matches expectedVersion.
func (s *MemStore[K, S, E, C]) Save(_ context.Context, key K, rec Record[S, E, C], expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.records[key]; ok {
		if cur.Version != expectedVersion {
			return ErrConflict
		}
	} else if expectedVersion != 0 {
		return ErrConflict
	}
	s.records[key] = rec
	return nil
}
