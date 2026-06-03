// SPDX-License-Identifier: Apache-2.0

package dlq

import (
	"context"
	"strconv"
	"sync"

	"github.com/stablekernel/crucible/source"
)

// MemDeadLetter is an in-memory dead-letter store that is both a [DeadLetter]
// sink and a replayable [source.Inlet]: the concrete realization of "the DLQ is
// itself an Inlet". Park terminal failures into it through the dead-letter
// [Middleware], then drain the parked records back through the same
// [source.Handler] — with each replayed message starting at attempt 1 again — by
// subscribing to it like any other inlet. It exists for tests and for small
// single-process replay; production backends supply their own topic/table-backed
// [DeadLetter] and a matching replay [source.Inlet].
//
// The zero value is not usable; construct one with [NewMemDeadLetter]. It is safe
// for concurrent Park and a single draining [source.Subscription].
type MemDeadLetter struct {
	mu      sync.Mutex
	records []DeadLetterRecord
}

// NewMemDeadLetter returns an empty in-memory dead-letter store.
func NewMemDeadLetter() *MemDeadLetter {
	return &MemDeadLetter{}
}

// Park appends rec to the store. It implements [DeadLetter] and never fails.
func (d *MemDeadLetter) Park(_ context.Context, rec DeadLetterRecord) error {
	d.mu.Lock()
	d.records = append(d.records, rec)
	d.mu.Unlock()
	return nil
}

// Records returns a snapshot copy of the parked records, for assertions. The
// returned slice is independent of the store.
func (d *MemDeadLetter) Records() []DeadLetterRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DeadLetterRecord, len(d.records))
	copy(out, d.records)
	return out
}

// Len reports how many records are currently parked.
func (d *MemDeadLetter) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.records)
}

// Subscribe drains the parked records as a [source.Subscription]: each parked
// record is reconstituted into a [source.Message] and yielded once, in park
// order, after which Next reports [source.ErrDrained]. The cfg is accepted for
// interface symmetry but ignored — a parking store has no topics or groups to
// honor. Draining is destructive: a record settled with [source.ActionAck] (a
// successful replay) is consumed; a record settled with [source.ActionNak] or
// [source.ActionTerm] is re-parked so a still-failing replay is not lost.
//
// This is the replay path: wire the returned subscription to a Hopper running the
// same handler, and the parking topic re-flows through the pipeline with attempt
// counts reset (the replayed message carries no retry attempt, so it is attempt
// 1 again).
func (d *MemDeadLetter) Subscribe(_ context.Context, _ source.SubscribeConfig) (source.Subscription, error) {
	d.mu.Lock()
	snapshot := make([]DeadLetterRecord, len(d.records))
	copy(snapshot, d.records)
	d.records = nil
	d.mu.Unlock()

	return &memSubscription{store: d, pending: snapshot}, nil
}

// Close releases the inlet. The in-memory store holds nothing to release.
func (d *MemDeadLetter) Close() error { return nil }

// memSubscription drains a snapshot of parked records taken at Subscribe time.
type memSubscription struct {
	store   *MemDeadLetter
	mu      sync.Mutex
	pending []DeadLetterRecord
	idx     int
	closed  bool
}

// Next yields the next parked record as a replayable [source.Message], or
// [source.ErrDrained] once the snapshot is exhausted or the subscription closed.
func (s *memSubscription) Next(ctx context.Context) (source.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.idx >= len(s.pending) {
		return nil, source.ErrDrained
	}
	rec := s.pending[s.idx]
	s.idx++
	return &replayMessage{rec: rec, seq: s.idx}, nil
}

// Settle consumes a successfully-replayed record (Ack) or re-parks one whose
// replay still failed (Nak/Term), so a flapping message survives the round-trip.
func (s *memSubscription) Settle(ctx context.Context, m source.Message, r source.Result) error {
	rm, ok := m.(*replayMessage)
	if !ok {
		return nil
	}
	switch r.Action {
	case source.ActionNak, source.ActionTerm:
		return s.store.Park(ctx, rm.rec)
	default:
		return nil
	}
}

// Close marks the subscription drained.
func (s *memSubscription) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// replayMessage adapts a parked [DeadLetterRecord] back onto [source.Message] so
// it can re-flow through a Hopper. It carries the original payload, headers,
// subject, and partition key; the retry attempt counter is deliberately absent,
// so a replayed message is attempt 1 again.
type replayMessage struct {
	rec DeadLetterRecord
	seq int
}

func (m *replayMessage) Key() []byte             { return m.rec.Key }
func (m *replayMessage) Value() []byte           { return m.rec.Value }
func (m *replayMessage) Headers() source.Headers { return m.rec.Headers }
func (m *replayMessage) Subject() string         { return m.rec.Subject }
func (m *replayMessage) PartitionKey() string    { return m.rec.PartitionKey }
func (m *replayMessage) Cursor() source.Cursor   { return replayCursor(m.seq) }

// As assigns the underlying [DeadLetterRecord] to target if it is a
// *DeadLetterRecord, the escape hatch for a replay handler that wants the parked
// metadata (reason, attempts, last error) alongside the payload.
func (m *replayMessage) As(target any) bool {
	if p, ok := target.(*DeadLetterRecord); ok {
		*p = m.rec
		return true
	}
	return false
}

// replayCursor is the stream-local position of a replayed record: its 1-based
// index within the drained snapshot.
type replayCursor int

func (c replayCursor) String() string { return "dlq-replay-" + strconv.Itoa(int(c)) }

var (
	_ DeadLetter          = (*MemDeadLetter)(nil)
	_ source.Inlet        = (*MemDeadLetter)(nil)
	_ source.Subscription = (*memSubscription)(nil)
	_ source.Message      = (*replayMessage)(nil)
)
