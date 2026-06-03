// SPDX-License-Identifier: Apache-2.0

package memsource

import (
	"context"
	"sync"

	"github.com/stablekernel/crucible/source"
)

// message is the in-memory [source.Message]. It is read-only from a handler's
// view; settlement is recorded on the inlet's ledger, never on the message.
type message struct {
	id           string
	key          []byte
	value        []byte
	headers      source.Headers
	subject      string
	partitionKey string
	cursor       cursor
}

func (m *message) Key() []byte             { return m.key }
func (m *message) Value() []byte           { return m.value }
func (m *message) Headers() source.Headers { return m.headers }
func (m *message) Subject() string         { return m.subject }
func (m *message) PartitionKey() string    { return m.partitionKey }
func (m *message) Cursor() source.Cursor   { return m.cursor }

// As assigns the message to a **message target, the documented escape hatch. A
// test driving a Manual result reaches the message through it.
func (m *message) As(target any) bool {
	if p, ok := target.(**message); ok {
		*p = m
		return true
	}
	return false
}

// cursor is the opaque in-memory position: the message's sequential ID.
type cursor string

func (c cursor) String() string { return string(c) }

// subscription is the in-memory [source.Subscription]. Next pops the inlet's
// queue and blocks (until a Queue or Close) when it is empty; Settle records the
// outcome on the inlet's ledger; Close drains.
type subscription struct {
	inlet  *Inlet
	signal chan struct{}

	mu       sync.Mutex
	inFlight int
	closed   bool
}

// notify wakes a blocked Next after a Queue or Close.
func (s *subscription) notify() {
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

// Next returns the next queued message, blocking until one is queued, the
// context is canceled, or the subscription is closed and its queue is empty.
//
// Drain reports ErrDrained as soon as the subscription is closed and no queued
// message remains, independent of messages still in flight: settling those is the
// engine's drain responsibility (it finishes its lanes after the fetch loop
// stops), not a precondition for "nothing left to fetch". Gating drain on
// in-flight==0 would deadlock batch mode, where the engine holds a trailing
// partial batch unsettled until the fetch loop closes its lanes — which it cannot
// do while still blocked here waiting for that very batch to settle.
func (s *subscription) Next(ctx context.Context) (source.Message, error) {
	for {
		if m, ok := s.inlet.take(); ok {
			s.mu.Lock()
			s.inFlight++
			s.mu.Unlock()
			return m, nil
		}

		s.mu.Lock()
		drained := s.closed
		s.mu.Unlock()
		if drained {
			return nil, source.ErrDrained
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.signal:
			// re-check the queue
		}
	}
}

// Settle records r against m on the inlet's ledger and decrements the in-flight
// count, waking Next so a drained subscription can report ErrDrained.
func (s *subscription) Settle(_ context.Context, m source.Message, r source.Result) error {
	mm, _ := m.(*message)
	id := ""
	if mm != nil {
		id = mm.id
	}
	s.inlet.ledger.record(id, r, s.inlet.now())

	s.mu.Lock()
	if s.inFlight > 0 {
		s.inFlight--
	}
	s.mu.Unlock()
	s.notify()
	return nil
}

// Close begins a drain: once in-flight messages settle, Next returns
// ErrDrained. It is idempotent.
func (s *subscription) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.notify()
	return nil
}

var _ source.Subscription = (*subscription)(nil)

// batchedSubscription is a [subscription] that also satisfies [source.Batched],
// used to exercise the engine's whole-batch fetch path. It is opt-in via
// [WithBatched] so the default memsource subscription stays the honest
// per-message common path.
type batchedSubscription struct {
	*subscription
}

// NextBatch returns up to limit queued messages, blocking for at least one
// (delegating to Next) and then draining whatever else is queued without
// blocking, so the engine receives a backend-shaped batch.
func (b *batchedSubscription) NextBatch(ctx context.Context, limit int) ([]source.Message, error) {
	if limit < 1 {
		limit = 1
	}
	first, err := b.Next(ctx)
	if err != nil {
		return nil, err
	}
	msgs := []source.Message{first}
	for len(msgs) < limit {
		m, ok := b.inlet.take()
		if !ok {
			break
		}
		b.mu.Lock()
		b.inFlight++
		b.mu.Unlock()
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// SettleBatch settles every message in ms with r, recording each on the ledger.
func (b *batchedSubscription) SettleBatch(ctx context.Context, ms []source.Message, r source.Result) error {
	for _, m := range ms {
		if err := b.Settle(ctx, m, r); err != nil {
			return err
		}
	}
	return nil
}

var (
	_ source.Subscription = (*batchedSubscription)(nil)
	_ source.Batched      = (*batchedSubscription)(nil)
)
