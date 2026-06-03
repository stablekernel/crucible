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
// context is canceled, or the subscription is closed and drained.
func (s *subscription) Next(ctx context.Context) (source.Message, error) {
	for {
		if m, ok := s.inlet.take(); ok {
			s.mu.Lock()
			s.inFlight++
			s.mu.Unlock()
			return m, nil
		}

		s.mu.Lock()
		drained := s.closed && s.inFlight == 0
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
