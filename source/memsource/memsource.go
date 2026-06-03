// SPDX-License-Identifier: Apache-2.0

// Package memsource is an in-memory, deterministic [source.Inlet] for tests.
// It is the zero-infra substrate the rest of the suite leans on: scripted
// messages drive a [source.Hopper] with no broker, an injected clock and ID
// function make every run reproducible, and every settle is recorded so a test
// can assert exactly which messages were acked, nak'd, termed, or dropped.
//
// The killer feature — drive a statechart from a stream, ack on a durable
// transition — is therefore unit-testable: feed an [Inlet], run a Hopper, and
// read the [Ledger].
package memsource

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/stablekernel/crucible/source"
)

// Msg is a programmable inbound message: the scripted input a test feeds an
// [Inlet]. All fields are optional; an empty Msg is a keyless, headerless,
// empty-payload message on the default subject.
type Msg struct {
	// Key is the routing/partition key. The Inlet hashes it into a PartitionKey
	// when PartitionKey is empty, so messages sharing a Key share an ordered lane.
	Key string
	// Value is the raw payload bytes the codec decodes.
	Value []byte
	// Headers are the message's metadata.
	Headers source.Headers
	// Subject is the topic/subject; defaults to "memsource" when empty.
	Subject string
	// PartitionKey overrides the ordering domain; when empty the Inlet derives one
	// from Key (so same-Key messages stay ordered) or leaves it empty.
	PartitionKey string
}

// Option configures an [Inlet].
type Option func(*Inlet)

// WithClock injects the clock the Inlet stamps cursors and records settle times
// with, for deterministic tests. The default is time.Now. A nil clock is
// ignored.
func WithClock(now func() time.Time) Option {
	return func(in *Inlet) {
		if now != nil {
			in.now = now
		}
	}
}

// WithIDFunc injects the function the Inlet assigns sequential message IDs and
// cursor labels from. It is called once per message; the default is a monotonic
// counter rendered as a decimal string. A nil func is ignored.
func WithIDFunc(next func() string) Option {
	return func(in *Inlet) {
		if next != nil {
			in.nextID = next
		}
	}
}

// Inlet is an in-memory [source.Inlet]. Messages queued with [Inlet.Queue] (or
// at construction with [WithMessages]) are delivered in order by the
// [source.Subscription] it opens; once the queue empties and the subscription is
// closed, [source.Subscription.Next] returns [source.ErrDrained]. Every settle is
// recorded on the shared [Ledger].
//
// Inlet is safe for concurrent use; a single Subscribe is the expected pattern
// (the Hopper drives one subscription), and a second Subscribe shares the same
// queue and ledger.
type Inlet struct {
	now    func() time.Time
	nextID func() string

	mu      sync.Mutex
	queue   []source.Message
	ledger  *Ledger
	subs    []*subscription
	counter int64
}

// New constructs an Inlet with the given options. With no options it uses
// time.Now and a monotonic decimal ID counter, and starts with an empty queue.
func New(opts ...Option) *Inlet {
	in := &Inlet{
		now:    time.Now,
		ledger: newLedger(),
	}
	in.nextID = func() string {
		in.counter++
		return strconv.FormatInt(in.counter, 10)
	}
	for _, o := range opts {
		o(in)
	}
	return in
}

// WithMessages is an [Option] that pre-queues msgs at construction, equivalent
// to a subsequent [Inlet.Queue].
func WithMessages(msgs ...Msg) Option {
	return func(in *Inlet) {
		for _, m := range msgs {
			in.queue = append(in.queue, in.build(m))
		}
	}
}

// Queue appends messages to the Inlet's delivery queue. It is safe to call
// before or during consumption; messages are delivered in queue order.
func (in *Inlet) Queue(msgs ...Msg) {
	in.mu.Lock()
	defer in.mu.Unlock()
	for _, m := range msgs {
		in.queue = append(in.queue, in.build(m))
	}
	for _, s := range in.subs {
		s.notify()
	}
}

// build materializes a Msg into a Message, stamping it with an ID and cursor.
// The caller holds in.mu, except during construction options where no
// subscription exists yet.
func (in *Inlet) build(m Msg) source.Message {
	id := in.nextID()
	subject := m.Subject
	if subject == "" {
		subject = "memsource"
	}
	pk := m.PartitionKey
	if pk == "" && m.Key != "" {
		pk = m.Key
	}
	return &message{
		id:           id,
		key:          []byte(m.Key),
		value:        m.Value,
		headers:      m.Headers,
		subject:      subject,
		partitionKey: pk,
		cursor:       cursor(id),
	}
}

// Subscribe opens a [source.Subscription] over the Inlet's queue. cfg is
// accepted for interface conformance but not otherwise interpreted (a single
// in-memory stream has no topics or groups to honor).
func (in *Inlet) Subscribe(_ context.Context, _ source.SubscribeConfig) (source.Subscription, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	s := &subscription{inlet: in, signal: make(chan struct{}, 1)}
	in.subs = append(in.subs, s)
	return s, nil
}

// Close releases the Inlet. It is a no-op for the in-memory implementation; the
// queue and ledger remain readable for post-run assertions.
func (in *Inlet) Close() error { return nil }

// Ledger returns the shared settle ledger, the record of every Settle the
// subscription applied. Read it after a run to assert outcomes.
func (in *Inlet) Ledger() *Ledger { return in.ledger }

// take pops the next queued message, or reports the queue empty.
func (in *Inlet) take() (source.Message, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if len(in.queue) == 0 {
		return nil, false
	}
	m := in.queue[0]
	in.queue = in.queue[1:]
	return m, true
}

var _ source.Inlet = (*Inlet)(nil)
