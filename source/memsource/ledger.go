// SPDX-License-Identifier: Apache-2.0

package memsource

import (
	"sync"
	"time"

	"github.com/stablekernel/crucible/source"
)

// Entry is one recorded settlement: the message ID, the [source.Result] the
// Hopper applied to it, and the time it settled (read from the inlet's injected
// clock). Entries are recorded in settle order, which for a single ordered lane
// is the message's delivery order.
type Entry struct {
	// ID is the message's sequential ID (from the inlet's ID func).
	ID string
	// Result is the disposition applied to the message.
	Result source.Result
	// At is the settle time, stamped from the inlet's injected clock.
	At time.Time
}

// Ledger records every [source.Subscription.Settle] the in-memory subscription
// applied, the assertion surface a test reads after a run. It is safe for
// concurrent use: the Hopper settles from per-lane worker goroutines.
type Ledger struct {
	mu      sync.Mutex
	entries []Entry
}

func newLedger() *Ledger { return &Ledger{} }

func (l *Ledger) record(id string, r source.Result, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, Entry{ID: id, Result: r, At: at})
}

// Entries returns a copy of every recorded settlement in settle order. The
// returned slice is the caller's to retain.
func (l *Ledger) Entries() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Entry(nil), l.entries...)
}

// Len returns the number of settlements recorded.
func (l *Ledger) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// Counts tallies the recorded settlements by outcome: acked (a plain ack),
// dropped (an ack classified Drop), nak'd, and termed. The four are mutually
// exclusive and sum to Len.
func (l *Ledger) Counts() Counts {
	l.mu.Lock()
	defer l.mu.Unlock()
	var c Counts
	for _, e := range l.entries {
		switch {
		case e.Result.Action == source.ActionAck && e.Result.Class == source.Drop:
			c.Dropped++
		case e.Result.Action == source.ActionAck:
			c.Acked++
		case e.Result.Action == source.ActionNak:
			c.Nak++
		case e.Result.Action == source.ActionTerm:
			c.Term++
		}
	}
	return c
}

// IDs returns the settled message IDs in settle order, the sequence a test
// asserts to confirm per-key in-order processing. Since IDs are assigned in
// queue order, an in-order lane settles them in ascending ID order.
func (l *Ledger) IDs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := make([]string, len(l.entries))
	for i, e := range l.entries {
		ids[i] = e.ID
	}
	return ids
}

// Counts is the tally [Ledger.Counts] returns.
type Counts struct {
	// Acked is plain successful acks.
	Acked int
	// Dropped is acks classified Drop.
	Dropped int
	// Nak is redelivery requests.
	Nak int
	// Term is permanent rejections (dead-letter).
	Term int
}
