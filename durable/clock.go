package durable

import (
	"sync"
	"time"

	"github.com/stablekernel/crucible/state"
)

// This file implements the clock record/replay seam: the smallest, fully
// deterministic nondeterminism source. A running instance reads time only through
// its host driver (the state.Scheduler armed and ticked by the durable Handle) —
// the kernel's Fire step never reads a clock. The Runner therefore wraps the real
// state.Clock in a recordingClock on the live path and a replayClock on recovery,
// so every Now() reading the scheduler consumes is journaled and returned
// verbatim on replay. This makes timer firing wall-clock-independent: a recovered
// instance re-derives the same deadlines and fires the same timers at the same
// recorded instants, regardless of the wall clock at recovery time.
//
// # What triggers a clock read
//
// The kernel emits ScheduleAfter / CancelScheduled effects as pure data; it reads
// no clock. The state.Scheduler reads Clock.Now() when it absorbs those effects
// (to compute a timer's absolute deadline, now+Delay) and when it ticks (to test
// which deadlines are due). Each such read is the unit the recordingClock
// journals.
//
// # How entries correlate to steps
//
// Clock reads are NOT one-per-Fire: a single Absorb or Tick may read once, and a
// step may absorb several effects. The recordingClock accumulates reads into a
// shared buffer the Handle flushes into the Record.Entries of the step during
// which they occurred (the step whose Fire/Tick produced them), in read order.
// Replay is order-driven, not step-keyed: the replayClock walks every recorded
// JournalClockRead across the loaded tail in recorded order and returns each
// reading once, so the scheduler re-derives identical deadlines.

// recordingClock wraps a real state.Clock and records every Now() reading as a
// JournalClockRead into a shared buffer, in read order. After delegates to the
// wrapped clock unchanged (the scheduler drives elapses through Now + Tick, not
// After, so After need not be recorded). It is concurrency-safe so a host driver
// may read it from its own timer goroutine.
type recordingClock struct {
	mu   sync.Mutex
	base state.Clock
	buf  *[]state.JournalEntry // shared accumulator the Handle flushes per step
}

// newRecordingClock wraps base, journaling each Now() reading into buf.
func newRecordingClock(base state.Clock, buf *[]state.JournalEntry) *recordingClock {
	return &recordingClock{base: base, buf: buf}
}

// Now reads the base clock, records the reading as a JournalClockRead, and
// returns the real value, so the live run behaves exactly as an unwrapped clock
// would while building the replay journal.
func (c *recordingClock) Now() time.Time {
	t := c.base.Now()
	c.mu.Lock()
	*c.buf = append(*c.buf, state.JournalEntry{
		Kind:          state.JournalClockRead,
		ClockUnixNano: t.UnixNano(),
	})
	c.mu.Unlock()
	return t
}

// After delegates to the wrapped clock; the scheduler drives timer elapses
// through Now + Tick, so After carries no recorded nondeterminism.
func (c *recordingClock) After(d time.Duration) <-chan time.Time { return c.base.After(d) }

// replayClock returns recorded clock readings in order instead of reading any
// real clock, so a recovered instance's scheduler re-derives identical timer
// deadlines without consulting the wall clock. It is the inverse of
// recordingClock: Now advances a cursor over the recorded JournalClockRead
// readings and returns each once.
//
// When the cursor is exhausted — a Now() beyond the recorded readings, which
// happens on the live re-arm after replay — it falls through to a real clock so
// post-recovery timing continues against wall time. The fallthrough never affects
// the replayed prefix, which is fully determined by the journal.
type replayClock struct {
	mu       sync.Mutex
	readings []int64
	cursor   int
	fallback state.Clock
}

// newReplayClock builds a replayClock over the recorded readings, falling through
// to fallback once the readings are exhausted (the live re-arm after replay).
func newReplayClock(readings []int64, fallback state.Clock) *replayClock {
	return &replayClock{readings: readings, fallback: fallback}
}

// Now returns the next recorded reading, or the fallback clock's reading once the
// recorded readings are exhausted.
func (c *replayClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cursor < len(c.readings) {
		t := time.Unix(0, c.readings[c.cursor]).UTC()
		c.cursor++
		return t
	}
	return c.fallback.Now()
}

// After delegates to the fallback clock; replay drives elapses through Now + Tick
// over the recorded readings, so After is never relied on during replay.
func (c *replayClock) After(d time.Duration) <-chan time.Time { return c.fallback.After(d) }

// hasTimerEffect reports whether effects contains a delayed-transition schedule
// or cancel — the only effects a state.Scheduler.Absorb acts on. The durable
// Handle skips Absorb (and thus the Scheduler's unconditional clock read) for a
// step that armed or canceled no timer, so a purely event-driven machine records
// no clock reads.
func hasTimerEffect(effects []state.Effect) bool {
	for _, eff := range effects {
		switch eff.(type) {
		case state.ScheduleAfter, state.CancelScheduled:
			return true
		}
	}
	return false
}

// clockReadings extracts the recorded clock readings from a loaded tail, in
// recorded order (Record order, then entry order within each Record), so a
// replayClock returns them in exactly the order the live run read them.
func clockReadings(tail []Record) []int64 {
	var out []int64
	for i := range tail {
		for _, e := range tail[i].Entries {
			if e.Kind == state.JournalClockRead {
				out = append(out, e.ClockUnixNano)
			}
		}
	}
	return out
}
