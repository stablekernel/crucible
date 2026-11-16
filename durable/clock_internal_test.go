package durable

import (
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// clockEpoch is a fixed instant for the internal clock-seam unit tests.
var clockEpoch = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

// TestRecordingClock_NowRecordsAndReturnsReal verifies a recording clock returns
// the real reading and journals it as a JournalClockRead.
func TestRecordingClock_NowRecordsAndReturnsReal(t *testing.T) {
	var buf []state.JournalEntry
	real := state.NewFakeClock(clockEpoch)
	c := newRecordingClock(real, &buf)

	got := c.Now()
	if !got.Equal(clockEpoch) {
		t.Fatalf("Now: want %v, got %v", clockEpoch, got)
	}
	if len(buf) != 1 {
		t.Fatalf("recorded entries: want 1, got %d", len(buf))
	}
	if buf[0].Kind != state.JournalClockRead {
		t.Fatalf("kind: want clockRead, got %q", buf[0].Kind)
	}
	if buf[0].ClockUnixNano != clockEpoch.UnixNano() {
		t.Fatalf("recorded reading: want %d, got %d", clockEpoch.UnixNano(), buf[0].ClockUnixNano)
	}
}

// TestRecordingClock_AfterDelegates confirms After delegates to the wrapped clock
// without recording anything.
func TestRecordingClock_AfterDelegates(t *testing.T) {
	var buf []state.JournalEntry
	real := state.NewFakeClock(clockEpoch)
	c := newRecordingClock(real, &buf)

	select {
	case got := <-c.After(time.Second):
		if !got.Equal(clockEpoch.Add(time.Second)) {
			t.Fatalf("After: want %v, got %v", clockEpoch.Add(time.Second), got)
		}
	default:
		t.Fatal("After channel did not deliver (FakeClock.After fires immediately)")
	}
	if len(buf) != 0 {
		t.Fatalf("After must record nothing, recorded %d", len(buf))
	}
}

// TestReplayClock_ReturnsRecordedThenFallsThrough confirms a replay clock returns
// the recorded readings in order, then falls through to its fallback once the
// readings are exhausted (the live re-arm after replay).
func TestReplayClock_ReturnsRecordedThenFallsThrough(t *testing.T) {
	readings := []int64{
		clockEpoch.UnixNano(),
		clockEpoch.Add(2 * time.Second).UnixNano(),
	}
	fallbackAt := clockEpoch.Add(100 * time.Hour)
	c := newReplayClock(readings, state.NewFakeClock(fallbackAt))

	if got := c.Now(); got.UnixNano() != readings[0] {
		t.Fatalf("first replay: want %d, got %d", readings[0], got.UnixNano())
	}
	if got := c.Now(); got.UnixNano() != readings[1] {
		t.Fatalf("second replay: want %d, got %d", readings[1], got.UnixNano())
	}
	// Exhausted: falls through to the fallback clock.
	if got := c.Now(); !got.Equal(fallbackAt) {
		t.Fatalf("fallthrough: want %v, got %v", fallbackAt, got)
	}
}

// TestReplayClock_AfterDelegatesToFallback confirms After delegates to the
// fallback clock.
func TestReplayClock_AfterDelegatesToFallback(t *testing.T) {
	c := newReplayClock(nil, state.NewFakeClock(clockEpoch))
	select {
	case got := <-c.After(time.Second):
		if !got.Equal(clockEpoch.Add(time.Second)) {
			t.Fatalf("After: want %v, got %v", clockEpoch.Add(time.Second), got)
		}
	default:
		t.Fatal("After channel did not deliver")
	}
}

// TestClockReadings_ExtractsInOrder confirms clockReadings flattens only the
// JournalClockRead entries from a tail, in Record-then-entry order.
func TestClockReadings_ExtractsInOrder(t *testing.T) {
	tail := []Record{
		{Step: 0, Entries: []state.JournalEntry{
			{Kind: state.JournalClockRead, ClockUnixNano: 1},
			{Kind: state.JournalServiceResult},
		}},
		{Step: 1, Entries: []state.JournalEntry{
			{Kind: state.JournalClockRead, ClockUnixNano: 2},
			{Kind: state.JournalClockRead, ClockUnixNano: 3},
		}},
	}
	got := clockReadings(tail)
	want := []int64{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("readings: want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reading %d: want %d, got %d", i, want[i], got[i])
		}
	}
}

// TestRecordingClock_ConcurrentNowAndDrain exercises the documented timer-goroutine
// pattern: a host drives the scheduler's clock reads from its own timer loop while
// the durable Handle drains the recording buffer to close steps. The recording
// clock's godoc promises concurrency safety for exactly this, so Now() (the timer
// goroutine) and drain/markBuf/armReadingAt (the Handle) must not race the shared
// buffer. Run under -race to catch a regression; the count assertion proves no
// reading is lost or double-counted across the interleaving.
func TestRecordingClock_ConcurrentNowAndDrain(t *testing.T) {
	var buf []state.JournalEntry
	c := newRecordingClock(state.NewFakeClock(clockEpoch), &buf)

	const reads = 5000
	var drained int
	done := make(chan struct{})

	// Timer goroutine: the scheduler reading the clock as it arms and ticks.
	go func() {
		for range reads {
			c.Now()
		}
		close(done)
	}()

	// Handle goroutine (this one): close steps by draining, and exercise the
	// mark/read-back pair absorbTimers uses, all under the same mutex Now() holds.
	for {
		before := c.markBuf()
		_, _ = c.armReadingAt(before - 1)
		drained += len(c.drain())
		select {
		case <-done:
			drained += len(c.drain()) // final flush of anything appended after the last drain
			if drained != reads {
				t.Fatalf("drained %d readings, want %d (a lost or double-counted reading signals a buffer race)", drained, reads)
			}
			return
		default:
		}
	}
}

// TestHasTimerEffect covers the schedule/cancel detection that gates Absorb.
func TestHasTimerEffect(t *testing.T) {
	if !hasTimerEffect([]state.Effect{state.ScheduleAfter{ID: "x"}}) {
		t.Fatal("ScheduleAfter must count as a timer effect")
	}
	if !hasTimerEffect([]state.Effect{state.CancelScheduled{ID: "x"}}) {
		t.Fatal("CancelScheduled must count as a timer effect")
	}
	if hasTimerEffect(nil) {
		t.Fatal("no effects must not count as a timer effect")
	}
}
