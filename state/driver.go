package state

import (
	"context"
	"sort"
	"sync"
	"time"
)

// This file ships the reusable host-driver harness for delayed (`after`)
// transitions. The kernel emits ScheduleAfter / CancelScheduled effects and stays
// pure; a Scheduler is the small, documented runtime that turns those effects
// into real timers and feeds the delayed event back through Fire. A production
// host wires SystemClock(); a test wires FakeClock and advances it manually, so
// `after` machines are fully deterministic.
//
// # Host-driver contract
//
// A host that wants `after` transitions to actually fire wraps its instance in a
// Scheduler and routes every Fire's effects through it:
//
//	sch := state.NewScheduler(inst)
//	res := inst.Fire(ctx, ev)
//	dispatch(res.Effects)      // your own effect dispatch
//	sch.Absorb(ctx, res.Effects) // arm/cancel timers from the same effects
//
// Absorb scans the effects for ScheduleAfter (arm a timer) and CancelScheduled
// (drop a timer). When a timer is due, the Scheduler calls Fire with the delayed
// event and recursively absorbs the resulting effects, so a chain of `after`
// states keeps scheduling correctly. A production driver runs Absorb's timers on
// real wall-clock goroutines; the deterministic FakeClock driver fires them only
// when the test advances the clock.

// pending is one armed delayed timer tracked by a Scheduler.
type pending[E comparable] struct {
	event E
	due   time.Time
}

// Scheduler is the reusable host-driver that turns the kernel's ScheduleAfter /
// CancelScheduled effects into real timers and re-fires delayed events through
// its instance. It is concurrency-safe. Construct one per instance with
// NewScheduler; drive it by passing each Fire's effects to Absorb. With a
// FakeClock it is fully deterministic — timers fire only when the test advances
// the clock via FakeClock.Advance.
type Scheduler[S comparable, E comparable, C any] struct {
	inst  *Instance[S, E, C]
	clock Clock

	mu      sync.Mutex
	pending map[string]pending[E]
}

// NewScheduler returns a Scheduler driving inst, reading the time seam wired to
// inst at Cast (WithClock). With a FakeClock the Scheduler is deterministic.
func NewScheduler[S comparable, E comparable, C any](inst *Instance[S, E, C]) *Scheduler[S, E, C] {
	return &Scheduler[S, E, C]{
		inst:    inst,
		clock:   inst.clock,
		pending: map[string]pending[E]{},
	}
}

// Absorb scans effects, arming a timer for each ScheduleAfter and dropping the
// timer for each CancelScheduled. It is how a host wires Fire's output back into
// the scheduler; call it with the effects of every Fire (including those the
// Scheduler itself triggers — Fire-on-elapse re-enters Absorb automatically).
// A ScheduleAfter whose Event is not the instance's event type is ignored, since
// the kernel cannot have produced it.
func (s *Scheduler[S, E, C]) Absorb(ctx context.Context, effects []Effect) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.absorbLocked(effects)
}

func (s *Scheduler[S, E, C]) absorbLocked(effects []Effect) {
	now := s.clock.Now()
	for _, eff := range effects {
		switch e := eff.(type) {
		case ScheduleAfter:
			ev, ok := e.Event.(E)
			if !ok {
				continue
			}
			s.pending[e.ID] = pending[E]{event: ev, due: now.Add(e.Delay)}
		case CancelScheduled:
			delete(s.pending, e.ID)
		}
	}
}

// Pending reports the number of armed (not-yet-fired, not-canceled) timers. A
// test asserts on it to confirm a timer was scheduled or auto-canceled on exit.
func (s *Scheduler[S, E, C]) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// HasPending reports whether a timer with the given schedule id is armed.
func (s *Scheduler[S, E, C]) HasPending(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pending[id]
	return ok
}

// Tick fires every timer whose due time is at or before the Scheduler clock's
// current time, in due-time order (ties broken by id for determinism). Each due
// timer is removed, then its delayed event is fired through the instance and the
// resulting effects are absorbed (so a chained `after` arms its successor). It
// returns the FireResults of the events it fired, in order. With a FakeClock a
// test calls FakeClock.Advance then Tick (or uses the Advance helper) to drive
// elapses deterministically; with SystemClock a host calls Tick from its own
// timer loop.
func (s *Scheduler[S, E, C]) Tick(ctx context.Context) []FireResult[S] {
	var out []FireResult[S]
	for {
		s.mu.Lock()
		now := s.clock.Now()
		id, p, ok := s.dueLocked(now)
		if ok {
			delete(s.pending, id)
		}
		s.mu.Unlock()
		if !ok {
			return out
		}
		res := s.inst.Fire(ctx, p.event)
		s.Absorb(ctx, res.Effects)
		out = append(out, res)
	}
}

// dueLocked returns the earliest due timer at or before now, ties broken by id.
func (s *Scheduler[S, E, C]) dueLocked(now time.Time) (string, pending[E], bool) {
	ids := make([]string, 0, len(s.pending))
	for id, p := range s.pending {
		if !p.due.After(now) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return "", pending[E]{}, false
	}
	sort.Slice(ids, func(a, b int) bool {
		pa, pb := s.pending[ids[a]], s.pending[ids[b]]
		if pa.due.Equal(pb.due) {
			return ids[a] < ids[b]
		}
		return pa.due.Before(pb.due)
	})
	return ids[0], s.pending[ids[0]], true
}

// FakeClock is a deterministic Clock for tests: time advances only when Advance
// is called. It implements Clock; pair it with a Scheduler (via WithClock at
// Cast) to drive `after` transitions with no real waiting. It is concurrency-safe.
//
// After behaves like time.After on the fake timeline: its channel stays empty
// until Advance moves the clock to or past the deadline. A host that selects on
// After (a silence timeout, a reconnect grace) sees it fire only when the test
// advances far enough. A waiter the clock never reaches never fires, and the
// clock holds it until an Advance does; there is no Stop.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	seq     uint64
	waiters []fakeWaiter
}

// fakeWaiter is one channel handed out by FakeClock.After that hasn't fired yet.
// seq records the order the waiters were armed in, to break ties between equal
// deadlines.
type fakeWaiter struct {
	due time.Time
	seq uint64
	ch  chan time.Time
}

// NewFakeClock returns a FakeClock starting at the given instant. The zero
// instant is fine; only relative advances matter for delayed transitions.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the fake clock's current instant.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that receives once the fake clock reaches the
// instant of the call plus d. The value sent is that deadline. A d of zero or
// less fires at once with the current instant, as time.After does. With no
// Advance far enough, a receive on the channel blocks forever. The Scheduler doesn't
// use After: it drives elapses through Now and Tick.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.seq++
	c.waiters = append(c.waiters, fakeWaiter{due: c.now.Add(d), seq: c.seq, ch: ch})
	return ch
}

// Advance moves the fake clock forward by d, then fires every After channel
// whose deadline the clock has now reached, earliest deadline first, and in
// the order they were armed when deadlines tie. A goroutine already blocked
// selecting on several of them receives from the earliest; a receiver that
// arrives after Advance returns finds them all ready, and select picks at
// random. After advancing, call
// Scheduler.Tick to fire any now-due timers.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	var due []fakeWaiter
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if w.due.After(c.now) {
			kept = append(kept, w)
			continue
		}
		due = append(due, w)
	}
	clear(c.waiters[len(kept):])
	c.waiters = kept
	sort.Slice(due, func(i, j int) bool {
		if !due[i].due.Equal(due[j].due) {
			return due[i].due.Before(due[j].due)
		}
		return due[i].seq < due[j].seq
	})
	// Each channel is buffered and receives exactly once, so these sends never
	// block while the lock is held.
	for _, w := range due {
		w.ch <- w.due
	}
}

// keep context imported for the driver contract's Fire symmetry.
var _ = context.Background
