package state_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// readLog is a Clock that logs who read it: each read gets the next instant.
type readLog struct {
	mu    sync.Mutex
	now   time.Time
	reads int
}

func (c *readLog) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	t := c.now
	c.now = c.now.Add(time.Millisecond)
	return t
}
func (c *readLog) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

// TestScheduler_Tick_ReadsTheClockOncePerCheckAndAbsorb pins the order of clock reads
// a Tick takes, which durable relies on to record a chained timer's deadline: one read
// to find each due timer, one to absorb that timer's effects, and a last read that
// finds none due. A chained timer is armed at its fire's absorb read.
func TestScheduler_Tick_ReadsTheClockOncePerCheckAndAbsorb(t *testing.T) {
	m := state.ForgeFor[*trec]("reads").
		State("idle").
		State("a").
		State("b").
		State("c").
		Initial("idle").
		Transition("idle").On("go").GoTo("a").
		Transition("a").After(time.Second).On("toB").GoTo("b").
		Transition("b").After(time.Second).On("toC").GoTo("c").
		Quench()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := &readLog{now: base}
	inst := m.Cast(&trec{}, state.WithInitialState("idle"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	sch.Absorb(context.Background(), inst.Fire(context.Background(), "go").Effects) // arms a's timer at base+1s
	clk.mu.Lock()
	clk.now, clk.reads = base.Add(2*time.Second), 0
	clk.mu.Unlock()
	// Reads, from 2s: 2.000 finds a's timer due, 2.001 absorbs its effects (arming b's
	// timer at 3.001), 2.002 finds nothing due.
	if fired := sch.Tick(context.Background()); len(fired) != 1 {
		t.Fatalf("Tick fired %d timers, want a's", len(fired))
	}
	if clk.reads != 3 {
		t.Fatalf("Tick read the clock %d times, want 3: a check, the absorb, a last check", clk.reads)
	}
	clk.mu.Lock()
	clk.now = base.Add(3*time.Second + 1*time.Millisecond - time.Nanosecond)
	clk.mu.Unlock()
	if fired := sch.Tick(context.Background()); len(fired) != 0 {
		t.Fatal("b's timer fired before 3.001s: it wasn't armed at the absorb read")
	}
	clk.mu.Lock()
	clk.now = base.Add(3*time.Second + time.Millisecond)
	clk.mu.Unlock()
	if fired := sch.Tick(context.Background()); len(fired) != 1 {
		t.Fatal("b's timer didn't fire at 3.001s, the absorb read plus its delay")
	}
}
