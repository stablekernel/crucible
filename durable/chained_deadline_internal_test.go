package durable

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// steppingClock advances one millisecond on every read while stepping, so each read in a
// Tick is a distinct instant, as a real clock's are; with stepping off it holds still.
type steppingClock struct {
	mu       sync.Mutex
	now      time.Time
	stepping bool
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.now
	if c.stepping {
		c.now = c.now.Add(time.Millisecond)
	}
	return t
}

func (c *steppingClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

func (c *steppingClock) set(t time.Time, stepping bool) {
	c.mu.Lock()
	c.now, c.stepping = t, stepping
	c.mu.Unlock()
}

// TestTick_RecordsAChainedTimersDeadlineAsTheSchedulerArmedIt pins that the deadline the
// Handle records for a chained timer, which a checkpoint persists and Recover re-arms,
// is the one the scheduler armed it at: the scheduler must not fire the timer before the
// recorded deadline.
func TestTick_RecordsAChainedTimersDeadlineAsTheSchedulerArmedIt(t *testing.T) {
	ctx := context.Background()
	type cx struct{}
	m := state.ForgeFor[cx]("chain").
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
	clk := &steppingClock{now: base}
	h, err := NewRunner(m, NewMemStore(), WithRunnerClock[string, string, cx](clk)).
		Start(ctx, "chain", cx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	// Tick with a moving clock: the timer into b fires, and b's timer is chained.
	clk.set(base.Add(2*time.Second), true)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := h.inst.Current(); got != "b" {
		t.Fatalf("state = %q, want b", got)
	}
	var recorded time.Time
	for _, pt := range h.timers {
		recorded = pt.due
	}
	if len(h.timers) != 1 {
		t.Fatalf("timer table holds %d timers, want b's alone", len(h.timers))
	}
	// Just before the recorded deadline, the scheduler must not consider b's timer due.
	clk.set(recorded.Add(-time.Millisecond), false)
	if fired := h.sched.Tick(ctx); len(fired) != 0 {
		t.Fatalf("the scheduler fired b's timer at %v, before the recorded deadline %v: a checkpoint would re-arm it later than live", recorded.Add(-time.Millisecond), recorded)
	}
}

// TestTick_RecordsEachChainedDeadlineInATwoTimerTick pins the same for two timers that
// fire in one Tick, each chaining another, and that Recover's replay of that Tick ends
// with the same timer table as the live Handle.
func TestTick_RecordsEachChainedDeadlineInATwoTimerTick(t *testing.T) {
	ctx := context.Background()
	type cx struct{}
	m := state.ForgeFor[cx]("pair").
		State("idle").
		SuperState("both").
		Region("left").
		Initial("l1").
		SubState("l1").
		SubState("l2").
		EndRegion().
		Region("right").
		Initial("r1").
		SubState("r1").
		SubState("r2").
		EndRegion().
		EndSuperState().
		Initial("idle").
		Transition("idle").On("go").GoTo("both").
		Transition("l1").After(time.Second).On("toL2").GoTo("l2").
		Transition("l2").After(time.Second).On("lDone").GoTo("l2").
		Transition("r1").After(time.Second).On("toR2").GoTo("r2").
		Transition("r2").After(time.Second).On("rDone").GoTo("r2").
		Quench()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := &steppingClock{now: base}
	store := NewMemStore()
	opts := []Option[string, string, cx]{WithRunnerClock[string, string, cx](clk)}
	h, err := NewRunner(m, store, opts...).Start(ctx, "pair", cx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	clk.set(base.Add(2*time.Second), true)
	fired, err := h.Tick(ctx)
	if err != nil || len(fired) != 2 {
		t.Fatalf("Tick fired %d timers, err %v; want both", len(fired), err)
	}
	if len(h.timers) != 2 {
		t.Fatalf("timer table holds %d timers, want the two chained ones", len(h.timers))
	}
	earliest := time.Time{}
	for _, pt := range h.timers {
		if earliest.IsZero() || pt.due.Before(earliest) {
			earliest = pt.due
		}
	}
	clk.set(earliest.Add(-time.Millisecond), false)
	if got := h.sched.Tick(ctx); len(got) != 0 {
		t.Fatalf("the scheduler fired a chained timer before its recorded deadline %v", earliest)
	}
	live := map[string]time.Time{}
	for id, pt := range h.timers {
		live[id] = pt.due
	}
	recovered, err := Recover(ctx, m, store, "pair", WithRunnerClock[string, string, cx](state.NewFakeClock(base.Add(time.Hour))))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(recovered.timers) != len(live) {
		t.Fatalf("recovered timer table holds %d timers, want %d", len(recovered.timers), len(live))
	}
	for id, due := range live {
		if got := recovered.timers[id].due; !got.Equal(due) {
			t.Fatalf("recovered deadline for %s = %v, want the live %v", id, got, due)
		}
	}
}

// TestTickArmReads_FailsOnAMissingRead pins that Tick and its replay fail, rather than
// fall back to another read, when a fired timer's arming read is missing: the
// scheduler's order of reads changed, and any other deadline would be wrong.
func TestTickArmReads_FailsOnAMissingRead(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	reads := []time.Time{base, base.Add(time.Millisecond), base.Add(2 * time.Millisecond)}
	reading := func(i int) (time.Time, bool) {
		if i < 0 || i >= len(reads) {
			return time.Time{}, false
		}
		return reads[i], true
	}
	got, err := tickArmReads(reading, 0, 1)
	if err != nil || len(got) != 1 || !got[0].Equal(reads[1]) {
		t.Fatalf("one fired timer = %v, %v; want the absorb read %v", got, err, reads[1])
	}
	if _, err := tickArmReads(reading, 0, 2); err == nil {
		t.Fatal("two fired timers with three reads: want an error for the second timer's missing arming read")
	}
}
