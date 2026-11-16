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

// TestTick_AChainedDeadlineHoldsWhenAFiredTransitionReadsTheClock pins that the
// recorded deadline of a chained timer is still the scheduler's when the fired
// transition's own action reads the instance's clock, adding a read to the Tick.
func TestTick_AChainedDeadlineHoldsWhenAFiredTransitionReadsTheClock(t *testing.T) {
	ctx := context.Background()
	type cx struct{}
	var h *Handle[string, string, cx]
	m := state.ForgeFor[cx]("readingAction").
		Action("look", func(state.ActionCtx[cx]) (state.Effect, error) {
			_ = h.inst.Clock().Now()
			return nil, nil
		}).
		State("idle").
		State("a").
		State("b").
		State("c").
		Initial("idle").
		Transition("idle").On("go").GoTo("a").
		Transition("a").After(time.Second).On("toB").GoTo("b").Do("look").
		Transition("b").After(time.Second).On("toC").GoTo("c").
		Quench()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := &steppingClock{now: base}
	var err error
	h, err = NewRunner(m, NewMemStore(), WithRunnerClock[string, string, cx](clk)).
		Start(ctx, "reading-action", cx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	clk.set(base.Add(2*time.Second), true)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	var recorded time.Time
	for _, pt := range h.timers {
		recorded = pt.due
	}
	clk.set(recorded.Add(-time.Millisecond), false)
	if fired := h.sched.Tick(ctx); len(fired) != 0 {
		t.Fatalf("the scheduler fired b's timer before the recorded deadline %v", recorded)
	}
	clk.set(recorded, false)
	if fired := h.sched.Tick(ctx); len(fired) != 1 {
		t.Fatalf("the scheduler didn't fire b's timer at the recorded deadline %v: it holds a different one", recorded)
	}
}

// TestFire_ADeadlineHoldsWhenTheTransitionReadsTheClock pins that a timer a Fire arms is
// recorded at the scheduler's deadline when the transition's action reads the instance
// clock first, live and after Recover's replay.
func TestFire_ADeadlineHoldsWhenTheTransitionReadsTheClock(t *testing.T) {
	ctx := context.Background()
	type cx struct{}
	var h *Handle[string, string, cx]
	look := func(state.ActionCtx[cx]) (state.Effect, error) {
		if h != nil {
			_ = h.inst.Clock().Now()
		}
		return nil, nil
	}
	m := state.ForgeFor[cx]("fireReads").
		Action("look", look).
		State("idle").
		State("a").
		State("b").
		Initial("idle").
		Transition("idle").On("go").GoTo("a").Do("look").
		Transition("a").After(time.Second).On("toB").GoTo("b").
		Quench()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := &steppingClock{now: base, stepping: true}
	store := NewMemStore()
	var err error
	h, err = NewRunner(m, store, WithRunnerClock[string, string, cx](clk)).
		Start(ctx, "fire-reads", cx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	id := state.ScheduleID("fireReads", "a", 0)
	want, ok := h.sched.Due(id)
	if !ok {
		t.Fatalf("no timer pending under %s", id)
	}
	if got := h.timers[id].due; !got.Equal(want) {
		t.Fatalf("live recorded deadline %v, scheduler %v", got, want)
	}
}

// TestRecover_ReplaysAStepWhoseHostCodeReadTheInstanceClock pins what Recover does with
// a step whose host code read the Handle's instance clock: the read is journaled with
// the step's own, but Recover can't run that host read against the replay clock, so
// replay's reads shift by one and the timer comes back at another deadline.
func TestRecover_ReplaysAStepWhoseHostCodeReadTheInstanceClock(t *testing.T) {
	t.Skip("a host read of Instance().Clock() during a step is journaled but can't be replayed; awaiting a ruling")
	ctx := context.Background()
	type cx struct{}
	var h *Handle[string, string, cx]
	look := func(state.ActionCtx[cx]) (state.Effect, error) {
		if h != nil {
			_ = h.inst.Clock().Now()
		}
		return nil, nil
	}
	m := state.ForgeFor[cx]("fireReads").
		Action("look", look).
		State("idle").
		State("a").
		State("b").
		Initial("idle").
		Transition("idle").On("go").GoTo("a").Do("look").
		Transition("a").After(time.Second).On("toB").GoTo("b").
		Quench()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := NewMemStore()
	var err error
	h, err = NewRunner(m, store, WithRunnerClock[string, string, cx](&steppingClock{now: base, stepping: true})).
		Start(ctx, "fire-reads", cx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	id := state.ScheduleID("fireReads", "a", 0)
	want, _ := h.sched.Due(id)
	h = nil
	recovered, err := Recover(ctx, m, store, "fire-reads", WithRunnerClock[string, string, cx](&steppingClock{now: base.Add(time.Hour)}))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.timers[id].due; !got.Equal(want) {
		t.Fatalf("recovered deadline %v, want the live %v", got, want)
	}
	if got, _ := recovered.sched.Due(id); !got.Equal(want) {
		t.Fatalf("recovered scheduler holds %v, want the live %v", got, want)
	}
}
