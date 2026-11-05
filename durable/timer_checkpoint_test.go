package durable_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// pendingTimerMachine arms a long (`after`) timer on entering `waiting`, then lets
// event-driven steps advance within `waiting` before the timer elapses. The
// `after` edge stays armed across those steps, so a checkpoint taken mid-wait
// compacts away the step that armed the timer while the timer is still pending —
// exactly the durable-timer × checkpoint interaction this file proves.
//
// waiting owns a self-looping `poke` edge so several steps can fire while the
// timer remains armed (a `poke` re-enters `waiting`, re-arming the same stable
// scheduleID — idempotent), and the `after` edge fires `elapsed` to `done`.
func pendingTimerMachine() *state.Machine[string, string, *timerCtx] {
	return state.ForgeFor[*timerCtx]("pendingTimer").
		Action("markFired", func(c state.ActionCtx[*timerCtx]) (state.Effect, error) {
			c.Entity.Fired++
			return nil, nil
		}).
		State("idle").
		State("waiting").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("arm").GoTo("waiting").
		Transition("waiting").On("poke").GoTo("waiting").
		Transition("waiting").After(10 * time.Second).On("elapsed").GoTo("done").Do("markFired").
		Quench()
}

// TestTimerCheckpoint_PendingAtCheckpoint_ByteIdentical is the D7 acceptance gate:
// a timer armed BEFORE a checkpoint that compacts the arming step away must still
// fire from its recorded deadline on recovery, and the recovered snapshot must be
// byte-identical to a never-crashed run.
//
// The checkpoint policy lands a checkpoint while the timer is pending, so Load's
// tail no longer carries the ScheduleAfter the live arm absorbed; recovery must
// re-arm the pending timer from the checkpoint's persisted absolute deadline, not
// from a fresh wall-clock read.
func TestTimerCheckpoint_PendingAtCheckpoint_ByteIdentical(t *testing.T) {
	ctx := context.Background()
	m := pendingTimerMachine()

	// Never-crashed reference: arm, poke a few times (timer stays pending), advance
	// past the deadline, tick to fire.
	refStore := durable.NewMemStore()
	refClock := state.NewFakeClock(epoch)
	refRunner := durable.NewRunner(m, refStore,
		durable.WithRunnerClock[string, string, *timerCtx](refClock),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	ref, err := refRunner.Start(ctx, durable.InstanceID("ref"), &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("ref Start: %v", err)
	}
	for _, ev := range []string{"arm", "poke", "poke", "poke"} {
		if _, err = ref.Fire(ctx, ev); err != nil {
			t.Fatalf("ref Fire(%q): %v", ev, err)
		}
	}
	refClock.Advance(11 * time.Second)
	if _, err = ref.Tick(ctx); err != nil {
		t.Fatalf("ref Tick: %v", err)
	}
	if got := ref.Instance().Snapshot().Current; got != "done" {
		t.Fatalf("ref did not reach done: %q", got)
	}
	refBytes, err := state.MarshalSnapshot(ref.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal ref: %v", err)
	}

	// Crashing run: arm + poke so a checkpoint lands mid-wait, then drop the runner
	// with the timer still pending and unfired.
	store := durable.NewMemStore()
	id := durable.InstanceID("pending-checkpoint")
	clk := state.NewFakeClock(epoch)
	r1 := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	h1, err := r1.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"arm", "poke", "poke", "poke"} {
		if _, err = h1.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%q): %v", ev, err)
		}
	}

	// Confirm compaction actually removed the arming step: the tail must not carry
	// the timer's ScheduleAfter, so recovery is forced to re-arm from the checkpoint.
	snap, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if snap == nil {
		t.Fatal("expected a checkpoint after compaction")
	}
	for _, rec := range tail {
		if rec.Step == 0 {
			t.Fatalf("arm step (0) was not compacted; tail still carries it: %+v", tail)
		}
	}

	// Recover on a wholly different wall-clock baseline: if re-arm read the wall
	// clock the deadline would diverge. It must re-arm from the recorded deadline.
	recClock := state.NewFakeClock(epoch.Add(500 * time.Hour))
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](recClock),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// Advance the recovery clock past the recorded deadline and tick: the pending
	// timer must fire from its recorded deadline, reaching done.
	recClock.Advance(11 * time.Second)
	if _, err = recovered.Tick(ctx); err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	if got := recovered.Instance().Snapshot().Current; got != "done" {
		t.Fatalf("recovered did not fire pending timer across checkpoint: %q", got)
	}
	gotBytes, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered: %v", err)
	}
	if string(refBytes) != string(gotBytes) {
		t.Fatalf("pending-timer-at-checkpoint resume mismatch\n ref: %s\n got: %s", refBytes, gotBytes)
	}
}

// TestTimerCheckpoint_CompactionPastTickBarrier proves a checkpoint that compacts
// past a tick barrier still replays correctly: the chained-timer machine fires its
// first timer (recording a tick barrier), a checkpoint compacts that barrier away,
// and recovery still reaches the post-barrier state and fires the second timer.
func TestTimerCheckpoint_CompactionPastTickBarrier(t *testing.T) {
	ctx := context.Background()
	m := chainedTimerMachine()

	// Never-crashed reference.
	refStore := durable.NewMemStore()
	refClock := state.NewFakeClock(epoch)
	refRunner := durable.NewRunner(m, refStore,
		durable.WithRunnerClock[string, string, *timerCtx](refClock),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	ref, err := refRunner.Start(ctx, durable.InstanceID("ref"), &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("ref Start: %v", err)
	}
	if _, err = ref.Fire(ctx, "arm"); err != nil {
		t.Fatalf("ref Fire(arm): %v", err)
	}
	refClock.Advance(4 * time.Second)
	if _, err = ref.Tick(ctx); err != nil { // first timer fires: tick barrier recorded
		t.Fatalf("ref Tick 1: %v", err)
	}
	refClock.Advance(8 * time.Second)
	if _, err = ref.Tick(ctx); err != nil { // second timer fires
		t.Fatalf("ref Tick 2: %v", err)
	}
	refBytes, err := state.MarshalSnapshot(ref.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal ref: %v", err)
	}

	// Crashing run: arm, fire the first timer (tick barrier), then a checkpoint lands
	// past that barrier while the SECOND timer is pending. Drop the runner.
	store := durable.NewMemStore()
	id := durable.InstanceID("compact-barrier")
	clk := state.NewFakeClock(epoch)
	r1 := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	h1, err := r1.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h1.Fire(ctx, "arm"); err != nil { // step 0
		t.Fatalf("Fire(arm): %v", err)
	}
	clk.Advance(4 * time.Second)
	if _, err = h1.Tick(ctx); err != nil { // tick barrier at step 1, fires first timer, arms second
		t.Fatalf("Tick 1: %v", err)
	}

	// Recover on a fresh baseline; the second timer must survive compaction of the
	// tick barrier and fire from its recorded deadline.
	recClock := state.NewFakeClock(epoch.Add(300 * time.Hour))
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](recClock),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	recClock.Advance(8 * time.Second)
	if _, err = recovered.Tick(ctx); err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	if got := recovered.Instance().Snapshot().Current; got != "done" {
		t.Fatalf("recovered did not fire second timer across compacted barrier: %q", got)
	}
	gotBytes, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered: %v", err)
	}
	if string(refBytes) != string(gotBytes) {
		t.Fatalf("compaction-past-barrier mismatch\n ref: %s\n got: %s", refBytes, gotBytes)
	}
}

// TestTimerCheckpoint_InterleavedTimersAndCheckpoints exercises the chained-timer
// machine through several checkpoints (every step), recovering at the very end:
// every timer must have survived its surrounding compaction.
func TestTimerCheckpoint_InterleavedTimersAndCheckpoints(t *testing.T) {
	ctx := context.Background()
	m := chainedTimerMachine()

	live := func(every int) string {
		store := durable.NewMemStore()
		clk := state.NewFakeClock(epoch)
		runner := durable.NewRunner(m, store,
			durable.WithRunnerClock[string, string, *timerCtx](clk),
			durable.WithCheckpointEvery[string, string, *timerCtx](every))
		h, err := runner.Start(ctx, durable.InstanceID("x"), &timerCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "arm"); err != nil {
			t.Fatalf("Fire(arm): %v", err)
		}
		clk.Advance(4 * time.Second)
		if _, err = h.Tick(ctx); err != nil {
			t.Fatalf("Tick 1: %v", err)
		}
		clk.Advance(8 * time.Second)
		if _, err = h.Tick(ctx); err != nil {
			t.Fatalf("Tick 2: %v", err)
		}
		b, err := state.MarshalSnapshot(h.Instance().Snapshot())
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return string(b)
	}

	// A no-checkpoint run and a checkpoint-every-1 run must reach the same snapshot.
	noCheckpoint := live(0)

	store := durable.NewMemStore()
	id := durable.InstanceID("interleaved")
	clk := state.NewFakeClock(epoch)
	runner := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](1))
	h, err := runner.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	clk.Advance(4 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	// Crash here with the second timer pending across a per-step checkpoint.
	recClock := state.NewFakeClock(epoch.Add(900 * time.Hour))
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](recClock),
		durable.WithCheckpointEvery[string, string, *timerCtx](1))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	recClock.Advance(8 * time.Second)
	if _, err = recovered.Tick(ctx); err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	got, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered: %v", err)
	}
	if noCheckpoint != string(got) {
		t.Fatalf("interleaved timers+checkpoints mismatch\n no-ckpt: %s\n got:     %s", noCheckpoint, got)
	}
	if recovered.Instance().Snapshot().Context.Fired != 2 {
		t.Fatalf("recovered fired count: want 2, got %d", recovered.Instance().Snapshot().Context.Fired)
	}
}

// TestTimerCheckpoint_ResumeFiresOnce proves a timer pending across a checkpoint
// fires exactly once after resume: the markFired action runs once, not twice, so
// the recovered Fired count matches a single firing.
func TestTimerCheckpoint_ResumeFiresOnce(t *testing.T) {
	ctx := context.Background()
	m := pendingTimerMachine()

	store := durable.NewMemStore()
	id := durable.InstanceID("fire-once")
	clk := state.NewFakeClock(epoch)
	r1 := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	h1, err := r1.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"arm", "poke", "poke", "poke"} {
		if _, err = h1.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%q): %v", ev, err)
		}
	}

	recClock := state.NewFakeClock(epoch.Add(50 * time.Hour))
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](recClock),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	recClock.Advance(11 * time.Second)
	if _, err = recovered.Tick(ctx); err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	// A second tick after the timer fired must be a no-op: the timer is gone.
	if _, err = recovered.Tick(ctx); err != nil {
		t.Fatalf("recovered Tick 2: %v", err)
	}
	if got := recovered.Instance().Snapshot().Context.Fired; got != 1 {
		t.Fatalf("timer fired %d times across resume, want exactly 1", got)
	}
}

// TestTimerCheckpoint_RecoveryDeterministic confirms two independent recoveries of
// the same pending-timer-across-checkpoint run yield byte-identical snapshots after
// firing the surviving timer.
func TestTimerCheckpoint_RecoveryDeterministic(t *testing.T) {
	ctx := context.Background()
	m := pendingTimerMachine()

	store := durable.NewMemStore()
	id := durable.InstanceID("det")
	clk := state.NewFakeClock(epoch)
	r1 := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](2))
	h1, err := r1.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"arm", "poke", "poke", "poke"} {
		if _, err = h1.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%q): %v", ev, err)
		}
	}

	fire := func() string {
		recClock := state.NewFakeClock(epoch.Add(7 * time.Hour))
		recovered, rerr := durable.Recover(ctx, m, store, id,
			durable.WithRunnerClock[string, string, *timerCtx](recClock),
			durable.WithCheckpointEvery[string, string, *timerCtx](2))
		if rerr != nil {
			t.Fatalf("Recover: %v", rerr)
		}
		recClock.Advance(11 * time.Second)
		if _, terr := recovered.Tick(ctx); terr != nil {
			t.Fatalf("Tick: %v", terr)
		}
		b, merr := state.MarshalSnapshot(recovered.Instance().Snapshot())
		if merr != nil {
			t.Fatalf("marshal: %v", merr)
		}
		return string(b)
	}
	if first, second := fire(), fire(); first != second {
		t.Fatalf("recovery across checkpoint nondeterministic\n first:  %s\n second: %s", first, second)
	}
}

// TestTimerCheckpoint_FiredAfterCheckpoint_ByteIdentical proves a timer persisted by
// a checkpoint and fired later in the tail, after an idle tick, replays at its
// recorded instant instead of being missed.
func TestTimerCheckpoint_FiredAfterCheckpoint_ByteIdentical(t *testing.T) {
	ctx := context.Background()
	m := pendingTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](3),
	}
	runner := durable.NewRunner(m, store, opts...)
	h, err := runner.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"arm", "poke", "poke"} { // checkpoint lands with the timer pending
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%s): %v", ev, err)
		}
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 0 {
		t.Fatalf("tail holds %d records, want the checkpoint to have compacted them", len(tail))
	}
	for _, d := range []time.Duration{5 * time.Second, 6 * time.Second} { // idle, then due
		clk.Advance(d)
		if _, err = h.Tick(ctx); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}

	if got := h.Instance().Current(); got != "done" {
		t.Fatalf("live state %q, want done", got)
	}

	// A clock still at the start proves the timer replays at its recorded instant.
	recovered, err := durable.Recover(ctx, m, store, id, append(opts, durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch)))...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if live, got := handleSnapshot(t, h), handleSnapshot(t, recovered); got != live {
		t.Fatalf("recovered instance differs from live:\n live: %s\n got:  %s", live, got)
	}
}

// TestStateAt_TimerArmedAtStart_Fires proves a historical read re-arms the timers a
// baseline carries, so a timer armed by Start fires in the replayed window.
func TestStateAt_TimerArmedAtStart_Fires(t *testing.T) {
	ctx := context.Background()
	m := timerMachine()
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("timer-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, *timerCtx]{durable.WithRunnerClock[string, string, *timerCtx](clk)}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("arming"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	clk.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	view, err := durable.StateAt(ctx, m, store, id, 0, opts...)
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if got := view.Instance().Current(); got != "done" {
		t.Fatalf("StateAt state %q, want done", got)
	}
}

// TestRecover_OlderIdleTickRecord_FiresNothing proves an idle tick Record from an older
// journal replays as nothing fired, even with a due re-armed timer.
func TestRecover_OlderIdleTickRecord_FiresNothing(t *testing.T) {
	ctx := context.Background()
	m := pendingTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](3),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"arm", "poke", "poke"} { // the checkpoint carries the timer
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%s): %v", ev, err)
		}
	}
	// An older build recorded this idle tick with a reading past the timer's deadline.
	late := epoch.Add(time.Hour).UnixNano()
	idle := durable.Record{Step: 3, Tick: true, Entries: []state.JournalEntry{{Kind: state.JournalClockRead, ClockUnixNano: late}}}
	if _, err = store.Append(ctx, id, idle); err != nil {
		t.Fatalf("Append: %v", err)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "waiting" {
		t.Fatalf("recovered in %q, want waiting: the Record says nothing fired", got)
	}
}

// driftingClock moves by step on every read, as a real clock moves during a tick.
type driftingClock struct {
	now  time.Time
	step time.Duration
}

func (c *driftingClock) Now() time.Time {
	t := c.now
	c.now = c.now.Add(c.step)
	return t
}

func (c *driftingClock) After(time.Duration) <-chan time.Time { return nil }

// twoTimerMachine holds two timers on a parent state. Each fires into one of parent's
// children, so firing one cancels neither.
func twoTimerMachine() *state.Machine[string, string, *timerCtx] {
	return state.ForgeFor[*timerCtx]("twoTimers").
		State("idle").
		SuperState("parent").
		Initial("p1").
		SubState("p1").
		SubState("p2").
		SubState("p3").
		EndSuperState().
		Initial("idle").
		Transition("idle").On("go").GoTo("parent").
		Transition("parent").On("poke").GoTo("p1").
		Transition("parent").After(10 * time.Second).On("first").GoTo("p2").
		Transition("parent").After(10*time.Second + 10*time.Millisecond).On("second").GoTo("p3").
		Quench()
}

// tickAcrossTwoDeadlines starts a twoTimerMachine and ticks it so the tick's first read
// finds one timer due and its second read finds the other.
func tickAcrossTwoDeadlines(
	t *testing.T,
	m *state.Machine[string, string, *timerCtx],
	store durable.Store,
	id durable.InstanceID,
	clk *driftingClock,
	opts []durable.Option[string, string, *timerCtx],
) {
	t.Helper()
	ctx := context.Background()
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	clk.now, clk.step = epoch.Add(10*time.Second+5*time.Millisecond), 20*time.Millisecond
	fired, err := h.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(fired) != 2 {
		t.Fatalf("Tick fired %d timers, want 2", len(fired))
	}
	clk.step = 0
}

// slowChainMachine has a chained timer armed after a slow action, and a second region
// whose timer fires between the chained timer's start and its real deadline.
func slowChainMachine(clk *driftingClock, other time.Duration) *state.Machine[string, string, *timerCtx] {
	return state.ForgeFor[*timerCtx]("slowChain").
		Action("slow", func(state.ActionCtx[*timerCtx]) (state.Effect, error) {
			clk.now = clk.now.Add(200 * time.Millisecond)
			return nil, nil
		}).
		State("idle").
		SuperState("live").
		Region("first").
		Initial("a1").
		SubState("a1").
		SubState("a2").
		SubState("a3").
		EndRegion().
		Region("second").
		Initial("b1").
		SubState("b1").
		SubState("b2").
		EndRegion().
		EndSuperState().
		Initial("idle").
		Transition("idle").On("arm").GoTo("live").
		Transition("a1").After(10 * time.Second).On("x").GoTo("a2").Do("slow").
		Transition("a2").After(5 * time.Second).On("c").GoTo("a3").
		Transition("b1").After(other).On("y").GoTo("b2").
		Transition("b1").On("poke").GoTo("b1").
		Quench()
}

// TestTick_ChainedTimerAfterSlowAction_SurvivesRecovery proves the Handle holds a chained
// timer at the deadline the scheduler armed, after the action ran. An earlier deadline
// would drop it when a later tick passes that instant, and recovery would lose it.
func TestTick_ChainedTimerAfterSlowAction_SurvivesRecovery(t *testing.T) {
	ctx := context.Background()
	clk := &driftingClock{now: epoch, step: 100 * time.Microsecond}
	other := 15*time.Second - 100*time.Microsecond
	m := slowChainMachine(clk, other)
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](1),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	for _, at := range []time.Duration{10 * time.Second, other} {
		clk.now = epoch.Add(at)
		if _, err = h.Tick(ctx); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	clk.now = epoch.Add(16 * time.Second)
	fired, err := recovered.Tick(ctx)
	if err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	if len(fired) != 1 {
		t.Fatalf("recovered Tick fired %d timers, want the chained timer", len(fired))
	}
}

// TestRecover_ChainedTimerAfterSlowAction_NotFiredEarly proves a checkpointed chained
// timer keeps its real deadline, so a tail tick doesn't fire it before live did.
func TestRecover_ChainedTimerAfterSlowAction_NotFiredEarly(t *testing.T) {
	ctx := context.Background()
	clk := &driftingClock{now: epoch, step: 100 * time.Microsecond}
	other := 15*time.Second - 100*time.Microsecond
	m := slowChainMachine(clk, other)
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](3), // the first tick checkpoints, the second stays in the tail
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	clk.now = epoch.Add(10 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	clk.now = epoch.Add(other)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) == 0 {
		t.Fatal("tail is empty, want the second tick left for replay")
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if live, got := handleSnapshot(t, h), handleSnapshot(t, recovered); got != live {
		t.Fatalf("recovered instance differs from live:\n live: %s\n got:  %s", live, got)
	}
}

// TestRecover_ReplayedChainedTimer_KeepsItsDeadline proves a replayed tick mirrors a
// chained timer at the deadline live armed, so the next checkpoint persists that deadline.
func TestRecover_ReplayedChainedTimer_KeepsItsDeadline(t *testing.T) {
	ctx := context.Background()
	clk := &driftingClock{now: epoch, step: 100 * time.Microsecond}
	m := slowChainMachine(clk, time.Minute)
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](6),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	clk.now = epoch.Add(10 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	first, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	for range 3 { // cross a checkpoint boundary after the tick has replayed
		if _, err = first.Fire(ctx, "poke"); err != nil {
			t.Fatalf("Fire(poke): %v", err)
		}
	}
	second, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	clk.now = epoch.Add(15*time.Second + 100*time.Millisecond) // before 15.2s, the real deadline
	fired, err := second.Tick(ctx)
	if err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("recovered Tick fired %d timers before the deadline, want 0", len(fired))
	}
}

// TestRecover_OlderJournal_TailEndingOnIdleTicks_LeavesNoReadings proves a tail ending on
// idle tick Records leaves none of their readings for the live clock, so the first Tick
// after recovery reads the real time and fires what is due.
func TestRecover_OlderJournal_TailEndingOnIdleTicks_LeavesNoReadings(t *testing.T) {
	ctx := context.Background()
	m := pendingTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, *timerCtx]{durable.WithRunnerClock[string, string, *timerCtx](clk)}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	for step := 1; step <= 3; step++ {
		read := state.JournalEntry{Kind: state.JournalClockRead, ClockUnixNano: epoch.Add(time.Duration(step) * time.Second).UnixNano()}
		idle := durable.Record{Step: step, Tick: true, Entries: []state.JournalEntry{read}}
		if _, err = store.Append(ctx, id, idle); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	clk.Advance(20 * time.Second)
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	fired, err := recovered.Tick(ctx)
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if len(fired) != 1 {
		t.Fatalf("first Tick after recovery fired %d timers, want 1", len(fired))
	}
}

// TestRecover_OlderJournal_FireArmsFromItsOwnRead proves a replayed Fire arms its timer
// from its own reading, the last in its Record, past a read an older journal left in front.
func TestRecover_OlderJournal_FireArmsFromItsOwnRead(t *testing.T) {
	ctx := context.Background()
	m := pendingTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, *timerCtx]{durable.WithRunnerClock[string, string, *timerCtx](clk)}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	clk.Advance(time.Second)
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	stale := state.JournalEntry{Kind: state.JournalClockRead, ClockUnixNano: epoch.UnixNano()}
	fire := tail[0]
	fire.Entries = append([]state.JournalEntry{stale}, fire.Entries...)
	older := durable.NewMemStore()
	if _, err = durable.NewRunner(m, older, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = older.Append(ctx, id, fire); err != nil {
		t.Fatalf("Append: %v", err)
	}

	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	clk.Advance(9*time.Second + 500*time.Millisecond) // the timer is due at 11s
	for range 2 {                                     // a misaligned replay leaves a reading for the first
		fired, err := recovered.Tick(ctx)
		if err != nil {
			t.Fatalf("Tick: %v", err)
		}
		if len(fired) != 0 {
			t.Fatalf("Tick fired %d timers before the deadline, want 0", len(fired))
		}
	}
}

// TestTick_TimerArmedAndFiredInOneTick_NotPersisted proves a timer a tick both arms and
// fires leaves the table, so a checkpoint doesn't persist it for recovery to fire again.
func TestTick_TimerArmedAndFiredInOneTick_NotPersisted(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*timerCtx]("zeroDelay").
		State("idle").
		State("wait").
		SuperState("parent").
		Initial("p1").
		SubState("p1").
		SubState("p2").
		SubState("p3").
		EndSuperState().
		Initial("idle").
		Transition("idle").On("arm").GoTo("wait").
		Transition("wait").After(10 * time.Second).On("go").GoTo("parent").
		Transition("parent").After(0).On("zero").GoTo("p2").
		Transition("p2").On("poke").GoTo("p3").
		Quench()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](1),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	clk.Advance(10 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	fired, err := recovered.Tick(ctx)
	if err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	if len(fired) != 0 {
		t.Fatalf("recovered Tick fired %d timers, want 0", len(fired))
	}
}

// TestTick_TimerDueAfterTheFirstRead_NotPersisted proves a timer that comes due between a
// tick's clock reads leaves the table with the others it fired, so a checkpoint doesn't
// persist it and recovery doesn't fire it again.
func TestTick_TimerDueAfterTheFirstRead_NotPersisted(t *testing.T) {
	ctx := context.Background()
	m := twoTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	clk := &driftingClock{now: epoch}
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](1),
	}
	tickAcrossTwoDeadlines(t, m, store, id, clk, opts)

	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	again, err := recovered.Tick(ctx)
	if err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("recovered Tick fired %d timers, want 0", len(again))
	}
}

// TestRecover_TimerDueAfterTheFirstRead_NotPersisted proves the same when the tick is
// replayed: the replayed tick drops the timer, so the next checkpoint doesn't persist it.
func TestRecover_TimerDueAfterTheFirstRead_NotPersisted(t *testing.T) {
	ctx := context.Background()
	m := twoTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")
	clk := &driftingClock{now: epoch}
	opts := []durable.Option[string, string, *timerCtx]{
		durable.WithRunnerClock[string, string, *timerCtx](clk),
		durable.WithCheckpointEvery[string, string, *timerCtx](6),
	}
	tickAcrossTwoDeadlines(t, m, store, id, clk, opts)

	first, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("first Recover: %v", err)
	}
	for range 3 { // cross a checkpoint boundary after the tick has replayed
		if _, err = first.Fire(ctx, "poke"); err != nil {
			t.Fatalf("Fire(poke): %v", err)
		}
	}
	second, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	again, err := second.Tick(ctx)
	if err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("recovered Tick fired %d timers, want 0", len(again))
	}
}

// TestTimerCheckpoint_BarriersCannotStepOverTheInterval proves a checkpoint still lands
// when tick barriers keep stepping over the configured multiple.
func TestTimerCheckpoint_BarriersCannotStepOverTheInterval(t *testing.T) {
	ctx := context.Background()
	m := pollingMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("poll-1")
	clk := state.NewFakeClock(epoch)
	opts := []durable.Option[string, string, *runCtx]{
		durable.WithRunnerClock[string, string, *runCtx](clk),
		durable.WithCheckpointEvery[string, string, *runCtx](8),
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Two Fires put the barriers on even steps, so none lands on the step before a multiple
	// of 8, where a plain run checkpoints.
	for _, ev := range []string{"start", "poke"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%s): %v", ev, err)
		}
	}
	for range 50 {
		clk.Advance(time.Second)
		if _, err = h.Tick(ctx); err != nil {
			t.Fatalf("Tick: %v", err)
		}
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) > 8 {
		t.Fatalf("tail holds %d records after 50 ticks, want the checkpoint to bound it", len(tail))
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if live, got := handleSnapshot(t, h), handleSnapshot(t, recovered); got != live {
		t.Fatalf("recovered instance differs from live:\n live: %s\n got:  %s", live, got)
	}
}
