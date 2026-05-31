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
	return state.Forge[string, string, *timerCtx]("pendingTimer").
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
