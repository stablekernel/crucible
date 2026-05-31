package durable_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// recordAssignRun starts an instance of assignMachine and fires events through a
// live Handle against a history-retaining MemStore, returning the store and id so
// a time-travel reader can reconstruct each intermediate step. The assign machine
// folds context on every transition, so a state reconstructed at step k matches a
// never-durable live run of exactly k+1 events only if the bounded replay stopped
// at the right step.
func recordAssignRun(t *testing.T, events []string) (durable.Store, durable.InstanceID) {
	t.Helper()
	ctx := context.Background()
	m := assignMachine()
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("tt-assign")

	runner := durable.NewRunner(m, store)
	h, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range events {
		if _, ferr := h.Fire(ctx, ev); ferr != nil {
			t.Fatalf("Fire(%q): %v", ev, ferr)
		}
	}
	return store, id
}

// referenceSnapshotAtStep drives a fresh, never-durable instance through exactly
// the first n events and returns its marshaled snapshot — the byte-identical
// ground truth a time-travel read at step n-1 must reproduce.
func referenceSnapshotAtStep(t *testing.T, events []string, n int) []byte {
	t.Helper()
	m := assignMachine()
	return liveSnapshotBytes(t, m, "idle", &runCtx{}, events[:n])
}

// TestStateAt_ByteIdenticalAtEachStep is the time-travel acceptance gate: for every
// recorded step k, the reconstructed state is byte-identical to a never-durable run
// of exactly the first k+1 events. The assign machine mutates context each step, so
// matching at every k proves the bounded replay stops at exactly the right record.
func TestStateAt_ByteIdenticalAtEachStep(t *testing.T) {
	ctx := context.Background()
	events := []string{"go", "again", "again", "finish"}
	store, id := recordAssignRun(t, events)
	m := assignMachine()

	for k := 0; k < len(events); k++ {
		view, err := durable.StateAt(ctx, m, store, id, k)
		if err != nil {
			t.Fatalf("StateAt(step=%d): %v", k, err)
		}
		got, err := state.MarshalSnapshot(view.Instance().Snapshot())
		if err != nil {
			t.Fatalf("marshal reconstructed step %d: %v", k, err)
		}
		want := referenceSnapshotAtStep(t, events, k+1)
		if string(got) != string(want) {
			t.Fatalf("step %d not byte-identical\n want: %s\n  got: %s", k, want, got)
		}
		if view.Step() != k {
			t.Fatalf("view.Step() = %d, want %d", view.Step(), k)
		}
	}
}

// TestStateAt_BaselineStep reconstructs the instance at the start baseline (before
// any event fired): it must equal a fresh cast of the machine.
func TestStateAt_BaselineStep(t *testing.T) {
	ctx := context.Background()
	events := []string{"go", "again", "finish"}
	store, id := recordAssignRun(t, events)
	m := assignMachine()

	view, err := durable.StateAt(ctx, m, store, id, durable.BaselineStep)
	if err != nil {
		t.Fatalf("StateAt(baseline): %v", err)
	}
	got, err := state.MarshalSnapshot(view.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal baseline: %v", err)
	}
	want := referenceSnapshotAtStep(t, events, 0)
	if string(got) != string(want) {
		t.Fatalf("baseline not byte-identical\n want: %s\n  got: %s", want, got)
	}
}

// TestStateAt_RejectsOutOfRange surfaces a clear error for a step beyond the
// recorded history (and below the baseline), rather than silently returning the
// latest or panicking.
func TestStateAt_RejectsOutOfRange(t *testing.T) {
	ctx := context.Background()
	events := []string{"go", "again", "finish"}
	store, id := recordAssignRun(t, events)
	m := assignMachine()

	for _, step := range []int{len(events), len(events) + 5, durable.BaselineStep - 1} {
		if _, err := durable.StateAt(ctx, m, store, id, step); !errors.Is(err, durable.ErrStepOutOfRange) {
			t.Fatalf("StateAt(step=%d): want ErrStepOutOfRange, got %v", step, err)
		}
	}
}

// TestStateAt_UnknownInstance surfaces the Store's not-found error through the
// reader rather than masking it.
func TestStateAt_UnknownInstance(t *testing.T) {
	ctx := context.Background()
	m := assignMachine()
	store := durable.NewMemStore(durable.WithHistory())
	if _, err := durable.StateAt(ctx, m, store, durable.InstanceID("nope"), 0); !errors.Is(err, durable.ErrInstanceNotFound) {
		t.Fatalf("want ErrInstanceNotFound, got %v", err)
	}
}

// TestStateAt_NonMutating proves the reader is read-only: a StateAt call leaves the
// live instance and the store untouched, so a subsequent recovery still reaches the
// same final state the live run did and the recorded history is unchanged.
func TestStateAt_NonMutating(t *testing.T) {
	ctx := context.Background()
	events := []string{"go", "again", "finish"}
	store, id := recordAssignRun(t, events)
	m := assignMachine()

	// Capture the full history and the final recovered snapshot BEFORE any read.
	stepsBefore, err := durable.Steps(ctx, store, id)
	if err != nil {
		t.Fatalf("Steps before: %v", err)
	}
	finalBefore := recoverFinalSnapshot(t, m, store, id)

	// A battery of time-travel reads at every step, plus a repeat read.
	for k := durable.BaselineStep; k < len(events); k++ {
		if _, rerr := durable.StateAt(ctx, m, store, id, k); rerr != nil {
			t.Fatalf("StateAt(step=%d): %v", k, rerr)
		}
	}
	if _, rerr := durable.StateAt(ctx, m, store, id, 1); rerr != nil {
		t.Fatalf("StateAt repeat: %v", rerr)
	}

	// The store's recorded steps and the recovered final state must be unchanged.
	stepsAfter, err := durable.Steps(ctx, store, id)
	if err != nil {
		t.Fatalf("Steps after: %v", err)
	}
	if len(stepsBefore) != len(stepsAfter) {
		t.Fatalf("StateAt mutated recorded history: %d steps before, %d after", len(stepsBefore), len(stepsAfter))
	}
	for i := range stepsBefore {
		if stepsBefore[i] != stepsAfter[i] {
			t.Fatalf("StateAt reordered recorded steps at %d: %v vs %v", i, stepsBefore, stepsAfter)
		}
	}
	finalAfter := recoverFinalSnapshot(t, m, store, id)
	if string(finalBefore) != string(finalAfter) {
		t.Fatalf("StateAt mutated the live instance\n before: %s\n  after: %s", finalBefore, finalAfter)
	}
}

// TestStateAt_NoEffectDispatch proves a time-travel read never re-runs a side
// effect: an effect-emitting machine is read at the step that emitted its effect,
// and the recording effect handler is asserted untouched.
func TestStateAt_NoEffectDispatch(t *testing.T) {
	ctx := context.Background()
	m := effectMachine()
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("tt-effect")

	var dispatched int
	handler := func(context.Context, string, state.Effect) error { dispatched++; return nil }
	runner := durable.NewRunner(m, store,
		durable.WithEffectHandler[string, string, *runCtx](handler))
	h, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil { // emits the notify effect
		t.Fatalf("Fire(go): %v", err)
	}
	liveDispatched := dispatched

	// Reading at the effect-emitting step must NOT invoke the handler again.
	if _, err = durable.StateAt(ctx, m, store, id, 0,
		durable.WithEffectHandler[string, string, *runCtx](handler)); err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if dispatched != liveDispatched {
		t.Fatalf("time-travel read dispatched an effect: %d before, %d after", liveDispatched, dispatched)
	}
}

// TestSteps_EnumeratesRecordedSteps confirms Steps reports exactly the fired step
// ordinals in order, so a caller can drive StateAt across the run.
func TestSteps_EnumeratesRecordedSteps(t *testing.T) {
	ctx := context.Background()
	events := []string{"go", "again", "finish"}
	store, id := recordAssignRun(t, events)

	steps, err := durable.Steps(ctx, store, id)
	if err != nil {
		t.Fatalf("Steps: %v", err)
	}
	want := []int{0, 1, 2}
	if len(steps) != len(want) {
		t.Fatalf("Steps len: want %d, got %d (%v)", len(want), len(steps), steps)
	}
	for i := range want {
		if steps[i] != want[i] {
			t.Fatalf("Steps[%d] = %d, want %d", i, steps[i], want[i])
		}
	}
}

// TestStateAt_AcrossCheckpointCompaction proves time-travel reads work even when a
// checkpoint policy compacted the live tail: a history-retaining store keeps the
// pre-checkpoint records, so every intermediate step is still reconstructable.
func TestStateAt_AcrossCheckpointCompaction(t *testing.T) {
	ctx := context.Background()
	events := []string{"go", "again", "again", "finish"}
	m := assignMachine()
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("tt-compact")

	runner := durable.NewRunner(m, store,
		durable.WithCheckpointEvery[string, string, *runCtx](2))
	h, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range events {
		if _, ferr := h.Fire(ctx, ev); ferr != nil {
			t.Fatalf("Fire(%q): %v", ev, ferr)
		}
	}

	// Even step 0 — compacted out of the live tail by the checkpoint — is readable.
	for k := 0; k < len(events); k++ {
		view, verr := durable.StateAt(ctx, m, store, id, k)
		if verr != nil {
			t.Fatalf("StateAt(step=%d) across compaction: %v", k, verr)
		}
		got, merr := state.MarshalSnapshot(view.Instance().Snapshot())
		if merr != nil {
			t.Fatalf("marshal step %d: %v", k, merr)
		}
		want := referenceSnapshotAtStep(t, events, k+1)
		if string(got) != string(want) {
			t.Fatalf("step %d across compaction not byte-identical\n want: %s\n  got: %s", k, want, got)
		}
	}
}

// TestStateAt_AllSeams reconstructs an instance that exercises every recorded seam
// (service, actor, timer) at its final and an intermediate step, so the reader's
// service-settle, actor-refire, and scheduler-tick replay branches are all driven —
// purely from recorded values, byte-identical to a never-durable reference at each.
func TestStateAt_AllSeams(t *testing.T) {
	ctx := context.Background()
	m := pipelineMachine()
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("tt-seams")

	var svcCalls, actorRuns int64
	seams := newPipelineSeams()
	clk := state.NewFakeClock(epoch)
	runner := durable.NewRunner(
		m, store,
		durable.WithRunnerClock[string, string, *pipelineCtx](clk),
		durable.WithServiceRegistry[string, string, *pipelineCtx](pipelineRegistry(pipelineService(&svcCalls))),
		durable.WithActorPalette[string, string, *pipelineCtx](pipelinePalette(&actorRuns)),
		durable.WithEffectHandler[string, string, *pipelineCtx](seams.effectHandler()),
	)
	h, err := runner.Start(ctx, id, &pipelineCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	drivePipeline(t, ctx, h, clk)

	// A read at the final step must reconstruct the done state without re-running any
	// seam: the service/actor counters and the effect handler stay put.
	svcBefore, actorBefore := atomic.LoadInt64(&svcCalls), atomic.LoadInt64(&actorRuns)
	effectsBefore := seams.totalEffectApplies()

	steps, err := durable.Steps(ctx, store, id)
	if err != nil {
		t.Fatalf("Steps: %v", err)
	}
	last := steps[len(steps)-1]
	// The reader settles recorded service/actor outcomes through the same host drivers
	// the live run used, so it needs the registry and palette wired — though it invokes
	// neither (a different counter would expose a re-run).
	view, err := durable.StateAt(ctx, m, store, id, last,
		durable.WithRunnerClock[string, string, *pipelineCtx](state.NewFakeClock(epoch)),
		durable.WithServiceRegistry[string, string, *pipelineCtx](pipelineRegistry(pipelineService(&svcCalls))),
		durable.WithActorPalette[string, string, *pipelineCtx](pipelinePalette(&actorRuns)))
	if err != nil {
		t.Fatalf("StateAt(final): %v", err)
	}
	if got := view.Snapshot().Current; got != "done" {
		t.Fatalf("reconstructed final state = %q, want done", got)
	}
	if atomic.LoadInt64(&svcCalls) != svcBefore || atomic.LoadInt64(&actorRuns) != actorBefore {
		t.Fatalf("time-travel re-ran a seam: svc %d->%d actor %d->%d",
			svcBefore, atomic.LoadInt64(&svcCalls), actorBefore, atomic.LoadInt64(&actorRuns))
	}
	if seams.totalEffectApplies() != effectsBefore {
		t.Fatalf("time-travel re-dispatched an effect: %d->%d", effectsBefore, seams.totalEffectApplies())
	}

	// The final reconstruction is byte-identical to a never-durable reference run.
	wantBytes, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal live final: %v", err)
	}
	gotBytes, err := state.MarshalSnapshot(view.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal reconstructed: %v", err)
	}
	if string(gotBytes) != string(wantBytes) {
		t.Fatalf("final reconstruction not byte-identical\n want: %s\n  got: %s", wantBytes, gotBytes)
	}
}

// TestStateAt_FallbackWithoutHistory proves the reader works against a plain MemStore
// (no WithHistory): it falls back to the latest checkpoint plus its tail, so steps at
// or after that checkpoint are reconstructable even without full-history retention.
func TestStateAt_FallbackWithoutHistory(t *testing.T) {
	ctx := context.Background()
	events := []string{"go", "again", "again", "finish"}
	m := assignMachine()
	store := durable.NewMemStore() // no WithHistory: fall back to Load's view
	id := durable.InstanceID("tt-fallback")

	runner := durable.NewRunner(m, store)
	h, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range events {
		if _, ferr := h.Fire(ctx, ev); ferr != nil {
			t.Fatalf("Fire(%q): %v", ev, ferr)
		}
	}

	// With no checkpoint policy, Load returns the baseline plus the full tail, so every
	// step is still reachable through the fallback path.
	for k := 0; k < len(events); k++ {
		view, verr := durable.StateAt(ctx, m, store, id, k)
		if verr != nil {
			t.Fatalf("StateAt(step=%d) fallback: %v", k, verr)
		}
		got, merr := state.MarshalSnapshot(view.Instance().Snapshot())
		if merr != nil {
			t.Fatalf("marshal step %d: %v", k, merr)
		}
		want := referenceSnapshotAtStep(t, events, k+1)
		if string(got) != string(want) {
			t.Fatalf("fallback step %d not byte-identical\n want: %s\n  got: %s", k, want, got)
		}
	}

	// Steps enumerates the fallback tail too.
	steps, err := durable.Steps(ctx, store, id)
	if err != nil {
		t.Fatalf("Steps fallback: %v", err)
	}
	if len(steps) != len(events) {
		t.Fatalf("fallback Steps len: want %d, got %d", len(events), len(steps))
	}
}

// TestStateAt_FileStoreFallback drives the reader against a FileStore, which does not
// implement the HistoryStore seam, exercising loadHistory's Load fallback. With no
// checkpoint policy the on-disk journal carries every step, so each is reconstructable.
func TestStateAt_FileStoreFallback(t *testing.T) {
	ctx := context.Background()
	events := []string{"go", "again", "finish"}
	m := assignMachine()
	store, err := durable.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	id := durable.InstanceID("tt-file")

	runner := durable.NewRunner(m, store)
	h, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range events {
		if _, ferr := h.Fire(ctx, ev); ferr != nil {
			t.Fatalf("Fire(%q): %v", ev, ferr)
		}
	}

	for k := 0; k < len(events); k++ {
		view, verr := durable.StateAt(ctx, m, store, id, k)
		if verr != nil {
			t.Fatalf("StateAt(step=%d) on FileStore: %v", k, verr)
		}
		got, merr := state.MarshalSnapshot(view.Instance().Snapshot())
		if merr != nil {
			t.Fatalf("marshal step %d: %v", k, merr)
		}
		want := referenceSnapshotAtStep(t, events, k+1)
		if string(got) != string(want) {
			t.Fatalf("FileStore step %d not byte-identical\n want: %s\n  got: %s", k, want, got)
		}
	}

	if _, err = durable.Steps(ctx, store, durable.InstanceID("missing")); !errors.Is(err, durable.ErrInstanceNotFound) {
		t.Fatalf("Steps(missing) on FileStore: want ErrInstanceNotFound, got %v", err)
	}
}

// TestStateAt_TimerInstance reconstructs a timer-driven instance at the step before
// and after the timer fires, exercising the reader's scheduler-tick replay branch and
// lastStep's tick-barrier accounting.
func TestStateAt_TimerInstance(t *testing.T) {
	ctx := context.Background()
	m := timerMachine()
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("tt-timer")

	clk := state.NewFakeClock(epoch)
	runner := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *timerCtx](clk))
	h, err := runner.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil { // step 0: arms the after-timer
		t.Fatalf("Fire(arm): %v", err)
	}
	clk.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil { // tick barrier: fires the timer to done
		t.Fatalf("Tick: %v", err)
	}

	// The last recorded ordinal spans the tick barrier; reading it must reach done via
	// the replayed tick, on the recorded deadline (no wall clock consulted).
	steps, err := durable.Steps(ctx, store, id)
	if err != nil {
		t.Fatalf("Steps: %v", err)
	}
	last := steps[len(steps)-1]
	view, err := durable.StateAt(ctx, m, store, id, last+1, // the tick fired a timer at last+1
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch.Add(100*time.Hour))))
	if err != nil {
		t.Fatalf("StateAt(after tick): %v", err)
	}
	if got := view.Snapshot().Current; got != "done" {
		t.Fatalf("reconstructed post-tick state = %q, want done", got)
	}
}

// TestStateAt_FreshlyStarted reconstructs an instance that was started but never
// fired: only the baseline exists, so the recorded range is empty and the only valid
// read is BaselineStep — any fired step is out of range.
func TestStateAt_FreshlyStarted(t *testing.T) {
	ctx := context.Background()
	m := assignMachine()
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("tt-fresh")

	runner := durable.NewRunner(m, store)
	if _, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}

	steps, err := durable.Steps(ctx, store, id)
	if err != nil {
		t.Fatalf("Steps: %v", err)
	}
	if len(steps) != 0 {
		t.Fatalf("freshly started instance has %d recorded steps, want 0", len(steps))
	}

	view, err := durable.StateAt(ctx, m, store, id, durable.BaselineStep)
	if err != nil {
		t.Fatalf("StateAt(baseline) on fresh instance: %v", err)
	}
	if got := view.Snapshot().Current; got != "idle" {
		t.Fatalf("fresh baseline state = %q, want idle", got)
	}
	if _, err := durable.StateAt(ctx, m, store, id, 0); !errors.Is(err, durable.ErrStepOutOfRange) {
		t.Fatalf("StateAt(0) on fresh instance: want ErrStepOutOfRange, got %v", err)
	}
}

// recoverFinalSnapshot recovers the instance and returns its marshaled snapshot,
// the live final state a non-mutating read must leave intact.
func recoverFinalSnapshot(t *testing.T, m *state.Machine[string, string, *runCtx], store durable.Store, id durable.InstanceID) []byte {
	t.Helper()
	h, err := durable.Recover(context.Background(), m, store, id)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	b, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered: %v", err)
	}
	return b
}
