package durable_test

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// timerCtx is a JSON-marshalable context for the clock record/replay proofs.
type timerCtx struct {
	Fired int `json:"fired"`
}

// timerMachine is a time-dependent machine: arming -> (after 5s) -> done. The
// transition out of arming is driven purely by a delayed (`after`) timer, so the
// instant it fires is a function of the clock the host driver reads — exactly the
// nondeterminism the clock seam records and replays.
func timerMachine() *state.Machine[string, string, *timerCtx] {
	return state.Forge[string, string, *timerCtx]("timer").
		Action("markFired", func(c state.ActionCtx[*timerCtx]) (state.Effect, error) {
			c.Entity.Fired++
			return nil, nil
		}).
		State("idle").
		State("arming").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("arm").GoTo("arming").
		Transition("arming").After(5 * time.Second).On("elapsed").GoTo("done").Do("markFired").
		Quench()
}

// chainedTimerMachine arms a second timer on entering the intermediate state, so
// a single live lifetime reads the clock many times (arm, tick, re-arm, tick),
// exercising multiple clock reads correlated across steps.
func chainedTimerMachine() *state.Machine[string, string, *timerCtx] {
	return state.Forge[string, string, *timerCtx]("chained").
		Action("markFired", func(c state.ActionCtx[*timerCtx]) (state.Effect, error) {
			c.Entity.Fired++
			return nil, nil
		}).
		State("idle").
		State("first").
		State("second").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("arm").GoTo("first").
		Transition("first").After(3 * time.Second).On("t1").GoTo("second").Do("markFired").
		Transition("second").After(7 * time.Second).On("t2").GoTo("done").Do("markFired").
		Quench()
}

// epoch is a fixed, non-zero start instant so recorded ClockUnixNano values are
// distinctive and a wall-clock-independent replay is unmistakable.
var epoch = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

// TestClock_TimerReplay_ByteIdentical is the acceptance gate: a time-dependent
// machine driven live (with the recording clock advancing so the timer fires)
// recovers — through the replay clock, never reading wall-clock — to a snapshot
// byte-identical to the live instance. The timer must fire at the same recorded
// instant on replay, not at a new wall-clock time.
func TestClock_TimerReplay_ByteIdentical(t *testing.T) {
	ctx := context.Background()
	m := timerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-1")

	live := state.NewFakeClock(epoch)
	runner := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, *timerCtx](live))
	h, err := runner.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	// Advance past the 5s deadline and tick so the durable timer fires.
	live.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := h.Instance().Snapshot().Current; got != "done" {
		t.Fatalf("live did not reach done: %q", got)
	}
	liveBytes, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal live: %v", err)
	}

	// Recover on a different wall-clock baseline entirely: if replay read the wall
	// clock the timing would diverge. It must read the recorded values instead.
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch.Add(100*time.Hour))))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	recBytes, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered: %v", err)
	}
	if string(liveBytes) != string(recBytes) {
		t.Fatalf("timer replay not byte-identical\n live: %s\n got:  %s", liveBytes, recBytes)
	}
	if recovered.Instance().Snapshot().Current != "done" {
		t.Fatalf("recovered did not reach done: %q", recovered.Instance().Snapshot().Current)
	}
}

// TestClock_RecordsReadsInOrder confirms every clock read on the live run is
// recorded as a JournalClockRead, in read order, with the real reading.
func TestClock_RecordsReadsInOrder(t *testing.T) {
	ctx := context.Background()
	m := timerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-order")

	live := state.NewFakeClock(epoch)
	runner := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, *timerCtx](live))
	h, err := runner.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	live.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var reads []int64
	for _, rec := range tail {
		for _, e := range rec.Entries {
			if e.Kind != state.JournalClockRead {
				t.Fatalf("non-clock entry recorded: %+v", e)
			}
			reads = append(reads, e.ClockUnixNano)
		}
	}
	if len(reads) == 0 {
		t.Fatal("no clock reads recorded for a timer-driven machine")
	}
	// Reads are wall-clock non-decreasing in the order they happened: the arm-step
	// reads at epoch, the post-advance tick reads at epoch+6s.
	for i := 1; i < len(reads); i++ {
		if reads[i] < reads[i-1] {
			t.Fatalf("clock reads out of order at %d: %d < %d", i, reads[i], reads[i-1])
		}
	}
	if reads[0] != epoch.UnixNano() {
		t.Fatalf("first read: want epoch %d, got %d", epoch.UnixNano(), reads[0])
	}
	if reads[len(reads)-1] != epoch.Add(6*time.Second).UnixNano() {
		t.Fatalf("last read: want epoch+6s, got %d", reads[len(reads)-1])
	}
}

// TestClock_CrashWithPendingTimer_Resumes proves a crash mid-way through a
// pending timer resumes correctly: arm the timer, crash before it fires, recover
// on a fresh wall-clock baseline, then drive the timer to fire on the recovered
// instance. The result matches a never-crashed live run.
func TestClock_CrashWithPendingTimer_Resumes(t *testing.T) {
	ctx := context.Background()
	m := timerMachine()

	// Never-crashed reference run.
	refStore := durable.NewMemStore()
	refClock := state.NewFakeClock(epoch)
	refRunner := durable.NewRunner(m, refStore, durable.WithRunnerClock[string, string, *timerCtx](refClock))
	ref, err := refRunner.Start(ctx, durable.InstanceID("ref"), &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("ref Start: %v", err)
	}
	if _, err = ref.Fire(ctx, "arm"); err != nil {
		t.Fatalf("ref Fire(arm): %v", err)
	}
	refClock.Advance(6 * time.Second)
	if _, err = ref.Tick(ctx); err != nil {
		t.Fatalf("ref Tick: %v", err)
	}
	refBytes, err := state.MarshalSnapshot(ref.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal ref: %v", err)
	}

	// Crashing run: arm the timer, advance the clock, then drop the runner WITHOUT
	// ticking — the timer is pending and unfired at crash time.
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-crash")
	clk := state.NewFakeClock(epoch)
	r1 := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, *timerCtx](clk))
	h1, err := r1.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h1.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	clk.Advance(6 * time.Second) // deadline elapsed, but no Tick: timer still pending

	// Recover on a brand-new wall-clock baseline and tick: the timer must fire from
	// the recorded arm instant, not the new wall clock.
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch.Add(72*time.Hour))))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, err = recovered.Tick(ctx); err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
	if got := recovered.Instance().Snapshot().Current; got != "done" {
		t.Fatalf("recovered did not fire pending timer: %q", got)
	}
	gotBytes, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered: %v", err)
	}
	if string(refBytes) != string(gotBytes) {
		t.Fatalf("crash-with-pending-timer resume mismatch\n ref: %s\n got: %s", refBytes, gotBytes)
	}
}

// TestClock_MultipleReadsOneStep covers a chained-timer machine whose single live
// lifetime reads the clock many times, then recovers byte-identically.
func TestClock_MultipleReadsOneStep(t *testing.T) {
	ctx := context.Background()
	m := chainedTimerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("chained-1")

	clk := state.NewFakeClock(epoch)
	runner := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, *timerCtx](clk))
	h, err := runner.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	clk.Advance(4 * time.Second) // past the 3s first timer
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick 1: %v", err)
	}
	clk.Advance(8 * time.Second) // past the 7s second timer
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick 2: %v", err)
	}
	if got := h.Instance().Snapshot().Current; got != "done" {
		t.Fatalf("chained machine did not reach done: %q", got)
	}
	liveBytes, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal live: %v", err)
	}

	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch.Add(1000*time.Hour))))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	recBytes, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered: %v", err)
	}
	if string(liveBytes) != string(recBytes) {
		t.Fatalf("chained timer replay mismatch\n live: %s\n got:  %s", liveBytes, recBytes)
	}
	if recovered.Instance().Snapshot().Context.Fired != 2 {
		t.Fatalf("recovered fired count: want 2, got %d", recovered.Instance().Snapshot().Context.Fired)
	}
}

// TestClock_NoReadsMachine confirms an event-driven machine that never reads the
// clock still works under the clock-aware Runner, recording no clock entries.
func TestClock_NoReadsMachine(t *testing.T) {
	ctx := context.Background()
	m := linearMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("noclock-1")

	runner := durable.NewRunner(m, store,
		durable.WithRunnerClock[string, string, *runCtx](state.NewFakeClock(epoch)))
	h, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"go", "finish"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%q): %v", ev, err)
		}
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, rec := range tail {
		if len(rec.Entries) != 0 {
			t.Fatalf("event-driven machine recorded clock entries: %+v", rec.Entries)
		}
	}
	recovered, err := durable.Recover(ctx, m, store, id)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Snapshot().Current; got != "done" {
		t.Fatalf("recovered state: want done, got %q", got)
	}
}

// TestClock_GoldenEntrySequence pins the recorded clock-entry sequence of the
// timer machine, so a regression in what or when the clock is read is caught.
func TestClock_GoldenEntrySequence(t *testing.T) {
	ctx := context.Background()
	m := timerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-golden")

	clk := state.NewFakeClock(epoch)
	runner := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, *timerCtx](clk))
	h, err := runner.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	clk.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var entries []state.JournalEntry
	for _, rec := range tail {
		entries = append(entries, rec.Entries...)
	}
	got, err := json.Marshal(entries)
	if err != nil {
		t.Fatalf("marshal entries: %v", err)
	}
	golden := filepath.Join("testdata", "timer_clock_entries.golden.json")
	if *updateGolden {
		if werr := os.WriteFile(golden, append(got, '\n'), 0o644); werr != nil {
			t.Fatalf("write golden: %v", werr)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	if string(got) != strings.TrimRight(string(want), "\n") {
		t.Fatalf("clock-entry golden mismatch (run with -update to refresh)\n want: %s\n got:  %s", want, got)
	}
}

// updateGolden refreshes the committed clock-entry golden when set.
var updateGolden = flag.Bool("update", false, "update golden files")

// TestClock_ReplayDeterministic confirms two independent recoveries of the same
// recorded timer run yield byte-identical snapshots.
func TestClock_ReplayDeterministic(t *testing.T) {
	ctx := context.Background()
	m := timerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("timer-det")

	clk := state.NewFakeClock(epoch)
	runner := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, *timerCtx](clk))
	h, err := runner.Start(ctx, id, &timerCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	clk.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	first := recoverTimerBytes(t, ctx, m, store, id)
	second := recoverTimerBytes(t, ctx, m, store, id)
	if first != second {
		t.Fatalf("timer recovery nondeterministic\n first:  %s\n second: %s", first, second)
	}
}

// recoverTimerBytes recovers the instance and returns its marshaled snapshot.
func recoverTimerBytes(t *testing.T, ctx context.Context, m *state.Machine[string, string, *timerCtx], store durable.Store, id durable.InstanceID) string {
	t.Helper()
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithRunnerClock[string, string, *timerCtx](state.NewFakeClock(epoch.Add(5*time.Hour))))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	b, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}
