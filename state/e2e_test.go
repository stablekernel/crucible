package state_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// This file drives the connection lifecycle exemplar (exemplar_test.go) end-to-end
// through the real host runtime — ActorSystem + Scheduler (FakeClock) +
// ServiceRunner wired together — running a realistic sequence of events, timer
// ticks, service settles, and actor messages. It asserts the final configuration,
// key trace milestones, and a snapshot/restore-mid-run identity check, so the
// exemplar doubles as the flagship "here's a real machine driven for real" artifact.

// TestE2E_ConnectionLifecycle drives the exemplar through its full happy path:
// a transient dial failure that backs off and retries on a timer, a guarded
// admission into the parallel Connected configuration, a worker actor that runs a
// task to completion, a heartbeat round-trip, and an eventless run-to-completion
// shutdown. It asserts the configuration and the captured side effects at each
// milestone.
func TestE2E_ConnectionLifecycle(t *testing.T) {
	ctx := context.Background()
	h := newConnHarness()

	assertConfig(t, h, "initial", Disconnected)

	// Connect arms the dial service and counts the first attempt. The dial has not
	// settled yet, so the instance waits in Connecting.
	res := h.fire(ctx, Connect)
	assertConfig(t, h, "after Connect", Connecting)
	if h.run.Pending() != 1 {
		t.Fatalf("Connect should arm the dial service; pending=%d", h.run.Pending())
	}
	if got := h.inst.Entity().Dials; got != 1 {
		t.Fatalf("Connect should count one dial; got %d", got)
	}
	assertMicrostep(t, res, "service.start."+h.dialID())

	// The first dial fails transiently: onError routes DialFailed, which falls back
	// to Backoff and arms the connect-timeout retry timer.
	h.settleDial(ctx, false)
	assertConfig(t, h, "after dial failure", Backoff)
	if h.sch.Pending() != 1 {
		t.Fatalf("Backoff should arm one retry timer; pending=%d", h.sch.Pending())
	}

	// Advancing the fake clock past the connect timeout fires the delayed Retry
	// edge, which re-enters Connecting and re-arms the dial (second attempt).
	ticks := h.advancePastTimeout(ctx)
	if len(ticks) != 1 || ticks[0].NewState != Connecting {
		t.Fatalf("retry tick should re-enter Connecting; got %+v", ticks)
	}
	assertConfig(t, h, "after retry", Connecting)
	if got := h.inst.Entity().Dials; got != 2 {
		t.Fatalf("retry should count a second dial; got %d", got)
	}

	// The second dial succeeds: onDone routes Dialed, whose guard combinator
	// (canAdmit AND (isHealthy OR not stateIn Connected)) passes, admitting the
	// instance into the parallel Connected configuration (Beating + WorkIdle).
	res = h.settleDial(ctx, true)
	assertConfig(t, h, "after dial success", Beating, WorkIdle)
	assertGuards(t, res, "canAdmit", "isHealthy")
	assertEntered(t, res, Connected, Live, Beating, WorkIdle)
	if got := lastNote(h); got != "token:session-token" {
		t.Fatalf("Dialed action should capture the dial token; last note = %q", got)
	}

	// Assigning work spawns a worker child actor via the dynamic spawn built-in.
	h.fire(ctx, Assign)
	assertConfig(t, h, "after Assign", Beating, Processing)
	if h.sys.Running() != 1 {
		t.Fatalf("Assign should spawn one worker actor; running=%d", h.sys.Running())
	}

	// Stepping the worker to its final state routes Completed back through the
	// connection (the parent), advancing the Work region to Drained and capturing
	// the worker's output.
	h.runWorkers(ctx)
	assertConfig(t, h, "after worker completion", Beating, Drained)
	if h.sys.Running() != 0 {
		t.Fatalf("worker should auto-stop on completion; running=%d", h.sys.Running())
	}
	if got := lastNote(h); got != "work:task-output" {
		t.Fatalf("Completed action should capture the worker output; last note = %q", got)
	}

	// The Heartbeat region runs orthogonally: Ping moves it to Missed while the
	// Work region stays Drained, and Pong restores Beating.
	h.fire(ctx, Ping)
	assertConfig(t, h, "after Ping", Missed, Drained)
	h.fire(ctx, Pong)
	assertConfig(t, h, "after Pong", Beating, Drained)

	// Close exits the whole Connected compound to Closing, whose eventless Always
	// edge runs to completion and lands in the final Closed state.
	res = h.fire(ctx, Close)
	assertConfig(t, h, "after Close", Closed)
	assertMicrostep(t, res, "always")
	if !h.inst.InFinal() {
		t.Fatal("Closed is final; instance should report InFinal")
	}
}

// TestE2E_DeepHistoryReconnect drives the exemplar to a non-initial parallel
// configuration, drops the connection, and reconnects through the Connected deep
// history — asserting the prior Live configuration is restored exactly rather than
// re-entered at the regions' initial leaves.
func TestE2E_DeepHistoryReconnect(t *testing.T) {
	ctx := context.Background()
	h := newConnHarness()

	connect(ctx, h)
	h.fire(ctx, Assign)
	h.runWorkers(ctx) // Work region -> Drained
	h.fire(ctx, Ping) // Heartbeat region -> Missed
	assertConfig(t, h, "before drop", Missed, Drained)

	// Drop records the live configuration in deep history and falls back to
	// Disconnected.
	h.fire(ctx, Drop)
	assertConfig(t, h, "after drop", Disconnected)

	// Reconnect targets the Resume deep-history pseudo-state, restoring the exact
	// dropped configuration (Missed + Drained), not the initial Beating + WorkIdle.
	res := h.fire(ctx, Reconnect)
	assertConfig(t, h, "after reconnect", Missed, Drained)
	assertEntered(t, res, Connected, Live, Missed, Drained)
}

// TestE2E_SnapshotRestoreMidRun snapshots the instance mid-run (in a live parallel
// configuration with a spawned worker), restores it into a fresh machine and
// driver set, and asserts the restored instance resumes identically — same
// configuration, and the same next transition.
func TestE2E_SnapshotRestoreMidRun(t *testing.T) {
	ctx := context.Background()
	h := newConnHarness()

	connect(ctx, h)
	h.fire(ctx, Assign) // mid-run: Beating + Processing, worker in flight
	assertConfig(t, h, "mid-run", Beating, Processing)

	// Snapshot the live runtime state and round-trip it through JSON, exactly as a
	// host persisting an instance between Fires would.
	data, err := json.Marshal(h.inst.Snapshot())
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	var snap state.Snapshot[Conn, ConnEvent, Link]
	if err = json.Unmarshal(data, &snap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}

	// Restore into a fresh machine bound to a new driver set, as a recovering host
	// would after a restart.
	m := buildConnMachine()
	clk := state.NewFakeClock(time.Unix(0, 0))
	restored, err := m.Restore(snap, state.WithRestoreClock[Conn](clk))
	if err != nil {
		t.Fatalf("restore: %v", err)
	}

	// The restored configuration is identical to the live one at snapshot time.
	if got, want := fmtConfig(restored.Configuration()), fmtConfig(h.inst.Configuration()); got != want {
		t.Fatalf("restored configuration = %s, want identical %s", got, want)
	}

	// The restored instance resumes identically: a Ping advances the Heartbeat
	// region exactly as it would have on the original instance.
	run := state.NewServiceRunner(restored, nil)
	sys := state.NewActorSystem(restored).Register("worker", workerBehavior())
	sch := state.NewScheduler(restored)
	resume := restored.ResumeEffects()
	run.Absorb(ctx, resume)
	sch.Absorb(ctx, resume)
	sys.AbsorbFor(ctx, nil, resume)

	res := restored.Fire(ctx, Ping)
	if res.NewState != Missed {
		t.Fatalf("restored Ping should advance Heartbeat to Missed; got %v", res.NewState)
	}
	if got, want := fmtConfig(restored.Configuration()), fmtConfig([]Conn{Missed, Processing}); got != want {
		t.Fatalf("restored configuration after Ping = %s, want %s", got, want)
	}
}

// connect drives the exemplar from Disconnected to the live parallel Connected
// configuration (Beating + WorkIdle), exercising the dial failure, the timer-driven
// retry, and the guarded admission. It is the shared setup for the history and
// snapshot e2e tests.
func connect(ctx context.Context, h *connHarness) {
	h.fire(ctx, Connect)
	h.settleDial(ctx, false)
	h.advancePastTimeout(ctx)
	h.settleDial(ctx, true)
}

// assertConfig fails the test unless the instance's active configuration equals
// want exactly (order-sensitive, as the engine reports it).
func assertConfig(t *testing.T, h *connHarness, at string, want ...Conn) {
	t.Helper()
	if got, w := fmtConfig(h.inst.Configuration()), fmtConfig(want); got != w {
		t.Fatalf("%s: configuration = %s, want %s", at, got, w)
	}
}

// assertEntered fails the test unless the FireResult's entry cascade equals want.
func assertEntered(t *testing.T, res state.FireResult[Conn], want ...Conn) {
	t.Helper()
	got := res.Trace.EnteredStates
	if len(got) != len(want) {
		t.Fatalf("entered = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i].String() {
			t.Fatalf("entered = %v, want %v", got, want)
		}
	}
}

// assertGuards fails the test unless the FireResult evaluated exactly the named
// guard leaves, in order.
func assertGuards(t *testing.T, res state.FireResult[Conn], want ...string) {
	t.Helper()
	got := res.Trace.GuardsEvaluated
	if len(got) != len(want) {
		t.Fatalf("guards evaluated = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("guards evaluated = %v, want %v", got, want)
		}
	}
}

// assertMicrostep fails the test unless the FireResult's trace records the named
// microstep milestone.
func assertMicrostep(t *testing.T, res state.FireResult[Conn], want string) {
	t.Helper()
	for _, ms := range res.Trace.Microsteps {
		if ms == want {
			return
		}
	}
	t.Fatalf("microsteps %v missing milestone %q", res.Trace.Microsteps, want)
}

// fmtConfig renders a configuration as a stable comma-joined label for comparison.
func fmtConfig(cfg []Conn) string {
	out := ""
	for i, s := range cfg {
		if i > 0 {
			out += ","
		}
		out += s.String()
	}
	return out
}

// lastNote returns the last note the run's actions recorded on the entity, or ""
// when none were recorded.
func lastNote(h *connHarness) string {
	notes := h.inst.Entity().Notes
	if len(notes) == 0 {
		return ""
	}
	return notes[len(notes)-1]
}
