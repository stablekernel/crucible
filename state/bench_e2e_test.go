package state_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file adds the end-to-end benchmark over the connection lifecycle exemplar
// and micro-benchmarks for the hot paths the original bench_test.go does not cover:
// guard-combinator and stateIn evaluation, hierarchical and deep-nested Fire,
// history record/restore, actor spawn + dispatch + message delivery, the `after`
// schedule + fire cycle, snapshot + restore, and invoke start + settle. Each
// reports allocations; together with the existing Fire / cascade / parallel /
// JSON benchmarks they auto-join the benchstat CI gate (go test -bench=. on state).

// BenchmarkE2E_ConnectionLifecycle measures a full representative run of the
// exemplar through the wired runtime: Connect, a dial failure, a timer-driven
// retry, a successful guarded admission into the parallel configuration, a worker
// actor run to completion, a heartbeat round-trip, and an eventless shutdown.
func BenchmarkE2E_ConnectionLifecycle(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := newConnHarness()
		h.fire(ctx, Connect)
		h.settleDial(ctx, false)
		h.advancePastTimeout(ctx)
		h.settleDial(ctx, true)
		h.fire(ctx, Assign)
		h.runWorkers(ctx)
		h.fire(ctx, Ping)
		h.fire(ctx, Pong)
		h.fire(ctx, Close)
	}
}

// BenchmarkGuardExpr measures guard-expression evaluation on the Dialed edge,
// whose combinator nests And / Or / Not over named-ref leaves and the stateIn
// built-in — the composite-guard hot path the flat BenchmarkFireGuarded does not
// reach. The flat sub-benchmark isolates a single named-ref guard for comparison.
func BenchmarkGuardExpr(b *testing.B) {
	ctx := context.Background()

	b.Run("flat", func(b *testing.B) {
		m := buildDocMachine()
		doc := &Document{Status: Submitted, ReviewerID: strptr("rev-1")}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Cast(doc, state.WithInitialState(Submitted)).Fire(ctx, Approve)
		}
	})

	b.Run("combinator+stateIn", func(b *testing.B) {
		// Drive a fresh harness to Connecting with a re-armed dial, then settle the
		// dial done so the guarded Dialed edge (And(canAdmit, Or(isHealthy,
		// Not(stateIn(Connected))))) is evaluated each iteration.
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h := newConnHarness()
			h.fire(ctx, Connect)
			h.settleDial(ctx, false)
			h.advancePastTimeout(ctx)
			h.settleDial(ctx, true) // fires Dialed, evaluating the composite guard
		}
	})
}

// BenchmarkFireHierarchical measures Fire over nested compound states: a single
// entry-cascade transition (flat-to-superstate) and a deep nested transition that
// exits and re-enters several levels, the hot path the flat BenchmarkFire misses.
func BenchmarkFireHierarchical(b *testing.B) {
	ctx := context.Background()

	b.Run("hierarchical", func(b *testing.B) {
		m := buildJobMachine()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			job := &Job{Status: Queued}
			inst := m.Cast(job, state.WithInitialState(Queued))
			inst.Fire(ctx, Enqueue) // Queued -> Running (descends to Starting)
			inst.Fire(ctx, Begin)   // Starting -> Executing within Running
		}
	})

	b.Run("nested", func(b *testing.B) {
		m := buildNestedBenchMachine()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inst := m.Cast(&benchCtx{}, state.WithInitialState("l3a"))
			// Cross-subtree transition forces a deep exit/entry cascade (l3a up to the
			// common ancestor and back down to l3b).
			inst.Fire(ctx, "cross")
		}
	})
}

// BenchmarkHistory measures the deep-history record + restore cycle: driving the
// Work/Heartbeat regions to a non-initial configuration (record), dropping to
// Disconnected, and reconnecting through the deep-history pseudo-state (restore).
func BenchmarkHistory(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := newConnHarness()
		h.fire(ctx, Connect)
		h.settleDial(ctx, false)
		h.advancePastTimeout(ctx)
		h.settleDial(ctx, true)
		h.fire(ctx, Ping) // Heartbeat -> Missed (recorded in deep history on Drop)
		h.fire(ctx, Drop)
		h.fire(ctx, Reconnect) // restore [Missed, WorkIdle] via deep history
	}
}

// BenchmarkActor measures the actor host-driver hot path: spawning a worker actor
// (Assign) and delivering the finishing message that steps it to completion and
// routes its done event back through the parent.
func BenchmarkActor(b *testing.B) {
	ctx := context.Background()

	b.Run("spawn", func(b *testing.B) {
		// Measure spawn alone: drive to the live configuration, then Assign (which
		// spawns the worker) each iteration on a fresh harness.
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h := connectedHarness(ctx)
			h.fire(ctx, Assign)
		}
	})

	b.Run("dispatch", func(b *testing.B) {
		// Measure spawn + dispatch + delivery + completion routing together.
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h := connectedHarness(ctx)
			h.fire(ctx, Assign)
			h.runWorkers(ctx)
		}
	})
}

// BenchmarkAfter measures the delayed-transition schedule + fire cycle through the
// Scheduler and FakeClock: failing the dial arms a Backoff timer, and advancing
// the clock fires the delayed Retry edge.
func BenchmarkAfter(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := newConnHarness()
		h.fire(ctx, Connect)
		h.settleDial(ctx, false)  // -> Backoff, arm the timer
		h.advancePastTimeout(ctx) // advance + Tick: fire the delayed Retry
	}
}

// BenchmarkInvoke measures the invoke start + settle cycle: entering Connecting
// emits StartService (start), and settling the in-flight dial fires onDone/onError
// back through Fire (settle). The done sub-benchmark settles successfully; the
// error sub-benchmark routes onError.
func BenchmarkInvoke(b *testing.B) {
	ctx := context.Background()

	b.Run("settle-done", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h := newConnHarness()
			h.fire(ctx, Connect) // start the dial service
			// settleDial drives SettleDone directly, so the in-flight dial routes
			// onDone deterministically regardless of the attempt count.
			h.settleDial(ctx, true)
		}
	})

	b.Run("settle-error", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h := newConnHarness()
			h.fire(ctx, Connect)
			h.settleDial(ctx, false) // route onError -> Backoff
		}
	})
}

// BenchmarkSnapshotRestore measures the snapshot capture + JSON round-trip +
// Restore cycle on a mid-run instance in a live parallel configuration.
func BenchmarkSnapshotRestore(b *testing.B) {
	ctx := context.Background()
	h := connectedHarness(ctx)
	h.fire(ctx, Assign) // live parallel configuration with history to capture
	m := buildConnMachine()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := json.Marshal(h.inst.Snapshot())
		if err != nil {
			b.Fatal(err)
		}
		var snap state.Snapshot[Conn, ConnEvent, Link]
		if err = json.Unmarshal(data, &snap); err != nil {
			b.Fatal(err)
		}
		if _, rerr := m.Restore(snap); rerr != nil {
			b.Fatal(rerr)
		}
	}
}

// connectedHarness returns a harness driven to the live parallel Connected
// configuration (Beating + WorkIdle), the shared starting point for the actor and
// snapshot benchmarks.
func connectedHarness(ctx context.Context) *connHarness {
	h := newConnHarness()
	h.fire(ctx, Connect)
	h.settleDial(ctx, false)
	h.advancePastTimeout(ctx)
	h.settleDial(ctx, true)
	return h
}

// benchCtx is the entity for the deep-nested benchmark machine; it carries no
// state, the transitions being structural.
type benchCtx struct{}

// buildNestedBenchMachine forges a deep hierarchy (root > l1 > l2 > l3a/l3b) so a
// single "cross" event fires a transition that exits l3a up to l1 and re-enters
// down to l3b, exercising the multi-level exit/entry cascade.
func buildNestedBenchMachine() *state.Machine[string, string, *benchCtx] {
	return state.Forge[string, string, *benchCtx]("nested").
		SuperState("root").
		Initial("l1").
		SuperState("l1").
		Initial("l2a").
		SuperState("l2a").
		Initial("l3a").
		SubState("l3a").
		On("cross").GoTo("l3b").
		EndSuperState().
		SuperState("l2b").
		Initial("l3b").
		SubState("l3b").
		EndSuperState().
		EndSuperState().
		EndSuperState().
		Initial("root").
		CurrentStateFn(func(*benchCtx) string { return "l3a" }).
		Quench()
}
