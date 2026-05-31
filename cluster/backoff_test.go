package cluster_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
)

// backoffSystem builds a supervised System wired to a fake clock and one running
// worker, returning the System, the supervisor, and the clock so a test can fail
// the actor, advance time, and Tick.
func backoffSystem(t *testing.T, sup *cluster.Supervisor) *cluster.System[string, string, *parentEnt] {
	t.Helper()
	parent := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	actorSys := state.NewActorSystem(parent).
		Register("child", childBehavior()).
		WithEscalationHandler(sup.Handle)
	ctx := context.Background()
	res := parent.Fire(ctx, "go")
	actorSys.Absorb(ctx, res.Effects)
	sys := cluster.NewSystem("node-a", actorSys)
	sup.SetRespawner(sys)
	if sys.Running() != 1 {
		t.Fatalf("setup Running() = %d, want 1", sys.Running())
	}
	return sys
}

// TestSupervisor_BackoffDefersRestart confirms a Backoff decision does not restart
// immediately: the actor stays down until its delay elapses and Tick is called.
func TestSupervisor_BackoffDefersRestart(t *testing.T) {
	ctx := context.Background()
	clock := state.NewFakeClock(time.Unix(0, 0))
	sup := cluster.NewSupervisor(
		cluster.WithBackoff("child", 3, 100*time.Millisecond, time.Second, 2.0),
		cluster.WithClock(clock),
	)
	sys := backoffSystem(t, sup)

	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	// Deferred: not restarted yet, and not due.
	if sys.Running() != 0 {
		t.Fatalf("Running() right after failure = %d, want 0 (deferred)", sys.Running())
	}
	if n := sup.Tick(ctx); n != 0 {
		t.Fatalf("Tick before delay restarted %d, want 0", n)
	}
	if sys.Running() != 0 {
		t.Fatalf("Running() before delay = %d, want 0", sys.Running())
	}

	// Once the initial delay elapses, Tick reinstates the actor.
	clock.Advance(100 * time.Millisecond)
	if n := sup.Tick(ctx); n != 1 {
		t.Fatalf("Tick after delay restarted %d, want 1", n)
	}
	if sys.Running() != 1 {
		t.Fatalf("Running() after due Tick = %d, want 1", sys.Running())
	}
}

// TestSupervisor_BackoffGrowsDelay confirms the delay grows by the factor each
// restart: the second restart is not due until the longer delay elapses.
func TestSupervisor_BackoffGrowsDelay(t *testing.T) {
	ctx := context.Background()
	clock := state.NewFakeClock(time.Unix(0, 0))
	sup := cluster.NewSupervisor(
		cluster.WithBackoff("child", 3, 100*time.Millisecond, time.Second, 2.0),
		cluster.WithClock(clock),
	)
	sys := backoffSystem(t, sup)

	// First failure: 100ms delay.
	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	clock.Advance(100 * time.Millisecond)
	sup.Tick(ctx)
	if sys.Running() != 1 {
		t.Fatalf("after first backoff Running() = %d, want 1", sys.Running())
	}

	// Second failure: 200ms delay — 100ms is NOT enough.
	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	clock.Advance(100 * time.Millisecond)
	if n := sup.Tick(ctx); n != 0 {
		t.Fatalf("Tick at 100ms into a 200ms backoff restarted %d, want 0", n)
	}
	clock.Advance(100 * time.Millisecond) // now 200ms total
	if n := sup.Tick(ctx); n != 1 {
		t.Fatalf("Tick at 200ms restarted %d, want 1", n)
	}
}

// TestSupervisor_BackoffBudgetExhausts confirms backoff stops after the budget and
// then escalates.
func TestSupervisor_BackoffBudgetExhausts(t *testing.T) {
	ctx := context.Background()
	clock := state.NewFakeClock(time.Unix(0, 0))
	var escalated int
	sup := cluster.NewSupervisor(
		cluster.WithBackoff("child", 1, 10*time.Millisecond, time.Second, 2.0),
		cluster.WithClock(clock),
		cluster.WithEscalationSink(func(context.Context, *state.ActorEscalation) { escalated++ }),
	)
	sys := backoffSystem(t, sup)

	// First failure consumes the only restart.
	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	clock.Advance(10 * time.Millisecond)
	sup.Tick(ctx)
	if sys.Running() != 1 {
		t.Fatalf("after first backoff Running() = %d, want 1", sys.Running())
	}
	// Second failure exhausts the budget and escalates instead of scheduling.
	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	if escalated != 1 {
		t.Fatalf("escalations after budget = %d, want 1", escalated)
	}
	if n := sup.Tick(ctx); n != 0 {
		t.Fatalf("Tick after exhaustion restarted %d, want 0", n)
	}
}

// TestSupervisor_DefaultClock confirms a Supervisor with no WithClock uses a real
// clock and Tick is safe with nothing pending.
func TestSupervisor_DefaultClock(t *testing.T) {
	sup := cluster.NewSupervisor()
	if n := sup.Tick(context.Background()); n != 0 {
		t.Fatalf("Tick with nothing pending = %d, want 0", n)
	}
}
