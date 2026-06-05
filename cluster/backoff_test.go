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

// TestSupervisor_TickNilRespawnerNoPanic confirms a Tick whose Respawner was
// cleared after a backoff was scheduled does not panic: the due restart is a no-op
// and stays pending so a Respawner wired again later still applies it.
func TestSupervisor_TickNilRespawnerNoPanic(t *testing.T) {
	ctx := context.Background()
	clock := state.NewFakeClock(time.Unix(0, 0))
	sup := cluster.NewSupervisor(
		cluster.WithBackoff("child", 3, 100*time.Millisecond, time.Second, 2.0),
		cluster.WithClock(clock),
	)
	sys := backoffSystem(t, sup)

	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	clock.Advance(100 * time.Millisecond)

	// Clear the respawner between scheduling and the due Tick.
	sup.SetRespawner(nil)
	if n := sup.Tick(ctx); n != 0 {
		t.Fatalf("Tick with a cleared respawner restarted %d, want 0", n)
	}

	// The pending restart was preserved: rewiring a respawner and ticking applies it.
	sup.SetRespawner(sys)
	if n := sup.Tick(ctx); n != 1 {
		t.Fatalf("Tick after rewiring the respawner restarted %d, want 1", n)
	}
}

// TestSupervisor_ForgetEvictsBookkeeping confirms Forget drops an actor's spent
// restart counter and any pending restart, bounding the restarts map under churn.
// After Forget a re-spawn of the same id earns a fresh restart budget.
func TestSupervisor_ForgetEvictsBookkeeping(t *testing.T) {
	ctx := context.Background()
	clock := state.NewFakeClock(time.Unix(0, 0))
	sup := cluster.NewSupervisor(
		cluster.WithRestart("child", 1),
		cluster.WithClock(clock),
	)
	sys := backoffSystem(t, sup)

	// Spend the single restart budget.
	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	if sys.Running() != 1 {
		t.Fatalf("after first (restart) failure Running() = %d, want 1", sys.Running())
	}
	// Budget spent: a second failure escalates rather than restarts.
	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	if sys.Running() != 0 {
		t.Fatalf("after budget-exhausting failure Running() = %d, want 0", sys.Running())
	}

	// Forget the actor (a genuine teardown), then re-spawn it: it earns a fresh
	// budget, so the next failure restarts again rather than escalating.
	sup.Forget("worker-1")
	if _, err := sys.Respawn(ctx, "child", "worker-1", nil); err != nil {
		t.Fatalf("Respawn after Forget: %v", err)
	}
	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	if sys.Running() != 1 {
		t.Fatalf("after Forget+respawn, a failure should restart within the fresh budget; Running() = %d, want 1", sys.Running())
	}
}

// TestSupervisor_ForgetPendingBackoff confirms Forget removes a not-yet-due backoff
// restart, so a forgotten actor is not restarted by a later Tick.
func TestSupervisor_ForgetPendingBackoff(t *testing.T) {
	ctx := context.Background()
	clock := state.NewFakeClock(time.Unix(0, 0))
	sup := cluster.NewSupervisor(
		cluster.WithBackoff("child", 3, 100*time.Millisecond, time.Second, 2.0),
		cluster.WithClock(clock),
	)
	sys := backoffSystem(t, sup)

	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	sup.Forget("worker-1") // drop the scheduled restart before it is due

	clock.Advance(time.Second)
	if n := sup.Tick(ctx); n != 0 {
		t.Fatalf("Tick after forgetting the pending restart restarted %d, want 0", n)
	}
}
