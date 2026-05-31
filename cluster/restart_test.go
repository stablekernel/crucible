package cluster_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
)

// superviseSystem builds a node-scoped System whose local ActorSystem routes
// failures to sup, with one worker spawned and running. It returns the System so
// a test can fail and observe restarts.
func superviseSystem(t *testing.T, sup *cluster.Supervisor) *cluster.System[string, string, *parentEnt] {
	t.Helper()
	parent := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	actorSys := state.NewActorSystem(parent).
		Register("child", childBehavior()).
		WithEscalationHandler(sup.Handle)
	ctx := context.Background()
	res := parent.Fire(ctx, "go")
	actorSys.Absorb(ctx, res.Effects)
	sys := cluster.NewSystem("node-a", actorSys)
	if sys.Running() != 1 {
		t.Fatalf("setup Running() = %d, want 1", sys.Running())
	}
	return sys
}

// TestSupervisor_RestartReinstatesActor confirms a Restart decision re-spawns the
// failed actor, so the actor is running again after the failure.
func TestSupervisor_RestartReinstatesActor(t *testing.T) {
	ctx := context.Background()
	sup := cluster.NewSupervisor(cluster.WithRestart("child", 3))
	// Wire the respawner after the system exists.
	sys := superviseSystemWithRespawner(t, sup)

	if _, routed := sys.Local().SettleError(ctx, "worker-1", errors.New("boom")); routed {
		t.Fatal("SettleError routed an onError unexpectedly")
	}
	if sys.Running() != 1 {
		t.Fatalf("Running() after restart = %d, want 1 (actor reinstated)", sys.Running())
	}
	handled := sup.Handled()
	if len(handled) != 1 || handled[0].Decision != cluster.Restart {
		t.Fatalf("Handled() = %+v, want one Restart", handled)
	}
}

// superviseSystemWithRespawner is like superviseSystem but binds the System as the
// supervisor's respawner via WithRespawner, set before failures occur.
func superviseSystemWithRespawner(t *testing.T, sup *cluster.Supervisor) *cluster.System[string, string, *parentEnt] {
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

// TestSupervisor_RestartBudgetExhausts confirms restarts stop after the per-src
// budget is spent and the failure then escalates to the sink.
func TestSupervisor_RestartBudgetExhausts(t *testing.T) {
	ctx := context.Background()
	var escalated int
	sup := cluster.NewSupervisor(
		cluster.WithRestart("child", 2),
		cluster.WithEscalationSink(func(context.Context, *state.ActorEscalation) { escalated++ }),
	)
	sys := superviseSystemWithRespawner(t, sup)

	// Two failures are absorbed by restarts; the actor is running each time.
	for i := range 2 {
		sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
		if sys.Running() != 1 {
			t.Fatalf("after failure %d Running() = %d, want 1 (restarted)", i+1, sys.Running())
		}
	}
	if escalated != 0 {
		t.Fatalf("escalated %d times before budget exhausted, want 0", escalated)
	}

	// The third failure exhausts the budget: no restart, escalates instead.
	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	if sys.Running() != 0 {
		t.Fatalf("after budget exhausted Running() = %d, want 0 (not restarted)", sys.Running())
	}
	if escalated != 1 {
		t.Fatalf("escalations after exhaustion = %d, want 1", escalated)
	}
}

// TestSupervisor_WithRespawnerOption wires the respawner at construction (the
// build-system-first ordering) instead of via SetRespawner, and confirms restart
// still reinstates the actor.
func TestSupervisor_WithRespawnerOption(t *testing.T) {
	ctx := context.Background()
	parent := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	actorSys := state.NewActorSystem(parent).Register("child", childBehavior())
	sys := cluster.NewSystem("node-a", actorSys)
	sup := cluster.NewSupervisor(cluster.WithRestart("child", 1), cluster.WithRespawner(sys))
	actorSys.WithEscalationHandler(sup.Handle)

	res := parent.Fire(ctx, "go")
	actorSys.Absorb(ctx, res.Effects)
	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	if sys.Running() != 1 {
		t.Fatalf("Running() after restart = %d, want 1", sys.Running())
	}
}

// TestSupervisor_RestartNoRespawner falls through to escalation when a Restart
// decision is configured but no respawner is wired.
func TestSupervisor_RestartNoRespawner(t *testing.T) {
	ctx := context.Background()
	var escalated int
	sup := cluster.NewSupervisor(
		cluster.WithRestart("child", 3),
		cluster.WithEscalationSink(func(context.Context, *state.ActorEscalation) { escalated++ }),
	)
	sys := superviseSystem(t, sup) // no respawner bound

	sys.Local().SettleError(ctx, "worker-1", errors.New("boom"))
	if escalated != 1 {
		t.Fatalf("escalations without respawner = %d, want 1 (fell through)", escalated)
	}
	if sys.Running() != 0 {
		t.Fatalf("Running() = %d, want 0 (no restart possible)", sys.Running())
	}
}
