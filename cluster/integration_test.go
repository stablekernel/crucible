package cluster_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
)

// TestIntegration_RemoteSpawnDeliverSupervise exercises the distribution runtime
// end to end across two nodes: node-a spawns a supervised worker on node-b, drives
// it remotely, the worker fails, node-b's supervisor restarts it within budget, and
// node-a keeps driving the same ref — all without node-a knowing where the worker
// runs.
func TestIntegration_RemoteSpawnDeliverSupervise(t *testing.T) {
	ctx := context.Background()
	tr := cluster.NewInMemoryTransport()

	// node-b hosts workers and supervises them with a restart budget.
	parentB := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	actorSysB := state.NewActorSystem(parentB).Register("worker", pingBehavior())
	nodeB := cluster.NewSystem("node-b", actorSysB, cluster.WithTransport(tr))
	sup := cluster.NewSupervisor(cluster.WithRestart("worker", 2))
	sup.SetRespawner(nodeB)
	actorSysB.WithEscalationHandler(sup.Handle)

	// node-a only routes.
	parentA := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	nodeA := cluster.NewSystem("node-a", state.NewActorSystem(parentA), cluster.WithTransport(tr))

	tr.Register("node-a", nodeA)
	tr.Register("node-b", nodeB)

	// node-a spawns a worker on node-b and drives it through the returned ref.
	ref, err := nodeA.Spawn(ctx, "node-b", "worker", "w-1", nil)
	if err != nil {
		t.Fatalf("remote spawn: %v", err)
	}
	if ref.Node != "node-b" {
		t.Fatalf("ref Node = %q, want node-b", ref.Node)
	}
	if delivered, err := nodeA.Deliver(ctx, ref, "ping"); err != nil || !delivered {
		t.Fatalf("remote deliver = (%v, %v)", delivered, err)
	}

	// The worker fails on node-b; the supervisor restarts it within budget.
	actorSysB.SettleError(ctx, "w-1", errors.New("boom"))
	if nodeB.Running() != 1 {
		t.Fatalf("node-b Running() after supervised restart = %d, want 1", nodeB.Running())
	}
	if got := sup.Handled(); len(got) != 1 || got[0].Decision != cluster.Restart {
		t.Fatalf("supervisor Handled = %+v, want one Restart", got)
	}

	// node-a drives the restarted worker through the same opaque ref.
	if delivered, err := nodeA.Deliver(ctx, ref, "ping"); err != nil || !delivered {
		t.Fatalf("deliver after restart = (%v, %v)", delivered, err)
	}
}

// TestIntegration_MigrateAcrossNodes captures an instance running on one node and
// restores it onto an additively-evolved machine on another, preserving its
// configuration and actors.
func TestIntegration_MigrateAcrossNodes(t *testing.T) {
	ctx := context.Background()

	// Source: an instance in state b with a running actor.
	inst := migSource().Cast(&migEnt{Step: 7}, state.WithInitialState("a"))
	inst.Fire(ctx, "go")
	srcSys := state.NewActorSystem(inst).Register("child", childBehavior())
	srcSys.Absorb(ctx, []state.Effect{state.SpawnActor{ID: "w", Src: state.Ref{Name: "child"}}})

	cp, err := cluster.Capture(inst, srcSys, migSource())
	if err != nil {
		t.Fatalf("capture: %v", err)
	}

	// Target node runs an additively-evolved machine; the migration is allowed and
	// the instance resumes in state b with its actor.
	got, sys, err := cluster.Restore(ctx, cp, migAdditive(),
		cluster.WithActorBehaviors(map[string]state.ActorBehavior{"child": childBehavior()}))
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got.Current() != "b" || got.Entity().Step != 7 {
		t.Fatalf("migrated instance = (%q, step %d), want (b, 7)", got.Current(), got.Entity().Step)
	}
	if _, ok := sys.Ref("w"); !ok {
		t.Fatal("migrated actor w not present on target")
	}
}
