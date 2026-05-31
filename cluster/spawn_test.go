package cluster_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
)

// registeredSystem builds a node-scoped System that knows the "child" behavior
// but has spawned nothing yet — the starting point for an imperative spawn.
func registeredSystem(node string, opts ...cluster.Option) *cluster.System[string, string, *parentEnt] {
	parent := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	actorSys := state.NewActorSystem(parent).Register("child", childBehavior())
	return cluster.NewSystem(node, actorSys, opts...)
}

// TestSystem_SpawnLocal spawns an actor imperatively on the local node and
// confirms the returned ref is stamped with this node and is locally usable.
func TestSystem_SpawnLocal(t *testing.T) {
	ctx := context.Background()
	sys := registeredSystem("node-a")

	ref, err := sys.Spawn(ctx, "node-a", "child", "w1", nil)
	if err != nil {
		t.Fatalf("local Spawn error = %v", err)
	}
	if ref.ID != "w1" || ref.Node != "node-a" {
		t.Fatalf("ref = %+v, want ID=w1 Node=node-a", ref)
	}
	if sys.Running() != 1 {
		t.Fatalf("Running() = %d, want 1", sys.Running())
	}
	// An empty node argument is also local.
	if _, err := sys.Spawn(ctx, "", "child", "w2", nil); err != nil {
		t.Fatalf("empty-node Spawn error = %v", err)
	}
	if sys.Running() != 2 {
		t.Fatalf("Running() after second local spawn = %d, want 2", sys.Running())
	}
}

// TestSystem_SpawnRemote asks another node to spawn an actor and then drives it
// from the requesting node through the returned remote ref.
func TestSystem_SpawnRemote(t *testing.T) {
	ctx := context.Background()
	tr := cluster.NewInMemoryTransport()

	sysB := registeredSystem("node-b")
	sysA := registeredSystem("node-a", cluster.WithTransport(tr))
	tr.Register("node-a", sysA)
	tr.Register("node-b", sysB)

	ref, err := sysA.Spawn(ctx, "node-b", "child", "worker-remote", nil)
	if err != nil {
		t.Fatalf("remote Spawn error = %v", err)
	}
	if ref.Node != "node-b" {
		t.Fatalf("remote ref Node = %q, want node-b", ref.Node)
	}
	if sysB.Running() != 1 {
		t.Fatalf("node-b Running() after remote spawn = %d, want 1", sysB.Running())
	}

	// The requesting node drives the remote actor to completion through the ref.
	delivered, err := sysA.Deliver(ctx, ref, "finish")
	if err != nil || !delivered {
		t.Fatalf("Deliver to remote-spawned actor = (%v, %v)", delivered, err)
	}
	if sysB.Running() != 0 {
		t.Fatalf("node-b Running() after finish = %d, want 0", sysB.Running())
	}
}

// TestSystem_SpawnRemoteNoTransport reports ErrNoTransport when a remote spawn is
// requested with no transport configured.
func TestSystem_SpawnRemoteNoTransport(t *testing.T) {
	ctx := context.Background()
	sys := registeredSystem("node-a")
	if _, err := sys.Spawn(ctx, "node-b", "child", "w", nil); !errors.Is(err, cluster.ErrNoTransport) {
		t.Fatalf("remote Spawn without transport err = %v, want ErrNoTransport", err)
	}
}

// TestSystem_SpawnRemoteUnknownNode surfaces ErrNodeUnreachable when the target
// node was never registered with the transport.
func TestSystem_SpawnRemoteUnknownNode(t *testing.T) {
	ctx := context.Background()
	tr := cluster.NewInMemoryTransport()
	sysA := registeredSystem("node-a", cluster.WithTransport(tr))
	tr.Register("node-a", sysA)

	if _, err := sysA.Spawn(ctx, "ghost", "child", "w", nil); !errors.Is(err, cluster.ErrNodeUnreachable) {
		t.Fatalf("remote Spawn to unknown node err = %v, want ErrNodeUnreachable", err)
	}
}

// TestSystem_SpawnUnknownBehavior fails when the target node has no behavior
// registered under the requested src.
func TestSystem_SpawnUnknownBehavior(t *testing.T) {
	ctx := context.Background()
	sys := registeredSystem("node-a")
	if _, err := sys.Spawn(ctx, "node-a", "no-such-src", "w", nil); err == nil {
		t.Fatal("Spawn from unknown src = nil error, want a failure")
	}
}
