package cluster_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
)

// TestInMemoryTransport_RemoteDelivery wires two node-scoped Systems through one
// in-memory transport and delivers from node-a to an actor running on node-b. The
// holder on node-a addresses the actor through an opaque ref whose Node names
// node-b; the System routes the delivery over the transport, node-b delegates it
// to its local ActorSystem, and the actor advances — all without node-a holding
// the actor or knowing anything but the ref.
func TestInMemoryTransport_RemoteDelivery(t *testing.T) {
	ctx := context.Background()

	// node-b runs the worker locally.
	sysB, localRef := spawnedSystem(t, "node-b")
	if sysB.Running() != 1 {
		t.Fatalf("node-b Running() = %d, want 1", sysB.Running())
	}

	// node-a holds no local actor; it only routes.
	tr := cluster.NewInMemoryTransport()
	parentA := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	sysA := cluster.NewSystem("node-a", state.NewActorSystem(parentA), cluster.WithTransport(tr))

	tr.Register("node-a", sysA)
	tr.Register("node-b", sysB)

	// node-a addresses node-b's worker through a remote ref (same id, owning node
	// stamped). The "finish" event drives the worker to its final state.
	remote := state.ActorRef{ID: localRef.ID, Src: localRef.Src, Node: "node-b"}
	delivered, err := sysA.Deliver(ctx, remote, "finish")
	if err != nil {
		t.Fatalf("remote Deliver error = %v, want nil", err)
	}
	if !delivered {
		t.Fatal("remote Deliver = false, want true")
	}
	// The worker reached its final state and was reaped on node-b.
	if sysB.Running() != 0 {
		t.Fatalf("node-b Running() after remote finish = %d, want 0", sysB.Running())
	}
}

// TestInMemoryTransport_UnknownNode reports a transport-level failure when a ref
// names a node the transport has never seen.
func TestInMemoryTransport_UnknownNode(t *testing.T) {
	ctx := context.Background()
	tr := cluster.NewInMemoryTransport()
	parentA := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	sysA := cluster.NewSystem("node-a", state.NewActorSystem(parentA), cluster.WithTransport(tr))
	tr.Register("node-a", sysA)

	ghost := state.ActorRef{ID: "worker-1", Node: "node-z"}
	delivered, err := sysA.Deliver(ctx, ghost, "finish")
	if !errors.Is(err, cluster.ErrNodeUnreachable) {
		t.Fatalf("Deliver to unknown node err = %v, want ErrNodeUnreachable", err)
	}
	if delivered {
		t.Fatal("Deliver to unknown node delivered = true, want false")
	}
}

// TestInMemoryTransport_DirectDeliverUnknownActor confirms that reaching a known
// node that has no such actor returns (false, nil): the node was reached, it just
// had nothing to deliver to.
func TestInMemoryTransport_DirectDeliverUnknownActor(t *testing.T) {
	ctx := context.Background()
	sysB, _ := spawnedSystem(t, "node-b")
	tr := cluster.NewInMemoryTransport()
	tr.Register("node-b", sysB)

	missing := state.ActorRef{ID: "no-such-actor", Node: "node-b"}
	delivered, err := tr.Deliver(ctx, missing, "finish")
	if err != nil {
		t.Fatalf("Deliver to missing actor on known node err = %v, want nil", err)
	}
	if delivered {
		t.Fatal("Deliver to missing actor = true, want false")
	}
}
