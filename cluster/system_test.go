package cluster_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
)

// --- minimal parent/child machines exercising a real local actor ---

type childEnt struct{}

func childMachine() *state.Machine[string, string, *childEnt] {
	return state.Forge[string, string, *childEnt]("worker").
		State("working").
		State("done").Final().
		Initial("working").
		Transition("working").On("finish").GoTo("done").
		Quench()
}

func childBehavior() state.ActorBehavior {
	cm := childMachine()
	return func(map[string]any) (state.ActorInstance, error) {
		inst := cm.Cast(&childEnt{}, state.WithInitialState("working"))
		return state.NewActor(inst, nil), nil
	}
}

type parentEnt struct{}

func parentMachine() *state.Machine[string, string, *parentEnt] {
	return state.Forge[string, string, *parentEnt]("spawner").
		State("idle").
		State("active").
		Initial("idle").
		Transition("idle").On("go").GoTo("active").
		Spawn("child", "worker-1", state.WithSpawnOnDone("workerDone")).
		Transition("active").On("workerDone").GoTo("idle").
		Quench()
}

// spawnedSystem builds a node-scoped System running one local actor (worker-1),
// returning the System and the actor's ref.
func spawnedSystem(t *testing.T, node string, opts ...cluster.Option) (*cluster.System[string, string, *parentEnt], state.ActorRef) {
	t.Helper()
	parent := parentMachine().Cast(&parentEnt{}, state.WithInitialState("idle"))
	actorSys := state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()
	res := parent.Fire(ctx, "go")
	actorSys.Absorb(ctx, res.Effects)

	sys := cluster.NewSystem(node, actorSys, opts...)
	ref, ok := sys.Ref("worker-1")
	if !ok {
		t.Fatal("local actor worker-1 was not spawned")
	}
	return sys, ref
}

func TestSystem_DeliverLocal(t *testing.T) {
	ctx := context.Background()
	sys, ref := spawnedSystem(t, "node-a")
	if sys.Node() != "node-a" {
		t.Fatalf("Node() = %q, want node-a", sys.Node())
	}
	if sys.Running() != 1 {
		t.Fatalf("Running() = %d, want 1", sys.Running())
	}
	// A local ref carries an empty Node and is delegated straight through.
	delivered, err := sys.Deliver(ctx, ref, "finish")
	if err != nil {
		t.Fatalf("Deliver(local) error = %v, want nil", err)
	}
	if !delivered {
		t.Fatal("Deliver(local) = false, want true")
	}
}

func TestSystem_DeliverExplicitLocalNode(t *testing.T) {
	ctx := context.Background()
	sys, ref := spawnedSystem(t, "node-a")
	// A ref stamped with this node's own identifier is still local.
	ref.Node = "node-a"
	delivered, err := sys.Deliver(ctx, ref, "finish")
	if err != nil || !delivered {
		t.Fatalf("Deliver(explicit-local) = (%v, %v), want (true, nil)", delivered, err)
	}
}

func TestSystem_RemoteRefNoTransport(t *testing.T) {
	ctx := context.Background()
	sys, ref := spawnedSystem(t, "node-a")
	ref.Node = "node-b" // owned elsewhere; no transport configured
	delivered, err := sys.Deliver(ctx, ref, "finish")
	if !errors.Is(err, cluster.ErrNoTransport) {
		t.Fatalf("Deliver(remote, no transport) err = %v, want ErrNoTransport", err)
	}
	if delivered {
		t.Fatal("Deliver(remote, no transport) delivered = true, want false")
	}
}

// recordingTransport is a test double that captures the routed delivery.
type recordingTransport struct {
	calls     int
	gotRef    state.ActorRef
	gotEvent  any
	delivered bool
	err       error
}

func (rt *recordingTransport) Deliver(_ context.Context, ref state.ActorRef, event any) (bool, error) {
	rt.calls++
	rt.gotRef = ref
	rt.gotEvent = event
	return rt.delivered, rt.err
}

func (rt *recordingTransport) Spawn(_ context.Context, node, _, id string, _ map[string]any) (state.ActorRef, error) {
	rt.calls++
	return state.ActorRef{ID: id, Node: node}, rt.err
}

func TestSystem_RemoteRefRoutesToTransport(t *testing.T) {
	ctx := context.Background()
	rt := &recordingTransport{delivered: true}
	sys, ref := spawnedSystem(t, "node-a", cluster.WithTransport(rt))
	ref.Node = "node-b"

	delivered, err := sys.Deliver(ctx, ref, "finish")
	if err != nil || !delivered {
		t.Fatalf("Deliver(remote) = (%v, %v), want (true, nil)", delivered, err)
	}
	if rt.calls != 1 || rt.gotRef.Node != "node-b" || rt.gotEvent != "finish" {
		t.Fatalf("transport got calls=%d ref=%+v event=%v", rt.calls, rt.gotRef, rt.gotEvent)
	}
}

func TestSystem_RemoteTransportError(t *testing.T) {
	ctx := context.Background()
	wantErr := errors.New("node unreachable")
	rt := &recordingTransport{err: wantErr}
	sys, ref := spawnedSystem(t, "node-a", cluster.WithTransport(rt))
	ref.Node = "node-b"

	if _, err := sys.Deliver(ctx, ref, "finish"); !errors.Is(err, wantErr) {
		t.Fatalf("Deliver(remote, transport err) = %v, want %v", err, wantErr)
	}
}

func TestSystem_LocalAccessorsAndStop(t *testing.T) {
	ctx := context.Background()
	sys, ref := spawnedSystem(t, "node-a")

	if sys.Local() == nil {
		t.Fatal("Local() = nil, want the wrapped ActorSystem")
	}
	if !sys.DeliverByID(ctx, "worker-1", "finish") {
		t.Fatal("DeliverByID(worker-1) = false, want true")
	}

	// Stopping a remote ref is a no-op (teardown is the owner's job); the local
	// actor keeps running.
	remote := ref
	remote.Node = "node-b"
	sys.Stop(remote)

	// Re-spawn a fresh local actor to exercise the local Stop path deterministically.
	sys2, ref2 := spawnedSystem(t, "node-a")
	sys2.Stop(ref2)
	if sys2.Running() != 0 {
		t.Fatalf("Running() after local Stop = %d, want 0", sys2.Running())
	}
}
