package transport_test

import (
	"context"
	"net"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// --- self-contained test fixtures (cluster's test helpers are package-private) ---

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
		return state.NewActor(cm.Cast(&childEnt{}, state.WithInitialState("working")), nil), nil
	}
}

type hostEnt struct{}

func hostMachine() *state.Machine[string, string, *hostEnt] {
	return state.Forge[string, string, *hostEnt]("host").
		State("idle").
		Initial("idle").
		Quench()
}

func newNodeSystem(node string, opts ...cluster.Option) *cluster.System[string, string, *hostEnt] {
	parent := hostMachine().Cast(&hostEnt{}, state.WithInitialState("idle"))
	return cluster.NewSystem(node, state.NewActorSystem(parent).Register("worker", childBehavior()), opts...)
}

// dialBuf wires a client connection to a bufconn-served gRPC server.
func dialBuf(t *testing.T, lis *bufconn.Listener) grpc.ClientConnInterface {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestTransport_RemoteSpawnAndDeliverOverGRPC runs the cluster multi-node scenario
// over a real gRPC connection (in-memory via bufconn): node-a spawns a worker on
// node-b and drives it to completion through the returned ref, with every operation
// crossing the gRPC wire and being decoded into node-b's concrete event type.
func TestTransport_RemoteSpawnAndDeliverOverGRPC(t *testing.T) {
	ctx := context.Background()

	// node-b serves its WireEndpoint over gRPC on a bufconn listener.
	nodeB := newNodeSystem("node-b")
	lis := bufconn.Listen(1 << 20)
	gs := transport.NewServer(nodeB)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	// node-a routes to node-b through the gRPC client transport.
	tr := transport.New()
	tr.AddNode("node-b", dialBuf(t, lis))
	nodeA := newNodeSystem("node-a", cluster.WithTransport(tr))

	// Spawn a worker on node-b over gRPC.
	ref, err := nodeA.Spawn(ctx, "node-b", "worker", "w-1", nil)
	if err != nil {
		t.Fatalf("remote spawn over grpc: %v", err)
	}
	if ref.Node != "node-b" || ref.ID != "w-1" {
		t.Fatalf("ref = %+v, want ID=w-1 Node=node-b", ref)
	}
	if nodeB.Running() != 1 {
		t.Fatalf("node-b Running() after grpc spawn = %d, want 1", nodeB.Running())
	}

	// Deliver to it over gRPC; the string event round-trips and the worker completes.
	delivered, err := nodeA.Deliver(ctx, ref, "finish")
	if err != nil {
		t.Fatalf("remote deliver over grpc: %v", err)
	}
	if !delivered {
		t.Fatal("remote deliver over grpc = false, want true")
	}
	if nodeB.Running() != 0 {
		t.Fatalf("node-b Running() after grpc finish = %d, want 0", nodeB.Running())
	}
}

// TestTransport_UnknownNode reports the node as unreachable when it was never
// registered with the transport.
func TestTransport_UnknownNode(t *testing.T) {
	ctx := context.Background()
	tr := transport.New()
	if _, err := tr.Deliver(ctx, state.ActorRef{ID: "x", Node: "ghost"}, "e"); err == nil {
		t.Fatal("Deliver to unregistered node = nil error, want unreachable")
	}
	if _, err := tr.Spawn(ctx, "ghost", "worker", "x", nil); err == nil {
		t.Fatal("Spawn on unregistered node = nil error, want unreachable")
	}
}

// TestTransport_ServerInterceptor confirms operations work through a server built
// with a unary interceptor (the interceptor branch of the method handlers), and
// that the interceptor observes each call.
func TestTransport_ServerInterceptor(t *testing.T) {
	ctx := context.Background()
	var calls int
	interceptor := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		calls++
		return handler(ctx, req)
	}
	nodeB := newNodeSystem("node-b")
	lis := bufconn.Listen(1 << 20)
	gs := transport.NewServer(nodeB, grpc.ChainUnaryInterceptor(interceptor))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	tr := transport.New()
	tr.AddNode("node-b", dialBuf(t, lis))
	nodeA := newNodeSystem("node-a", cluster.WithTransport(tr))

	ref, err := nodeA.Spawn(ctx, "node-b", "worker", "w-1", map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if _, err := nodeA.Deliver(ctx, ref, "finish"); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if calls != 2 {
		t.Fatalf("interceptor saw %d calls, want 2 (spawn + deliver)", calls)
	}
}

// TestTransport_RemoteSpawnError surfaces a server-side spawn failure (unknown src)
// back to the client as an error over the wire.
func TestTransport_RemoteSpawnError(t *testing.T) {
	ctx := context.Background()
	nodeB := newNodeSystem("node-b")
	lis := bufconn.Listen(1 << 20)
	gs := transport.NewServer(nodeB)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	tr := transport.New()
	tr.AddNode("node-b", dialBuf(t, lis))
	nodeA := newNodeSystem("node-a", cluster.WithTransport(tr))

	if _, err := nodeA.Spawn(ctx, "node-b", "no-such-src", "x", nil); err == nil {
		t.Fatal("remote spawn of unknown src = nil error, want a wire error")
	}
}

// TestTransport_SatisfiesClusterTransport is a compile-time check that *Transport
// is a cluster.Transport.
func TestTransport_SatisfiesClusterTransport(t *testing.T) {
	var _ cluster.Transport = transport.New()
}
