package transport_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type ttEnt struct {
	N int `json:"n"`
}

// ttMachine advances a --next--> b --next--> c so an instance has distinct states
// at distinct recorded steps for time-travel to reconstruct.
func ttMachine() *state.Machine[string, string, *ttEnt] {
	return state.Forge[string, string, *ttEnt]("counter").
		State("a").
		State("b").
		State("c").Final().
		Initial("a").
		Transition("a").On("next").GoTo("b").
		Transition("b").On("next").GoTo("c").
		Quench()
}

// TestTimeTravel_RemoteStateAt records a durable instance on node-b, then from
// node-a reconstructs its state at earlier recorded steps over gRPC.
func TestTimeTravel_RemoteStateAt(t *testing.T) {
	ctx := context.Background()
	m := ttMachine()
	store := durable.NewMemStore(durable.WithHistory())
	runner := durable.NewRunner(m, store)

	const id = "counter-1"
	h, err := runner.Start(ctx, id, &ttEnt{}, state.WithInitialState("a"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err = h.Fire(ctx, "next"); err != nil { // -> b
		t.Fatalf("fire 1: %v", err)
	}
	if _, err = h.Fire(ctx, "next"); err != nil { // -> c
		t.Fatalf("fire 2: %v", err)
	}

	steps, err := durable.Steps(ctx, store, id)
	if err != nil || len(steps) < 2 {
		t.Fatalf("steps = %v, err = %v; want at least 2", steps, err)
	}

	// node-b serves time-travel over gRPC against its durable store.
	ep := transport.NewDurableTimeTravel(m, store)
	lis := bufconn.Listen(1 << 20)
	gs := transport.NewTimeTravelServer(ep)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	tr := transport.New()
	tr.AddNode("node-b", dialBuf(t, lis))

	// The final recorded step reconstructs state c; the prior step reconstructs b.
	last := steps[len(steps)-1]
	prev := steps[len(steps)-2]

	assertStateAt := func(step int, want string) {
		t.Helper()
		raw, err := tr.StateAt(ctx, "node-b", id, step)
		if err != nil {
			t.Fatalf("remote StateAt(%d): %v", step, err)
		}
		snap, err := state.UnmarshalSnapshot[string, string, *ttEnt](raw)
		if err != nil {
			t.Fatalf("unmarshal snapshot: %v", err)
		}
		if snap.Current != want {
			t.Fatalf("state at step %d = %q, want %q", step, snap.Current, want)
		}
	}
	assertStateAt(last, "c")
	assertStateAt(prev, "b")
}

// TestTimeTravel_ServerInterceptor confirms a time-travel query works through a
// server built with a unary interceptor (the interceptor branch of the handler).
func TestTimeTravel_ServerInterceptor(t *testing.T) {
	ctx := context.Background()
	m := ttMachine()
	store := durable.NewMemStore(durable.WithHistory())
	h, err := durable.NewRunner(m, store).Start(ctx, "c-int", &ttEnt{}, state.WithInitialState("a"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err = h.Fire(ctx, "next"); err != nil {
		t.Fatalf("fire: %v", err)
	}

	var calls int
	interceptor := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		calls++
		return handler(ctx, req)
	}
	lis := bufconn.Listen(1 << 20)
	gs := transport.NewTimeTravelServer(transport.NewDurableTimeTravel(m, store), grpc.ChainUnaryInterceptor(interceptor))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	tr := transport.New()
	tr.AddNode("node-b", dialBuf(t, lis))
	steps, _ := durable.Steps(ctx, store, "c-int")
	if _, err := tr.StateAt(ctx, "node-b", "c-int", steps[len(steps)-1]); err != nil {
		t.Fatalf("StateAt through interceptor: %v", err)
	}
	if calls != 1 {
		t.Fatalf("interceptor saw %d calls, want 1", calls)
	}
}

// TestTimeTravel_UnknownNode reports an error when the target node is not
// registered with the transport.
func TestTimeTravel_UnknownNode(t *testing.T) {
	tr := transport.New()
	if _, err := tr.StateAt(context.Background(), "ghost", "x", 1); err == nil {
		t.Fatal("StateAt on unregistered node = nil error, want unreachable")
	}
}

// TestTimeTravel_UnknownInstance surfaces a server-side reconstruction error (no
// such instance) back to the client over the wire.
func TestTimeTravel_UnknownInstance(t *testing.T) {
	ctx := context.Background()
	m := ttMachine()
	ep := transport.NewDurableTimeTravel(m, durable.NewMemStore(durable.WithHistory()))
	lis := bufconn.Listen(1 << 20)
	gs := transport.NewTimeTravelServer(ep)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	tr := transport.New()
	tr.AddNode("node-b", dialBuf(t, lis))
	if _, err := tr.StateAt(ctx, "node-b", "no-such-instance", 1); err == nil {
		t.Fatal("StateAt for unknown instance = nil error, want a wire error")
	}
}
