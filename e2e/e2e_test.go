package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
	"github.com/stablekernel/crucible/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// ---- durable ⊗ expr: a CEL-assign-driven durable instance that recovers ----

type order struct {
	Status string  `json:"status"`
	Total  float64 `json:"total"`
}

// discountMachine pays an order, applying a CEL rich assign that discounts the
// total. The machine is authored programmatically, serialized, then rebound to a
// registry carrying the compiled CEL assign — the production authoring flow.
func discountMachine(t *testing.T) *state.Machine[string, string, order] {
	t.Helper()
	reg := state.NewRegistry[order]()
	if err := expr.Assign(reg, "discount", `{"total": total * 0.9}`, state.SchemaOf[order]()); err != nil {
		t.Fatalf("author CEL assign: %v", err)
	}
	def := state.Forge[string, string, order]("order").
		Reducer("discount", func(in state.AssignCtx[order]) order { return in.Entity }).
		State("pending").
		State("paid").
		Initial("pending").
		Transition("pending").On("pay").GoTo("paid").Assign("discount").
		Quench()
	js, err := def.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, order](js)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	return ir.Provide(reg).Quench()
}

// TestE2E_DurableCELAssignSurvivesRecovery drives a durable instance through a
// CEL-assign transition, then recovers it from the store and confirms the
// CEL-computed context persisted — the assign is replayed deterministically.
func TestE2E_DurableCELAssignSurvivesRecovery(t *testing.T) {
	ctx := context.Background()
	m := discountMachine(t)
	store := durable.NewMemStore()
	runner := durable.NewRunner(m, store)

	const id = "order-1"
	h, err := runner.Start(ctx, id, order{Status: "pending", Total: 100}, state.WithInitialState("pending"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err = h.Fire(ctx, "pay"); err != nil {
		t.Fatalf("fire pay: %v", err)
	}
	if got := h.Instance().Entity().Total; got != 90 {
		t.Fatalf("after CEL assign, total = %v, want 90", got)
	}

	// Recover from the store: the CEL assign replays, yielding the same context.
	rec, err := durable.Recover(ctx, m, store, id)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := rec.Instance().Entity().Total; got != 90 {
		t.Fatalf("recovered total = %v, want 90 (CEL assign replayed)", got)
	}
	if got := rec.Instance().Current(); got != "paid" {
		t.Fatalf("recovered state = %q, want paid", got)
	}
}

// ---- cluster ⊗ transport ⊗ supervisor: supervised remote actor over gRPC ----

type pinger struct{}

func pingMachine() *state.Machine[string, string, *pinger] {
	return state.Forge[string, string, *pinger]("worker").
		State("up").
		Initial("up").
		Transition("up").On("ping").GoTo("up").
		Quench()
}

func pingBehavior() state.ActorBehavior {
	m := pingMachine()
	return func(map[string]any) (state.ActorInstance, error) {
		return state.NewActor(m.Cast(&pinger{}, state.WithInitialState("up")), nil), nil
	}
}

type host struct{}

func hostSystem(node string, opts ...cluster.Option) (*cluster.System[string, string, *host], *state.ActorSystem[string, string, *host]) {
	parent := state.Forge[string, string, *host]("host").State("idle").Initial("idle").Quench().
		Cast(&host{}, state.WithInitialState("idle"))
	as := state.NewActorSystem(parent).Register("worker", pingBehavior())
	return cluster.NewSystem(node, as, opts...), as
}

func dialBuf(t *testing.T, lis *bufconn.Listener) grpc.ClientConnInterface {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestE2E_SupervisedRemoteActorOverGRPC spawns a supervised worker on a remote node
// over real gRPC, drives it, fails it, lets the remote supervisor restart it within
// budget, and drives it again — exercising cluster, transport, and supervision
// together.
func TestE2E_SupervisedRemoteActorOverGRPC(t *testing.T) {
	ctx := context.Background()

	// node-b: hosts and supervises workers, served over gRPC.
	nodeB, asB := hostSystem("node-b")
	sup := cluster.NewSupervisor(cluster.WithRestart("worker", 2))
	sup.SetRespawner(nodeB)
	asB.WithEscalationHandler(sup.Handle)

	lis := bufconn.Listen(1 << 20)
	gs := transport.NewServer(nodeB)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	// node-a: routes over the gRPC transport.
	tr := transport.New()
	tr.AddNode("node-b", dialBuf(t, lis))
	nodeA, _ := hostSystem("node-a", cluster.WithTransport(tr))

	ref, err := nodeA.Spawn(ctx, "node-b", "worker", "w-1", nil)
	if err != nil {
		t.Fatalf("remote spawn: %v", err)
	}
	if delivered, err := nodeA.Deliver(ctx, ref, "ping"); err != nil || !delivered {
		t.Fatalf("remote deliver = (%v, %v)", delivered, err)
	}

	// Fail the worker on node-b; the supervisor restarts it within budget.
	asB.SettleError(ctx, "w-1", errors.New("boom"))
	if nodeB.Running() != 1 {
		t.Fatalf("after supervised restart, node-b Running() = %d, want 1", nodeB.Running())
	}
	// node-a drives the restarted worker again through the same opaque ref over gRPC.
	if delivered, err := nodeA.Deliver(ctx, ref, "ping"); err != nil || !delivered {
		t.Fatalf("deliver after restart = (%v, %v)", delivered, err)
	}
}

// ---- durable ⊗ transport: distributed time-travel ----

type counter struct {
	N int `json:"n"`
}

func counterMachine() *state.Machine[string, string, *counter] {
	return state.Forge[string, string, *counter]("counter").
		State("a").State("b").State("c").Final().
		Initial("a").
		Transition("a").On("next").GoTo("b").
		Transition("b").On("next").GoTo("c").
		Quench()
}

// TestE2E_DistributedTimeTravel records a durable instance on node-b and
// reconstructs its earlier state from node-a over gRPC.
func TestE2E_DistributedTimeTravel(t *testing.T) {
	ctx := context.Background()
	m := counterMachine()
	store := durable.NewMemStore(durable.WithHistory())
	h, err := durable.NewRunner(m, store).Start(ctx, "c-1", &counter{}, state.WithInitialState("a"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err = h.Fire(ctx, "next"); err != nil { // -> b
		t.Fatalf("fire 1: %v", err)
	}
	if _, err = h.Fire(ctx, "next"); err != nil { // -> c
		t.Fatalf("fire 2: %v", err)
	}
	steps, err := durable.Steps(ctx, store, "c-1")
	if err != nil || len(steps) < 2 {
		t.Fatalf("steps = %v, err = %v", steps, err)
	}

	lis := bufconn.Listen(1 << 20)
	gs := transport.NewTimeTravelServer(transport.NewDurableTimeTravel(m, store))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	tr := transport.New()
	tr.AddNode("node-b", dialBuf(t, lis))

	raw, err := tr.StateAt(ctx, "node-b", "c-1", steps[len(steps)-2]) // the step that reached b
	if err != nil {
		t.Fatalf("remote StateAt: %v", err)
	}
	snap, err := state.UnmarshalSnapshot[string, string, *counter](raw)
	if err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if snap.Current != "b" {
		t.Fatalf("reconstructed state = %q, want b", snap.Current)
	}
}
