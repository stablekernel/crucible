package e2e_test

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
	"github.com/stablekernel/crucible/transport"
	"github.com/stablekernel/crucible/wasm"
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

// ---- wasm ⊗ state ⊗ durable: a WASM-backed guard survives record/replay ----

type approvalOrder struct {
	Amount int64  `json:"amount"`
	Status string `json:"status"`
}

// buildApprovalGuest compiles the approval guard guest to wasip1/wasm with the
// standard Go toolchain (no committed binary, no TinyGo) and returns its bytes,
// mirroring the wasm package's own guest build so the e2e joint compiles its
// guard the same proven way.
func buildApprovalGuest(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "approval.wasm")
	cmd := exec.Command("go", "build", "-buildmode=c-shared", "-o", out, "./testdata/approvalguest")
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	if buildOut, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build approval guest: %v\n%s", err, buildOut)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read built guest: %v", err)
	}
	return b
}

// approvalMachine forges an order machine whose approve transition is gated by a
// WASM-backed guard (amount >= 100), authored through the full production flow
// (Forge → ToJSON → LoadFromJSON → Provide) so the foreign-engine guard resolves
// exactly like an in-tree one.
func approvalMachine(t *testing.T, mod *wasm.Module) *state.Machine[string, string, approvalOrder] {
	t.Helper()
	reg := state.NewRegistry[approvalOrder]()
	node := wasm.Guard[string](reg, "approved", mod)

	def := state.Forge[string, string, approvalOrder]("approval").
		Guard("approved", func(state.GuardCtx[approvalOrder]) bool { return false }). // stub, replaced by Provide
		State("pending").
		State("approved").
		Initial("pending").
		Transition("pending").On("approve").GoTo("approved").WhenExpr(node).
		Quench()

	js, err := def.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, approvalOrder](js)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	return ir.Provide(reg).Quench()
}

// TestE2E_WASMGuardedDurableTransitionSurvivesRecovery drives a durable instance
// through a transition gated by a WebAssembly-backed guard, then recovers it from
// the store — proving a foreign-engine guard composes with the durable
// record/replay seam: the approved order persists at approved and replays
// deterministically, and a below-threshold order is blocked through the same
// WASM evaluator. It is hermetic (the guest is built on demand with the Go wasm
// toolchain) and deterministic (the guard is a pure predicate over context).
func TestE2E_WASMGuardedDurableTransitionSurvivesRecovery(t *testing.T) {
	ctx := context.Background()
	mod, err := wasm.Compile(ctx, buildApprovalGuest(t))
	if err != nil {
		t.Fatalf("compile guard: %v", err)
	}
	t.Cleanup(func() { _ = mod.Close(ctx) })

	m := approvalMachine(t, mod)
	store := durable.NewMemStore()
	runner := durable.NewRunner(m, store)

	// An at-threshold order: the WASM guard admits it, the durable runner records
	// the transition, and recovery replays it to the same approved state.
	const okID = "order-ok"
	okH, err := runner.Start(ctx, okID, approvalOrder{Amount: 150, Status: "pending"}, state.WithInitialState("pending"))
	if err != nil {
		t.Fatalf("start ok order: %v", err)
	}
	if _, err = okH.Fire(ctx, "approve"); err != nil {
		t.Fatalf("fire approve: %v", err)
	}
	if got := okH.Instance().Current(); got != "approved" {
		t.Fatalf("WASM guard should admit amount 150; current=%q, want approved", got)
	}

	rec, err := durable.Recover(ctx, m, store, okID)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := rec.Instance().Current(); got != "approved" {
		t.Fatalf("recovered state = %q, want approved (WASM-guarded transition replayed)", got)
	}

	// A below-threshold order: the same WASM guard blocks it, so the transition is
	// rejected (a GuardFailedError) and the durable instance never leaves pending.
	const lowID = "order-low"
	lowH, err := runner.Start(ctx, lowID, approvalOrder{Amount: 50, Status: "pending"}, state.WithInitialState("pending"))
	if err != nil {
		t.Fatalf("start low order: %v", err)
	}
	_, err = lowH.Fire(ctx, "approve")
	var guardErr *state.GuardFailedError
	if !errors.As(err, &guardErr) {
		t.Fatalf("fire approve (low) error = %v, want a *state.GuardFailedError from the WASM guard", err)
	}
	if got := lowH.Instance().Current(); got != "pending" {
		t.Fatalf("WASM guard should block amount 50; current=%q, want pending", got)
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
