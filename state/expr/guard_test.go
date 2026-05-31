package expr_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// order is the sample context the rich-guard tests evaluate against. Its JSON tags
// match the variable names the CEL source references, so the schema-derived env and
// the activation projection agree on field naming.
type order struct {
	Status   string        `json:"status"`
	Total    float64       `json:"total"`
	Quantity int           `json:"quantity"`
	Rush     bool          `json:"rush"`
	Window   time.Duration `json:"window"`
}

// sampleOrder is a representative context value used across the eval tests.
func sampleOrder() order {
	return order{Status: "paid", Total: 42.50, Quantity: 3, Rush: true, Window: 90 * time.Minute}
}

// orderSchema is the ContextSchema for order, derived by reflection.
func orderSchema() state.ContextSchema { return state.SchemaOf[order]() }

// TestGuard_CompileAndEval exercises the core authoring-then-eval path: a rich guard
// compiles against the schema and evaluates correctly for representative contexts.
func TestGuard_CompileAndEval(t *testing.T) {
	cases := []struct {
		name   string
		source string
		want   bool
	}{
		{"numeric compare true", `total > 40.0`, true},
		{"numeric compare false", `total > 50.0`, false},
		{"int field compare", `quantity >= 3`, true},
		{"string equality", `status == "paid"`, true},
		{"bool field", `rush`, true},
		{"boolean composition", `status == "paid" && quantity > 1`, true},
		{"duration compare", `window > duration("1h")`, true},
		{"membership", `status in ["paid", "settled"]`, true},
		{"string op", `status.startsWith("pa")`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fireRich(t, tc.source, sampleOrder()); got != tc.want {
				t.Fatalf("source %q: enabled=%v, want %v", tc.source, got, tc.want)
			}
		})
	}
}

// TestGuard_RejectsBadSource asserts the schema type-check rejects ill-typed source
// at authoring time, not at eval: an unknown field, a non-bool result, and a
// type-incompatible comparison all fail Guard loudly.
func TestGuard_RejectsBadSource(t *testing.T) {
	cases := []struct {
		name   string
		source string
		substr string
	}{
		{"unknown field", `missing > 1`, "undeclared"},
		{"non-bool result", `total + 1.0`, "want bool"},
		{"type mismatch", `status > 1`, "no matching overload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := state.NewRegistry[order]()
			_, err := expr.Guard[string](reg, "g", tc.source, orderSchema())
			if err == nil {
				t.Fatalf("source %q should fail to compile", tc.source)
			}
			if !strings.Contains(err.Error(), tc.substr) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.substr)
			}
		})
	}
}

// TestGuard_ReturnsRichNode asserts the returned node is a named-ref leaf tagged
// Kind "rich" so analysis tooling can tell a rich guard from a Core one while the
// kernel still resolves it by name.
func TestGuard_ReturnsRichNode(t *testing.T) {
	reg := state.NewRegistry[order]()
	node, err := expr.Guard[string](reg, "isPaid", `status == "paid"`, orderSchema())
	if err != nil {
		t.Fatalf("Guard: %v", err)
	}
	if node.Op != state.GuardLeaf {
		t.Fatalf("op = %q, want leaf", node.Op)
	}
	if node.Kind != state.GuardKindRich {
		t.Fatalf("kind = %q, want rich", node.Kind)
	}
	if node.Ref == nil || node.Ref.Name != "isPaid" {
		t.Fatalf("ref = %+v, want name isPaid", node.Ref)
	}
}

// TestGuard_DrivesTransitionThroughFire is the end-to-end gate: a rich guard,
// authored against a registry and referenced from a machine, enables or blocks a
// transition when fired — proving the CEL binding evaluates inside the pure Fire
// step via the kernel's named-guard path. It also exercises the full IR round-trip
// (ToJSON -> LoadFromJSON -> Provide) so the rich guard binds off a rehydrated
// machine exactly like a Go guard.
func TestGuard_DrivesTransitionThroughFire(t *testing.T) {
	reg := state.NewRegistry[order]()
	node, err := expr.Guard[string](reg, "bigPaid", `status == "paid" && total >= 40.0`, orderSchema())
	if err != nil {
		t.Fatalf("Guard: %v", err)
	}

	def := state.Forge[string, string, order]("rich").
		Guard("bigPaid", func(state.GuardCtx[order]) bool { return false }).
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(node).
		State("to").
		Initial("from").
		Quench()

	js, err := def.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, order](js)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	m := ir.Provide(reg).Quench()

	pass := m.Cast(order{Status: "paid", Total: 50}, state.WithInitialState("from"))
	pass.Fire(context.Background(), "go")
	if pass.Current() != "to" {
		t.Fatalf("paid/50 should transition; current=%v", pass.Current())
	}

	block := m.Cast(order{Status: "open", Total: 50}, state.WithInitialState("from"))
	block.Fire(context.Background(), "go")
	if block.Current() != "from" {
		t.Fatalf("open/50 should block; current=%v", block.Current())
	}
}

// fireRich authors a rich guard from source, wires it onto a one-edge machine, fires
// the transition against the entity, and reports whether the transition was enabled.
func fireRich(t *testing.T, source string, e order) bool {
	t.Helper()
	reg := state.NewRegistry[order]()
	node, err := expr.Guard[string](reg, "g", source, orderSchema())
	if err != nil {
		t.Fatalf("Guard(%q): %v", source, err)
	}
	m := provideMachine(t, reg, node)
	inst := m.Cast(e, state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	return inst.Current() == "to"
}

// provideMachine builds a one-edge machine carrying node on its "go" transition,
// serializes it to IR, and binds it against reg (which holds the real CEL guard) via
// Provide+Quench. The authoring builder registers a stub under the guard's name so
// its own Quench validates the ref; Provide then overwrites that stub with reg's
// CEL binding, so the fired machine uses the rich guard. This mirrors the supported
// flow: behavior lives in a host registry and is bound by name at Provide.
func provideMachine(t *testing.T, reg *state.Registry[order], node state.GuardNode[string]) *state.Machine[string, string, order] {
	t.Helper()
	name := node.Ref.Name
	def := state.Forge[string, string, order]("rich").
		Guard(name, func(state.GuardCtx[order]) bool { return false }).
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(node).
		State("to").
		Initial("from").
		Quench()
	ir, err := state.LoadFromJSON[string, string, order](mustJSON(t, def))
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	return ir.Provide(reg).Quench()
}

// mustJSON serializes a machine to its IR JSON or fails the test.
func mustJSON(t *testing.T, m *state.Machine[string, string, order]) []byte {
	t.Helper()
	js, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	return js
}
