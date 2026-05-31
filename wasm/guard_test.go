package wasm_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/wasm"
)

// order is the context the guard guest evaluates (its status and total fields).
type order struct {
	Status string  `json:"status"`
	Total  float64 `json:"total"`
}

// TestGuard_DrivesTransitionThroughFire wires a WASM-backed guard onto a transition
// through the full authoring flow (Forge → ToJSON → LoadFromJSON → Provide → Quench)
// and confirms the guard, evaluated in WebAssembly, gates Fire exactly like a Go
// guard: the approved order transitions, the unapproved one is blocked.
func TestGuard_DrivesTransitionThroughFire(t *testing.T) {
	ctx := context.Background()
	mod, err := wasm.Compile(ctx, guardWASM)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	t.Cleanup(func() { _ = mod.Close(ctx) })

	reg := state.NewRegistry[order]()
	node := wasm.Guard[string](reg, "approved", mod)

	def := state.Forge[string, string, order]("rich").
		Guard("approved", func(state.GuardCtx[order]) bool { return false }). // stub, overwritten by Provide
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
	pass.Fire(ctx, "go")
	if pass.Current() != "to" {
		t.Fatalf("paid/50 should transition via WASM guard; current=%v", pass.Current())
	}

	block := m.Cast(order{Status: "open", Total: 50}, state.WithInitialState("from"))
	block.Fire(ctx, "go")
	if block.Current() != "from" {
		t.Fatalf("open/50 should be blocked by WASM guard; current=%v", block.Current())
	}
}

// TestGuard_BrokenModuleBlocksTransition confirms a guard whose WASM module errors
// at evaluation is fail-safe: the guard reports false (an error), so the transition
// is blocked rather than taken.
func TestGuard_BrokenModuleBlocksTransition(t *testing.T) {
	ctx := context.Background()
	mod, err := wasm.Compile(ctx, badWASM)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	t.Cleanup(func() { _ = mod.Close(ctx) })

	reg := state.NewRegistry[order]()
	node := wasm.Guard[string](reg, "approved", mod)
	def := state.Forge[string, string, order]("rich").
		Guard("approved", func(state.GuardCtx[order]) bool { return false }).
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
	inst := ir.Provide(reg).Quench().Cast(order{Status: "paid", Total: 50}, state.WithInitialState("from"))
	inst.Fire(ctx, "go")
	if inst.Current() != "from" {
		t.Fatalf("a broken WASM guard should block the transition; current=%v", inst.Current())
	}
}

// TestCompile_RejectsBadModule fails clearly on bytes that are not a valid module.
func TestCompile_RejectsBadModule(t *testing.T) {
	if _, err := wasm.Compile(context.Background(), []byte("not wasm")); err == nil {
		t.Fatal("Compile of non-wasm bytes = nil error, want a failure")
	}
}

// TestCompile_RejectsMissingABI fails when a valid module lacks the alloc/eval
// exports — an empty module compiles but is not a behavior module.
func TestCompile_RejectsMissingABI(t *testing.T) {
	// The smallest valid WebAssembly module: the 8-byte header (magic + version).
	empty := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	if _, err := wasm.Compile(context.Background(), empty); err == nil {
		t.Fatal("Compile of a module without the ABI exports = nil error, want a failure")
	}
}
