package expr_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// TestCatalog_RoundTripThroughIRMeta asserts a rich guard's type-checked AST
// survives the full IR Meta round-trip: authored into a Catalog, attached to a
// machine's Meta, serialized to JSON, reloaded, and read back with its source,
// dialect, and checked-AST bytes intact. This is the data-plane guarantee tooling
// and the polyglot evaluator rely on.
func TestCatalog_RoundTripThroughIRMeta(t *testing.T) {
	cat := expr.NewCatalog()
	reg := state.NewRegistry[order]()
	node, err := expr.Guard[string](reg, "bigPaid", `status == "paid" && total >= 40.0`, orderSchema(),
		expr.WithCatalog(cat))
	if err != nil {
		t.Fatalf("Guard: %v", err)
	}

	want, ok := cat.Entry("bigPaid")
	if !ok {
		t.Fatal("catalog missing authored guard")
	}
	if len(want.CheckedAST) == 0 {
		t.Fatal("catalog entry has no checked AST")
	}

	// Author a machine, attach the sidecar to the IR's machine-level Meta (the
	// reserved extension namespace the kernel round-trips verbatim), serialize, and
	// reload so the sidecar makes the full JSON round-trip.
	def := state.ForgeFor[order]("rich").
		Guard("bigPaid", func(state.GuardCtx[order]) bool { return false }).
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(node).
		State("to").
		Initial("from").
		Quench()

	ir0, err := state.LoadFromJSON[string, string, order](mustJSON(t, def))
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	ir0.Meta = cat.Meta()
	reJS, err := json.Marshal(ir0)
	if err != nil {
		t.Fatalf("marshal ir: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, order](reJS)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}

	got, err := expr.LoadCatalog(ir.Meta)
	if err != nil {
		t.Fatalf("LoadCatalog: %v", err)
	}
	if got.Len() != 1 {
		t.Fatalf("loaded catalog has %d entries, want 1", got.Len())
	}
	entry, ok := got.Entry("bigPaid")
	if !ok {
		t.Fatal("loaded catalog missing bigPaid")
	}
	if entry.Source != want.Source {
		t.Fatalf("source = %q, want %q", entry.Source, want.Source)
	}
	if entry.Dialect != expr.Dialect {
		t.Fatalf("dialect = %q, want %q", entry.Dialect, expr.Dialect)
	}
	if !bytes.Equal(entry.CheckedAST, want.CheckedAST) {
		t.Fatal("checked AST bytes changed across the round-trip")
	}
}

// TestCatalog_LoadFromEmptyMeta asserts a machine that never used the rich tier
// loads a clean, empty catalog rather than erroring.
func TestCatalog_LoadFromEmptyMeta(t *testing.T) {
	cat, err := expr.LoadCatalog(nil)
	if err != nil {
		t.Fatalf("LoadCatalog(nil): %v", err)
	}
	if cat.Len() != 0 {
		t.Fatalf("empty meta yielded %d entries", cat.Len())
	}
	cat, err = expr.LoadCatalog(map[string]any{"unrelated": 1})
	if err != nil {
		t.Fatalf("LoadCatalog(no sidecar): %v", err)
	}
	if cat.Len() != 0 {
		t.Fatalf("meta without sidecar yielded %d entries", cat.Len())
	}
}

// TestCatalog_RejectsDuplicateName asserts authoring two rich guards under the same
// name into one catalog is rejected, so the sidecar never silently loses an entry.
func TestCatalog_RejectsDuplicateName(t *testing.T) {
	cat := expr.NewCatalog()
	reg := state.NewRegistry[order]()
	if _, err := expr.Guard[string](reg, "dup", `rush`, orderSchema(), expr.WithCatalog(cat)); err != nil {
		t.Fatalf("first author: %v", err)
	}
	if _, err := expr.Guard[string](reg, "dup", `!rush`, orderSchema(), expr.WithCatalog(cat)); err == nil {
		t.Fatal("second author under the same name should be rejected")
	}
}

// TestCheckedAST_ReloadsToWorkingProgram asserts the stored checked-AST bytes are
// not merely opaque blobs: they reload into a CEL AST that rebuilds an equivalent,
// evaluable program — the property the polyglot/browser evaluator depends on.
func TestCheckedAST_ReloadsToWorkingProgram(t *testing.T) {
	cat := expr.NewCatalog()
	reg := state.NewRegistry[order]()
	if _, err := expr.Guard[string](reg, "g", `total > 40.0`, orderSchema(), expr.WithCatalog(cat)); err != nil {
		t.Fatalf("Guard: %v", err)
	}
	entry, _ := cat.Entry("g")
	pass, err := expr.EvalCheckedAST(entry.CheckedAST, orderSchema(), sampleOrder())
	if err != nil {
		t.Fatalf("EvalCheckedAST: %v", err)
	}
	if !pass {
		t.Fatal("reloaded program should pass for total=42.50 > 40.0")
	}
}
