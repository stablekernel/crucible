package expr_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// TestAssign_DrivesContextThroughFire authors a rich assign, wires it onto a
// transition, and fires it through a Provide+Quench machine — exactly the flow a
// Go reducer follows — asserting the CEL-computed field updates land on the context.
func TestAssign_DrivesContextThroughFire(t *testing.T) {
	reg := state.NewRegistry[order]()
	if err := expr.Assign(reg, "applyDiscount",
		`{"total": total * 0.9, "status": "discounted"}`, orderSchema()); err != nil {
		t.Fatalf("Assign: %v", err)
	}

	def := state.Forge[string, string, order]("rich").
		Reducer("applyDiscount", func(in state.AssignCtx[order]) order { return in.Entity }). // stub, overwritten by Provide
		State("from").
		Transition("from").On("go").GoTo("to").Assign("applyDiscount").
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

	inst := m.Cast(order{Status: "paid", Total: 100, Quantity: 2}, state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")

	if inst.Current() != "to" {
		t.Fatalf("did not transition; current=%v", inst.Current())
	}
	got := inst.Entity()
	if got.Total != 90 {
		t.Fatalf("total = %v, want 90 (discounted)", got.Total)
	}
	if got.Status != "discounted" {
		t.Fatalf("status = %q, want discounted", got.Status)
	}
	// An unlisted field is preserved by the shallow merge.
	if got.Quantity != 2 {
		t.Fatalf("quantity = %d, want 2 (preserved)", got.Quantity)
	}
}

// fireAssign authors a rich assign, wires it onto a one-edge machine through
// Provide+Quench, fires it over the given start context, and returns the resulting
// context.
func fireAssign(t *testing.T, source string, start order) order {
	t.Helper()
	reg := state.NewRegistry[order]()
	if err := expr.Assign(reg, "a", source, orderSchema()); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	def := state.Forge[string, string, order]("rich").
		Reducer("a", func(in state.AssignCtx[order]) order { return in.Entity }).
		State("from").
		Transition("from").On("go").GoTo("to").Assign("a").
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
	inst := ir.Provide(reg).Quench().Cast(start, state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	return inst.Entity()
}

// TestAssign_EmptyMapIsNoOp confirms an assign that evaluates to an empty update
// map leaves the context unchanged.
func TestAssign_EmptyMapIsNoOp(t *testing.T) {
	got := fireAssign(t, `{}`, order{Status: "paid", Total: 10})
	if got.Status != "paid" || got.Total != 10 {
		t.Fatalf("empty-map assign changed context: %+v", got)
	}
}

// TestAssign_TypeMismatchedUpdateIsNoOp confirms an update whose value cannot decode
// into the field's type (a string into an int field) leaves the context unchanged —
// the merge round-trip fails and the reducer returns the prior context.
func TestAssign_TypeMismatchedUpdateIsNoOp(t *testing.T) {
	got := fireAssign(t, `{"quantity": "not-a-number"}`, order{Quantity: 7})
	if got.Quantity != 7 {
		t.Fatalf("quantity = %d, want 7 (type-mismatched update ignored)", got.Quantity)
	}
}

// TestAssign_RuntimeEvalErrorIsNoOp confirms an expression that type-checks but
// fails at evaluation (a runtime division by zero) leaves the context unchanged.
func TestAssign_RuntimeEvalErrorIsNoOp(t *testing.T) {
	got := fireAssign(t, `{"quantity": quantity / quantity}`, order{Quantity: 0})
	if got.Quantity != 0 {
		t.Fatalf("quantity = %d, want 0 (eval error left context unchanged)", got.Quantity)
	}
}

// TestAssign_DuplicateCatalogEntryFails surfaces the catalog collision when two
// rich entries are authored under the same name into one catalog.
func TestAssign_DuplicateCatalogEntryFails(t *testing.T) {
	reg := state.NewRegistry[order]()
	cat := expr.NewCatalog()
	if err := expr.Assign(reg, "dup", `{"quantity": quantity + 1}`, orderSchema(), expr.WithCatalog(cat)); err != nil {
		t.Fatalf("first Assign: %v", err)
	}
	if err := expr.Assign(reg, "dup", `{"quantity": quantity + 2}`, orderSchema(), expr.WithCatalog(cat)); err == nil {
		t.Fatal("second Assign under the same catalog name = nil error, want a collision")
	}
}

// TestAssign_UnmarshalableContextIsNoOp confirms a context value that cannot be
// projected to JSON (a NaN float) leaves the context unchanged rather than panicking
// or corrupting it.
func TestAssign_UnmarshalableContextIsNoOp(t *testing.T) {
	got := fireAssign(t, `{"status": "x"}`, order{Status: "orig", Total: math.NaN()})
	if got.Status != "orig" {
		t.Fatalf("status = %q, want orig (unmarshalable context left unchanged)", got.Status)
	}
}

// TestAssign_PreservesLargeInt64Sibling confirms that an assign which only touches
// one field leaves an untouched int64 sibling (here a large time.Duration) exact,
// rather than corrupting it through a float64 JSON round-trip. A value above 2^53
// nanoseconds cannot be represented exactly as a float64, so a non-UseNumber merge
// would silently mangle it.
func TestAssign_PreservesLargeInt64Sibling(t *testing.T) {
	// 2^53 + 7 nanoseconds: the smallest range where float64 loses integer precision.
	const huge = time.Duration((int64(1) << 53) + 7)
	got := fireAssign(t, `{"status": "updated"}`, order{Status: "orig", Window: huge})
	if got.Status != "updated" {
		t.Fatalf("status = %q, want updated", got.Status)
	}
	if got.Window != huge {
		t.Fatalf("window = %d, want %d (large int64 sibling must survive the merge exactly)", got.Window, huge)
	}
}

// TestAssign_RejectsNonMapResult fails authoring when the expression does not
// evaluate to a map of field updates.
func TestAssign_RejectsNonMapResult(t *testing.T) {
	reg := state.NewRegistry[order]()
	if err := expr.Assign(reg, "bad", `total > 0`, orderSchema()); err == nil {
		t.Fatal("Assign with a bool result = nil error, want a type error")
	}
}

// TestAssign_RejectsBadSource fails authoring when the expression references an
// unknown field.
func TestAssign_RejectsBadSource(t *testing.T) {
	reg := state.NewRegistry[order]()
	if err := expr.Assign(reg, "bad", `{"total": nonexistent}`, orderSchema()); err == nil {
		t.Fatal("Assign referencing an unknown field = nil error, want a compile error")
	}
}

// TestAssign_RecordsCatalogAST collects the type-checked AST into a Catalog when the
// option is supplied, like guards.
func TestAssign_RecordsCatalogAST(t *testing.T) {
	reg := state.NewRegistry[order]()
	cat := expr.NewCatalog()
	if err := expr.Assign(reg, "bump", `{"quantity": quantity + 1}`, orderSchema(), expr.WithCatalog(cat)); err != nil {
		t.Fatalf("Assign: %v", err)
	}
	if _, ok := cat.Entry("bump"); !ok {
		t.Fatal("catalog recorded no entry for the rich assign")
	}
}
