package expr_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// TestWithCostLimit_BoundsEvaluation asserts a tight cost limit causes a costly
// expression's evaluation to fail (the guard then yields false), while a generous
// limit lets the same expression evaluate. This exercises the cost-limit option as a
// quantitative termination guardrail.
func TestWithCostLimit_BoundsEvaluation(t *testing.T) {
	// A guard whose cost exceeds a tiny limit. Authoring still succeeds (cost is an
	// eval-time bound); evaluation under the tiny limit errors and the binding yields
	// a non-transitioning false.
	source := `status == "paid" && status == "paid" && status == "paid"`

	tight := state.NewRegistry[order]()
	tightNode, err := expr.Guard[string](tight, "g", source, orderSchema(), expr.WithCostLimit(1))
	if err != nil {
		t.Fatalf("Guard(tight): %v", err)
	}
	if fireWith(t, tight, tightNode, sampleOrder()) {
		t.Fatal("a guard over its cost limit must not enable the transition")
	}

	loose := state.NewRegistry[order]()
	looseNode, err := expr.Guard[string](loose, "g", source, orderSchema(), expr.WithCostLimit(1_000))
	if err != nil {
		t.Fatalf("Guard(loose): %v", err)
	}
	if !fireWith(t, loose, looseNode, sampleOrder()) {
		t.Fatal("a guard within its cost limit should enable the transition")
	}
}

// TestCatalog_Names asserts Names returns the catalog's guard names sorted, so
// tooling enumerates rich guards deterministically.
func TestCatalog_Names(t *testing.T) {
	cat := expr.NewCatalog()
	reg := state.NewRegistry[order]()
	for _, n := range []string{"zeta", "alpha", "mid"} {
		if _, err := expr.Guard[string](reg, n, `rush`, orderSchema(), expr.WithCatalog(cat)); err != nil {
			t.Fatalf("Guard(%q): %v", n, err)
		}
	}
	got := cat.Names()
	want := []string{"alpha", "mid", "zeta"}
	if len(got) != len(want) {
		t.Fatalf("names = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("names = %v, want %v", got, want)
		}
	}
}

// fireWith binds node against reg and fires a one-edge machine, reporting whether the
// transition was enabled.
func fireWith(t *testing.T, reg *state.Registry[order], node state.GuardNode[string], e order) bool {
	t.Helper()
	m := provideMachine(t, reg, node)
	inst := m.Cast(e, state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	return inst.Current() == "to"
}
