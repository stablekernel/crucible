package expr_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// TestGuardString_EquivalentToGuard asserts GuardString[C] authors the same rich
// guard as the fully-spelled Guard[string, C]: it returns a named-ref leaf tagged
// Kind "rich" (mirroring TestGuard_ReturnsRichNode) and forwards its options, so a
// supplied Catalog records the type-checked entry exactly as Guard would.
func TestGuardString_EquivalentToGuard(t *testing.T) {
	t.Parallel()

	reg := state.NewRegistry[order]()
	cat := expr.NewCatalog()

	node, err := expr.GuardString(reg, "isPaid", `status == "paid"`, orderSchema(), expr.WithCatalog(cat))
	if err != nil {
		t.Fatalf("GuardString: %v", err)
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

	// Options forward through to Guard, so the Catalog records the entry. A second
	// author under the same name on the same Catalog collides, proving GuardString
	// wrote through identically to Guard[string, order].
	if _, ok := cat.Entry("isPaid"); !ok {
		t.Fatal("catalog has no entry for isPaid; option did not forward")
	}
	if _, err := expr.Guard[string, order](
		reg, "isPaid", `status == "paid"`, orderSchema(), expr.WithCatalog(cat),
	); err == nil {
		t.Fatal("expected duplicate-name catalog collision after GuardString, got nil")
	}
}
