package state

import "testing"

// directChildToward (history.go:177) is the spine-descent helper recordHistory
// uses to pick the compound's direct child toward a remembered leaf. Its two
// not-found arms — leaf == compound (idx==0) and leaf not descending from compound
// — are DEFENSIVE: the sole caller, recordHistory, only ever passes deep[0], a
// proper active leaf already filtered to be a strict descendant of the compound
// (recordHistory skips non-compounds and isDescendant-filters the leaf set, and a
// compound is never itself a live leaf). The public Fire path therefore never
// reaches those arms; the deep/shallow history tests in history_test.go exercise
// only the happy descent (the idx>0 return).
//
// This white-box test pins all three arms directly so the defensive returns are
// proven-correct rather than untested. It is package-internal because
// directChildToward is an unexported method; the conscious, documented reason is
// that the not-found arms are unreachable through the public API and must be
// covered as kernel invariants here, not contorted into a black-box scenario.
//
// Topology (compound "root" with two subtrees):
//
//	root
//	├── a   (compound)
//	│   └── a1 (leaf)
//	└── b   (leaf)
func directChildTowardMachine(t *testing.T) *Machine[string, string, struct{}] {
	t.Helper()
	return Forge[string, string, struct{}]("directchild").
		SuperState("root").
		Initial("a").
		SuperState("a").
		Initial("a1").
		SubState("a1").
		EndSuperState(). // close a
		SubState("b").
		EndSuperState(). // close root
		Initial("root").
		CurrentStateFn(func(struct{}) string { return "a1" }).
		Quench()
}

func TestDirectChildToward_Branches(t *testing.T) {
	m := directChildTowardMachine(t)

	t.Run("descends to direct child toward a nested leaf", func(t *testing.T) {
		// a1 is two levels below root (root -> a -> a1); the direct child of root
		// on a1's spine is "a". This is the reachable happy arm (idx>0).
		got, ok := m.directChildToward("root", "a1")
		if !ok {
			t.Fatalf("directChildToward(root, a1) ok=false, want true")
		}
		if got != "a" {
			t.Fatalf("directChildToward(root, a1) = %q, want %q (direct child on the spine)", got, "a")
		}
	})

	t.Run("direct child that is itself the leaf", func(t *testing.T) {
		// b is a direct child leaf of root; directChildToward must return b itself.
		got, ok := m.directChildToward("root", "b")
		if !ok || got != "b" {
			t.Fatalf("directChildToward(root, b) = (%q,%v), want (b,true)", got, ok)
		}
	})

	t.Run("DEFENSIVE: leaf == compound yields no proper descendant", func(t *testing.T) {
		// idx==0 arm (history.go:181): the compound is its own ancestor head, so
		// there is no proper descendant to descend toward. recordHistory never hits
		// this (a compound is never an active leaf), but the invariant is pinned.
		got, ok := m.directChildToward("root", "root")
		if ok {
			t.Fatalf("directChildToward(root, root) ok=true (got %q), want false: a compound has no proper descendant toward itself", got)
		}
		if got != "" {
			t.Fatalf("directChildToward(root, root) returned %q, want zero value", got)
		}
	})

	t.Run("DEFENSIVE: leaf not descending from compound", func(t *testing.T) {
		// not-found arm (history.go:189): "b" does not descend from "a", so the
		// loop never finds "a" in b's ancestor chain. recordHistory never hits this
		// (it isDescendant-filters first), but the invariant is pinned.
		got, ok := m.directChildToward("a", "b")
		if ok {
			t.Fatalf("directChildToward(a, b) ok=true (got %q), want false: b does not descend from a", got)
		}
		if got != "" {
			t.Fatalf("directChildToward(a, b) returned %q, want zero value", got)
		}
	})

	t.Run("DEFENSIVE: leaf unknown to the machine", func(t *testing.T) {
		// An unknown leaf has no chain containing the compound; the not-found arm
		// returns (zero,false) rather than panicking.
		got, ok := m.directChildToward("root", "ghost")
		if ok || got != "" {
			t.Fatalf("directChildToward(root, ghost) = (%q,%v), want (\"\",false)", got, ok)
		}
	})
}
