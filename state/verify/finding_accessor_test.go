package verify_test

import (
	"testing"

	"github.com/stablekernel/crucible/state/verify"
)

// TestFinding_Accessors_PerKindPolarity asserts that the kind-specific accessors
// interpret the overloaded Reachable bool correctly for every FindingKind, so a
// caller never has to hard-code the per-kind polarity. The matrix covers both the
// true and false value of Reachable for each kind.
func TestFinding_Accessors_PerKindPolarity(t *testing.T) {
	cases := []struct {
		name        string
		kind        verify.FindingKind
		reachable   bool
		isReachable bool
		holds       bool
		violated    bool
		covered     bool
	}{
		{"reachability true", verify.KindReachability, true, true, false, false, false},
		{"reachability false", verify.KindReachability, false, false, false, false, false},
		{"conditional true", verify.KindConditionalReachability, true, true, false, false, false},
		{"conditional false", verify.KindConditionalReachability, false, false, false, false, false},
		{"liveness holds", verify.KindLiveness, true, false, true, false, false},
		{"liveness violated", verify.KindLiveness, false, false, false, true, false},
		{"invariant holds", verify.KindInvariant, true, false, true, false, false},
		{"invariant violated", verify.KindInvariant, false, false, false, true, false},
		{"bounded holds", verify.KindBoundedViolation, true, false, true, false, false},
		{"bounded violated", verify.KindBoundedViolation, false, false, false, true, false},
		{"coverage covered", verify.KindCoverage, true, false, false, false, true},
		{"coverage uncovered", verify.KindCoverage, false, false, false, false, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			f := verify.Finding{Kind: c.kind, Reachable: c.reachable}
			if got := f.IsReachable(); got != c.isReachable {
				t.Errorf("IsReachable() = %v, want %v", got, c.isReachable)
			}
			if got := f.Holds(); got != c.holds {
				t.Errorf("Holds() = %v, want %v", got, c.holds)
			}
			if got := f.Violated(); got != c.violated {
				t.Errorf("Violated() = %v, want %v", got, c.violated)
			}
			if got := f.Covered(); got != c.covered {
				t.Errorf("Covered() = %v, want %v", got, c.covered)
			}
		})
	}
}

// TestFinding_Accessors_AgreeWithResultHelpers cross-checks the new accessors
// against the existing Result-level helpers on a real Verify pass, so the additive
// methods can never drift from the field they interpret.
func TestFinding_Accessors_AgreeWithResultHelpers(t *testing.T) {
	res := verify.Verify(linearChain(), verify.Reachable("b"))
	f, ok := res.For("b")
	if !ok {
		t.Fatal("expected a reachability finding for b")
	}
	if f.IsReachable() != res.CanReach("b") {
		t.Errorf("IsReachable() = %v but CanReach = %v", f.IsReachable(), res.CanReach("b"))
	}
	// A reachability finding has no holds/violated/covered property.
	if f.Holds() || f.Violated() || f.Covered() {
		t.Errorf("reachability finding must not report Holds/Violated/Covered: %+v", f)
	}
}
