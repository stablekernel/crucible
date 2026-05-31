package verify_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/verify"
)

func TestResult_Initial(t *testing.T) {
	res := verify.Verify(linearChain())
	if got := res.Initial(); got != "a" {
		t.Errorf("Initial() = %q, want %q", got, "a")
	}
}

func TestResult_String_NoFindings(t *testing.T) {
	// Restricting to a state that is not declared leaves the result empty.
	res := verify.Verify(linearChain(), verify.Reachable("does-not-exist"))
	if got := res.String(); got != "no findings" {
		t.Errorf("String() = %q, want %q", got, "no findings")
	}
}

func TestReachable_Union(t *testing.T) {
	// Two Reachable options union their targets.
	res := verify.Verify(linearChain(), verify.Reachable("b"), verify.Reachable("d"))
	if len(res.Findings) != 2 {
		t.Fatalf("expected 2 findings from unioned targets, got %d:\n%s", len(res.Findings), res)
	}
	if !res.CanReach("b") || !res.CanReach("d") {
		t.Errorf("both b and d should be reachable: %s", res)
	}
	if _, ok := res.For("c"); ok {
		t.Error("c was not a requested target; it must not appear")
	}
}

// TestVerify_SingleFinalMachine covers the minimal machine: one final state that
// is also initial.
func TestVerify_SingleFinalMachine(t *testing.T) {
	m := state.Forge[string, string, any]("single").
		State("only").Final().
		Initial("only").
		Quench()

	res := verify.Verify(m)
	if !res.OK() {
		t.Errorf("single-state machine should be OK: %s", res)
	}
	f, ok := res.For("only")
	if !ok || !f.Reachable {
		t.Fatal("the only state must be reachable")
	}
	if len(f.Witness.Steps) != 0 {
		t.Errorf("initial state witness must be empty, got %v", f.Witness.Steps)
	}
}
