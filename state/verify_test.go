package state_test

import (
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestVerify_FailFast asserts Verify returns *VerifyError with a single failure in
// the default fail-fast mode when the entity does not satisfy the state.
func TestVerify_FailFast(t *testing.T) {
	m := buildDocMachine()
	// A document in Approved state requires a reviewer; this one has none.
	doc := &Document{}
	err := m.Verify(Approved, doc)
	var ae *state.VerifyError
	if !errors.As(err, &ae) {
		t.Fatalf("err = %v, want *VerifyError", err)
	}
	if len(ae.Failures) != 1 {
		t.Fatalf("Failures = %d, want 1 (fail-fast)", len(ae.Failures))
	}
}

// TestVerify_Aggregate asserts Aggregate collects all failing requirements
// and that the error type is uniform (*VerifyError) across both modes.
func TestVerify_Aggregate(t *testing.T) {
	m := buildDocMachine()
	doc := &Document{}
	err := m.Verify(Approved, doc, state.Aggregate())
	var ae *state.VerifyError
	if !errors.As(err, &ae) {
		t.Fatalf("err = %v, want *VerifyError", err)
	}
	if len(ae.Failures) < 1 {
		t.Fatalf("Failures = %d, want >= 1 (aggregate)", len(ae.Failures))
	}
}

// TestVerify_Satisfied asserts Verify returns nil when the entity satisfies the
// state's requirements.
func TestVerify_Satisfied(t *testing.T) {
	m := buildDocMachine()
	doc := &Document{ReviewerID: strptr("rev-1")}
	if err := m.Verify(Approved, doc); err != nil {
		t.Fatalf("Verify err = %v, want nil", err)
	}
}
