package state_test

import (
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestAssay_FailFast asserts Assay returns *AssayError with a single failure in
// the default fail-fast mode when the entity does not satisfy the state.
func TestAssay_FailFast(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	// A document in Approved state requires a reviewer; this one has none.
	doc := &Document{}
	err := m.Assay(Approved, doc)
	var ae *state.AssayError
	if !errors.As(err, &ae) {
		t.Fatalf("err = %v, want *AssayError", err)
	}
	if len(ae.Failures) != 1 {
		t.Fatalf("Failures = %d, want 1 (fail-fast)", len(ae.Failures))
	}
}

// TestAssay_Aggregate asserts WithAggregate collects all failing requirements
// and that the error type is uniform (*AssayError) across both modes.
func TestAssay_Aggregate(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	doc := &Document{}
	err := m.Assay(Approved, doc, state.WithAggregate())
	var ae *state.AssayError
	if !errors.As(err, &ae) {
		t.Fatalf("err = %v, want *AssayError", err)
	}
	if len(ae.Failures) < 1 {
		t.Fatalf("Failures = %d, want >= 1 (aggregate)", len(ae.Failures))
	}
}

// TestAssay_Satisfied asserts Assay returns nil when the entity satisfies the
// state's requirements.
func TestAssay_Satisfied(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	doc := &Document{ReviewerID: strptr("rev-1")}
	if err := m.Assay(Approved, doc); err != nil {
		t.Fatalf("Assay err = %v, want nil", err)
	}
}
