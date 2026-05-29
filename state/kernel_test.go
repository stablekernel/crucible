package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// safeBuild recovers a panic from the not-yet-implemented Quench so a single
// test can assert on a specific later step without aborting the whole run with
// an un-recovered panic. When the kernel is implemented, recovered will be nil
// and the returned machine is real.
func safeBuild(t *testing.T) (m *state.Machine[DocState, DocEvent, *Document], recovered any) {
	t.Helper()
	defer func() { recovered = recover() }()
	m = buildDocMachine()
	return m, nil
}

// TestForgeQuench_BuildsMachine asserts the foundry build path
// (Forge -> ... -> Quench) yields a usable, named machine.
func TestForgeQuench_BuildsMachine(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Fatalf("Quench panicked (expected once implemented): %v", rec)
	}
	if m == nil {
		t.Fatal("Quench returned nil machine")
	}
	if got := m.Name(); got != "document" {
		t.Fatalf("Name() = %q, want %q", got, "document")
	}
}

// TestCastFire_HappyPath asserts the core foundry step: Cast an instance and
// Fire an event, advancing state and emitting effects with a success trace.
func TestCastFire_HappyPath(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	inst := m.Cast(doc)
	res := inst.Fire(context.Background(), Submit)
	if res.Err != nil {
		t.Fatalf("Fire(Submit) err = %v, want nil", res.Err)
	}
	if res.NewState != Submitted {
		t.Fatalf("NewState = %v, want %v", res.NewState, Submitted)
	}
	if res.Trace.Outcome != state.OutcomeSuccess {
		t.Fatalf("Trace.Outcome = %v, want OutcomeSuccess", res.Trace.Outcome)
	}
	if len(res.Effects) == 0 {
		t.Fatal("expected at least one emitted effect")
	}
}

// TestFire_TraceAlwaysNonNil asserts the invariant that Fire records a trace
// even on a failing transition.
func TestFire_TraceAlwaysNonNil(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	inst := m.Cast(&Document{Status: Draft})
	// Approve is not a valid event from Draft.
	res := inst.Fire(context.Background(), Approve)
	if res.Err == nil {
		t.Fatal("expected error firing Approve from Draft")
	}
	if res.Trace.Outcome == state.OutcomeSuccess {
		t.Fatal("expected non-success outcome recorded in trace")
	}
	if res.NewState != Draft {
		t.Fatalf("state must be unchanged on error: got %v, want %v", res.NewState, Draft)
	}
}

// TestFire_InvalidTransition asserts the typed ErrInvalidTransition.
func TestFire_InvalidTransition(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	inst := m.Cast(&Document{Status: Published})
	res := inst.Fire(context.Background(), Submit)
	var it *state.ErrInvalidTransition
	if !errors.As(res.Err, &it) {
		t.Fatalf("err = %v, want *ErrInvalidTransition", res.Err)
	}
}

// TestFire_GuardFailed asserts the typed ErrGuardFailed when a guard returns
// false (no reviewer on the document).
func TestFire_GuardFailed(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	inst := m.Cast(&Document{Status: Submitted}) // no reviewer set on this entity
	res := inst.Fire(context.Background(), Approve)
	var gf *state.ErrGuardFailed
	if !errors.As(res.Err, &gf) {
		t.Fatalf("err = %v, want *ErrGuardFailed", res.Err)
	}
	if gf.GuardName != "hasReviewer" {
		t.Fatalf("GuardName = %q, want %q", gf.GuardName, "hasReviewer")
	}
}

// TestPlanPath asserts BFS path planning returns the shortest event sequence.
func TestPlanPath(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	doc := &Document{ReviewerID: strptr("rev-1")}
	path, err := m.PlanPath(Draft, Published, doc)
	if err != nil {
		t.Fatalf("PlanPath err = %v, want nil", err)
	}
	want := []DocEvent{Submit, Approve, Publish}
	if len(path) != len(want) {
		t.Fatalf("path = %v, want %v", path, want)
	}
	for i := range want {
		if path[i] != want[i] {
			t.Fatalf("path[%d] = %v, want %v", i, path[i], want[i])
		}
	}
}

// TestPlanPath_NoPath asserts the typed ErrNoPath when no sequence connects.
func TestPlanPath_NoPath(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	doc := &Document{}
	_, err := m.PlanPath(Archived, Draft, doc)
	var np *state.ErrNoPath
	if !errors.As(err, &np) {
		t.Fatalf("err = %v, want *ErrNoPath", err)
	}
}

// TestFireSeq drives a sequence of events into one instance under
// run-to-completion, threading intermediate state.
func TestFireSeq(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	inst := m.Cast(doc)
	// Approve carries the hasReviewer guard, which reads the entity bound at
	// Cast; no context smuggling needed.
	br := inst.FireSeq(context.Background(), []DocEvent{Submit, Approve, Publish})
	if br.Err != nil {
		t.Fatalf("FireSeq err = %v, want nil", br.Err)
	}
	if len(br.Steps) != 3 {
		t.Fatalf("Steps = %d, want 3", len(br.Steps))
	}
	if last := br.Steps[len(br.Steps)-1]; last.NewState != Published {
		t.Fatalf("final state = %v, want %v", last.NewState, Published)
	}
}
