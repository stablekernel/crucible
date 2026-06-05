package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// docRegistry returns the host behavior palette for the example machine,
// identical to what the DSL registers.
func docRegistry() *state.Registry[*Document] {
	return state.NewRegistry[*Document]().
		Guard("hasReviewer", func(ctx state.GuardCtx[*Document]) bool {
			return ctx.Entity.ReviewerID != nil
		}).
		Action("emit", emitEvent)
}

// TestRoundTrip_Identity asserts the lossless round-trip conformance pillar: a
// machine Forged in code and the same machine after ToJSON -> LoadFromJSON ->
// Provide -> Quench behave identically.
func TestRoundTrip_Identity(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}

	jsonBytes, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	if len(jsonBytes) == 0 {
		t.Fatal("ToJSON produced empty bytes")
	}

	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](jsonBytes)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	m2 := ir.Provide(docRegistry()).Quench()
	if m2 == nil {
		t.Fatal("Provide().Quench() returned nil")
	}

	// Behavioral identity: the same Fire produces the same outcome.
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	r1 := m.Cast(doc).Fire(context.Background(), Submit)
	// m2 was rehydrated from JSON, which does not carry the non-serializable
	// CurrentStateFn; supply the starting state explicitly.
	r2 := m2.Cast(doc, state.WithInitialState(doc.Status)).Fire(context.Background(), Submit)
	if r1.NewState != r2.NewState {
		t.Fatalf("round-trip diverged: %v vs %v", r1.NewState, r2.NewState)
	}
	if r1.Trace.Outcome != r2.Trace.Outcome {
		t.Fatalf("round-trip outcome diverged: %v vs %v", r1.Trace.Outcome, r2.Trace.Outcome)
	}
}

// TestProvide_UnboundRef asserts Provide fails with *UnboundRefError when a ref
// name in the IR has no registry binding.
func TestProvide_UnboundRef(t *testing.T) {
	m, rec := safeBuild(t)
	if rec != nil {
		t.Skipf("build not implemented yet: %v", rec)
	}
	jsonBytes, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](jsonBytes)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}

	// An empty registry resolves none of the IR's refs.
	empty := state.NewRegistry[*Document]()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected Provide/Quench to fail on unbound ref")
		}
		if err, ok := r.(error); ok {
			var ub *state.UnboundRefError
			if !errors.As(err, &ub) {
				t.Fatalf("recovered err = %v, want *UnboundRefError", err)
			}
		}
	}()
	_ = ir.Provide(empty).Quench()
}
