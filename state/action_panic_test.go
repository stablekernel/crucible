package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// apctx is a tiny entity for the action-panic regression tests.
type apctx struct{}

// errActionBoom is a sentinel error a panicking action panics with, so the test
// can assert errors.Is reaches it through ActionPanicError.Unwrap.
var errActionBoom = errors.New("action boom")

// TestActionPanic_OnEntrySurfacesTyped asserts a panicking OnEntry action is
// recovered into a *ActionPanicError on FireResult.Err rather than crashing Fire.
func TestActionPanic_OnEntrySurfacesTyped(t *testing.T) {
	m := state.Forge[string, string, apctx]("ap-entry").
		Action("boom", func(state.ActionCtx[apctx]) (state.Effect, error) { panic("kaboom") }).
		State("from").
		State("to").OnEntry("boom").
		Transition("from").On("go").GoTo("to").
		Initial("from").
		Quench()

	inst := m.Cast(apctx{}, state.WithInitialState("from"))
	res := inst.Fire(context.Background(), "go")

	if res.Err == nil {
		t.Fatalf("want non-nil FireResult.Err from panicking OnEntry action")
	}
	var ap *state.ActionPanicError
	if !errors.As(res.Err, &ap) {
		t.Fatalf("want *ActionPanicError, got %T: %v", res.Err, res.Err)
	}
	if ap.ActionName != "boom" {
		t.Fatalf("ActionName = %q, want %q", ap.ActionName, "boom")
	}
}

// TestActionPanic_TransitionActionSurfacesTyped asserts a panicking transition
// action (Do) is recovered into a *ActionPanicError on FireResult.Err.
func TestActionPanic_TransitionActionSurfacesTyped(t *testing.T) {
	m := state.Forge[string, string, apctx]("ap-trans").
		Action("boom", func(state.ActionCtx[apctx]) (state.Effect, error) { panic("kaboom") }).
		State("from").
		State("to").
		Transition("from").On("go").GoTo("to").Do("boom").
		Initial("from").
		Quench()

	inst := m.Cast(apctx{}, state.WithInitialState("from"))
	res := inst.Fire(context.Background(), "go")

	if res.Err == nil {
		t.Fatalf("want non-nil FireResult.Err from panicking transition action")
	}
	var ap *state.ActionPanicError
	if !errors.As(res.Err, &ap) {
		t.Fatalf("want *ActionPanicError, got %T: %v", res.Err, res.Err)
	}
	if ap.ActionName != "boom" {
		t.Fatalf("ActionName = %q, want %q", ap.ActionName, "boom")
	}
}

// TestActionPanic_UnwrapsInnerError asserts that when an action panics with a
// sentinel error value, errors.Is reaches that inner error through
// ActionPanicError.Unwrap, and errors.As still reaches the *ActionPanicError.
func TestActionPanic_UnwrapsInnerError(t *testing.T) {
	m := state.Forge[string, string, apctx]("ap-unwrap").
		Action("boom", func(state.ActionCtx[apctx]) (state.Effect, error) { panic(errActionBoom) }).
		State("from").
		State("to").
		Transition("from").On("go").GoTo("to").Do("boom").
		Initial("from").
		Quench()

	inst := m.Cast(apctx{}, state.WithInitialState("from"))
	res := inst.Fire(context.Background(), "go")

	var ap *state.ActionPanicError
	if !errors.As(res.Err, &ap) {
		t.Fatalf("want *ActionPanicError, got %T: %v", res.Err, res.Err)
	}
	if !errors.Is(res.Err, errActionBoom) {
		t.Fatalf("errors.Is did not reach errActionBoom through Unwrap: %v", res.Err)
	}
}
