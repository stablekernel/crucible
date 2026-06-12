package state_test

import (
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// rlCtx is a trivial context for the region-lint regression tests.
type rlCtx struct{ N int }

// quenchPanic runs build (which is expected to panic at Quench) and returns the
// recovered value as an error, or nil if no panic occurred.
func quenchPanic(t *testing.T, build func()) error {
	t.Helper()
	var got error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if err, ok := r.(error); ok {
					got = err
					return
				}
				t.Fatalf("Quench panicked with a non-error value: %v", r)
			}
		}()
		build()
	}()
	return got
}

// TestQuench_RejectsRegionEscape proves that a region-internal transition whose
// target lies OUTSIDE the enclosing parallel region is rejected at Quench with a
// typed *RegionEscapeError (probe T7). SCXML would exit the whole parallel, which
// the region-scoped builder API does not express, so the construct is ill-defined
// and rejected at build time rather than corrupting the configuration at runtime.
func TestQuench_RejectsRegionEscape(t *testing.T) {
	err := quenchPanic(t, func() {
		state.Forge[string, string, rlCtx]("escape").
			State("off").
			Transition("off").On("go").GoTo("par").
			SuperState("par").
			Region("a").Initial("a1").SubState("a1").EndRegion().
			Region("b").Initial("bIdle").SubState("bIdle").EndRegion().
			EndSuperState().
			State("out").
			Initial("off").
			CurrentStateFn(func(rlCtx) string { return "off" }).
			Transition("a1").On("esc").GoTo("out").
			Quench()
	})
	if err == nil {
		t.Fatalf("Quench accepted a region-escaping transition; want a panic")
	}
	var esc *state.RegionEscapeError
	if !errors.As(err, &esc) {
		t.Fatalf("Quench panic = %v, want *RegionEscapeError", err)
	}
	if esc.Region != "a" {
		t.Errorf("RegionEscapeError.Region = %q, want \"a\"", esc.Region)
	}
}

// TestQuench_AcceptsInRegionTransition is the control: a transition that stays
// within its region (and one that exits to a region-final) must Quench cleanly.
func TestQuench_AcceptsInRegionTransition(t *testing.T) {
	err := quenchPanic(t, func() {
		state.Forge[string, string, rlCtx]("inregion").
			State("off").
			Transition("off").On("go").GoTo("par").
			SuperState("par").
			Region("a").Initial("a1").SubState("a1").SubState("a2").EndRegion().
			Region("b").Initial("bIdle").SubState("bIdle").EndRegion().
			EndSuperState().
			Initial("off").
			CurrentStateFn(func(rlCtx) string { return "off" }).
			Transition("a1").On("tick").GoTo("a2").
			Quench()
	})
	if err != nil {
		t.Fatalf("Quench rejected a valid in-region transition: %v", err)
	}
}
