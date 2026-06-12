package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file raises proven coverage on settleParallelDone (parallel.go:521) by
// driving, through the public Fire path, the two branches the v1.0 gap analysis
// flagged as untested:
//
//   - the OnDone-action ERROR branch: the parallel's own OnDone action fails, so
//     settleParallelDone must surface a typed *ActionFailedError tagged
//     "onDone:<parallel>" with OutcomeEffectError, rather than swallowing it.
//   - the upward-cascade branch: a parallel state nested *inside* a parent
//     compound, so once the parallel completes settleParallelDone runs and then
//     attempts to settle the parent's done (the `pn.hasParent` arm).
//
// Both branches are exercised end-to-end (no white-box reach-in) so the pins also
// lock the observable Fire contract callers depend on.

// TestParallel_OnDoneActionError_SurfacesTaggedFailure drives the error arm of
// settleParallelDone: when every region reaches final in one macrostep and the
// parallel's OnDone action returns an error, Fire must report a typed
// *ActionFailedError whose TransitionName identifies the parallel's onDone and
// whose cause unwraps to the original error, with OutcomeEffectError. A swallowed
// done-error would be a soundness hole: the caller would believe the macrostep
// succeeded while the OnDone effect never ran.
func TestParallel_OnDoneActionError_SurfacesTaggedFailure(t *testing.T) {
	boom := errors.New("ondone-boom")
	m := state.Forge[string, string, prCtx]("par-ondone-error").
		Action("failDone", func(state.ActionCtx[prCtx]) (state.Effect, error) {
			return nil, boom
		}).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").OnDone("failDone").
		Region("a").Initial("a1").SubState("a1").SubState("af").Final().EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("bf").Final().EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("e").GoTo("af").
		Transition("b1").On("e").GoTo("bf").
		Quench()

	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	// Both regions reach final on the same event; the parallel's OnDone then runs
	// and fails.
	res := inst.Fire(ctx, "e")

	var af *state.ActionFailedError
	if !errors.As(res.Err, &af) {
		t.Fatalf("err = %v, want *ActionFailedError", res.Err)
	}
	if !errors.Is(res.Err, boom) {
		t.Fatalf("err does not unwrap to boom: %v", res.Err)
	}
	if af.ActionName != "failDone" {
		t.Fatalf("ActionName = %q, want %q", af.ActionName, "failDone")
	}
	if af.TransitionName != "onDone:par" {
		t.Fatalf("TransitionName = %q, want %q (the parallel's onDone tag)", af.TransitionName, "onDone:par")
	}
	if res.Trace.Outcome != state.OutcomeEffectError {
		t.Fatalf("outcome = %v, want OutcomeEffectError", res.Trace.Outcome)
	}
}

// TestParallel_NestedUnderCompound_SettlesParentDone drives the upward-cascade arm
// of settleParallelDone (`if pn.hasParent { settleDone(...) }`): a parallel "par"
// is the sole child of a compound "outer". When both regions of "par" reach final
// in one macrostep, settleParallelDone runs par.OnDone and then attempts to settle
// "outer"'s done — so outer.OnDone must fire too, and exactly once.
//
// par.OnDone emits "Pdone"; outer.OnDone emits "Odone". After the completing event
// the effect stream must contain BOTH, in inner-then-outer order, each exactly
// once.
//
// REGRESSION PIN (formerly a freeze blocker): settleParallelDone's upward cascade
// once dropped the enclosing compound's OnDone because it routed through
// settleDone, which gates on n.state.IsFinal — and a PARALLEL state is never
// IsFinal, so the call was a dead no-op while stateComplete(outer) was true. The
// fix routes the cascade through completion semantics (settleEnclosingDone, the
// upward counterpart of settleInteriorDone, gating on stateComplete), so a nested
// parallel now propagates done to its enclosing compound exactly as a nested
// compound already did. This pin locks that emission and its inner-then-outer
// ordering; it must NOT be weakened back to the buggy 0-count.
func TestParallel_NestedUnderCompound_SettlesParentDone(t *testing.T) {
	note := func(s string) state.ActionFn[prCtx] {
		return func(state.ActionCtx[prCtx]) (state.Effect, error) { return s, nil }
	}
	m := state.Forge[string, string, prCtx]("par-nested-done").
		Action("Pdone", note("Pdone")).
		Action("Odone", note("Odone")).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("outer").OnDone("Odone").
		Initial("par").
		SuperState("par").OnDone("Pdone").
		Region("a").Initial("a1").SubState("a1").SubState("af").Final().EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("bf").Final().EndRegion().
		EndSuperState(). // close par
		EndSuperState(). // close outer
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("e").GoTo("af").
		Transition("b1").On("e").GoTo("bf").
		Quench()

	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering nested parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "e")
	if res.Err != nil {
		t.Fatalf("e errored: %v (config=%v)", res.Err, inst.Configuration())
	}

	// par.OnDone must fire exactly once.
	if got := countEffect(res.Effects, "Pdone"); got != 1 {
		t.Fatalf("par.OnDone fired %d times, want exactly 1 (effects=%v)",
			got, stringEffects(res.Effects))
	}

	// The cascade-upward arm: once "par" completes, its enclosing compound "outer"
	// is also complete (par is outer's only active descendant and is done), so
	// outer.OnDone must fire — exactly once.
	if got := countEffect(res.Effects, "Odone"); got != 1 {
		t.Fatalf("outer.OnDone fired %d times after the nested parallel completed, want exactly 1 (effects=%v)\n"+
			"the settleParallelDone upward-cascade arm must propagate done to the enclosing compound",
			got, stringEffects(res.Effects))
	}

	// Inner-then-outer ordering: the parallel's own done settles before its parent's.
	got := stringEffects(res.Effects)
	pIdx, oIdx := -1, -1
	for i, e := range got {
		switch e {
		case "Pdone":
			pIdx = i
		case "Odone":
			oIdx = i
		}
	}
	if pIdx < 0 || oIdx < 0 || pIdx >= oIdx {
		t.Fatalf("effect order = %v, want Pdone before Odone (inner done settles before the enclosing compound's done)", got)
	}
}

// TestParallel_NestedTwoCompoundsDeep_CascadesAllDone pins the RECURSIVE arm of
// the upward cascade: a parallel "par" nested inside compound "mid" nested inside
// compound "outer". When "par" completes in one macrostep, the done settlement
// must walk the whole enclosing spine innermost-first — par.OnDone, then
// mid.OnDone, then outer.OnDone — because each enclosing ancestor in turn becomes
// stateComplete. Each fires exactly once, in spine order.
func TestParallel_NestedTwoCompoundsDeep_CascadesAllDone(t *testing.T) {
	note := func(s string) state.ActionFn[prCtx] {
		return func(state.ActionCtx[prCtx]) (state.Effect, error) { return s, nil }
	}
	m := state.Forge[string, string, prCtx]("par-nested-2deep").
		Action("Pdone", note("Pdone")).
		Action("Mdone", note("Mdone")).
		Action("Odone", note("Odone")).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("outer").OnDone("Odone").
		Initial("mid").
		SuperState("mid").OnDone("Mdone").
		Initial("par").
		SuperState("par").OnDone("Pdone").
		Region("a").Initial("a1").SubState("a1").SubState("af").Final().EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("bf").Final().EndRegion().
		EndSuperState(). // close par
		EndSuperState(). // close mid
		EndSuperState(). // close outer
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("e").GoTo("af").
		Transition("b1").On("e").GoTo("bf").
		Quench()

	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering nested parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "e")
	if res.Err != nil {
		t.Fatalf("e errored: %v (config=%v)", res.Err, inst.Configuration())
	}

	for _, want := range []string{"Pdone", "Mdone", "Odone"} {
		if got := countEffect(res.Effects, want); got != 1 {
			t.Fatalf("%s fired %d times, want exactly 1 (effects=%v)", want, got, stringEffects(res.Effects))
		}
	}

	// Innermost-first spine order: Pdone < Mdone < Odone.
	idx := map[string]int{"Pdone": -1, "Mdone": -1, "Odone": -1}
	for i, e := range stringEffects(res.Effects) {
		if _, ok := idx[e]; ok {
			idx[e] = i
		}
	}
	if idx["Pdone"] >= idx["Mdone"] || idx["Mdone"] >= idx["Odone"] {
		t.Fatalf("effect order = %v, want Pdone < Mdone < Odone (innermost-first up the spine)", stringEffects(res.Effects))
	}
}

// TestParallel_NestedUnderIncompleteCompound_DoesNotCascade pins the cascade's
// completion GATE: the enclosing compound "outer" also has a sibling child "side"
// that stays active, so "outer" is NOT stateComplete when "par" completes. The
// parallel's own OnDone must still fire, but the enclosing compound's OnDone must
// NOT — the cascade halts at the first incomplete ancestor. This guards against a
// fix that unconditionally walks the spine instead of gating on stateComplete.
func TestParallel_NestedUnderIncompleteCompound_DoesNotCascade(t *testing.T) {
	note := func(s string) state.ActionFn[prCtx] {
		return func(state.ActionCtx[prCtx]) (state.Effect, error) { return s, nil }
	}
	m := state.Forge[string, string, prCtx]("par-nested-incomplete").
		Action("Pdone", note("Pdone")).
		Action("Odone", note("Odone")).
		State("off").
		Transition("off").On("go").GoTo("par").
		// "outer" is a parallel state with two regions: one holding "par", one
		// holding a non-final "side". When "par" completes, outer's other region
		// is still active and non-final, so outer is not stateComplete.
		SuperState("outer").OnDone("Odone").
		Region("main").Initial("par").
		SuperState("par").OnDone("Pdone").
		Region("a").Initial("a1").SubState("a1").SubState("af").Final().EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("bf").Final().EndRegion().
		EndSuperState(). // close par
		EndRegion().     // close main region
		Region("side").Initial("s1").SubState("s1").EndRegion().
		EndSuperState(). // close outer
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("e").GoTo("af").
		Transition("b1").On("e").GoTo("bf").
		Quench()

	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering nested parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "e")
	if res.Err != nil {
		t.Fatalf("e errored: %v (config=%v)", res.Err, inst.Configuration())
	}

	if got := countEffect(res.Effects, "Pdone"); got != 1 {
		t.Fatalf("par.OnDone fired %d times, want exactly 1 (effects=%v)", got, stringEffects(res.Effects))
	}
	if got := countEffect(res.Effects, "Odone"); got != 0 {
		t.Fatalf("outer.OnDone fired %d times, want 0 — outer is not complete (its 'side' region is still active) (effects=%v)",
			got, stringEffects(res.Effects))
	}
}
