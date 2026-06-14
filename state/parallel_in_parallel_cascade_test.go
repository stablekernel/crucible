package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file is the headline parallel-in-parallel done-cascade pin. It builds an
// OUTER parallel "par0" whose region "A" goes final directly and whose region "B"
// holds a COMPOUND "bcomp" that contains an INNER parallel "par1" whose own regions
// all go final. The done-cascade must walk the whole enclosing spine
// innermost-first:
//
//	par1.OnDone (P1done)  — inner parallel completes
//	bcomp.OnDone (Bdone)  — its enclosing compound becomes stateComplete
//	par0.OnDone (P0done)  — both outer regions complete (A final; B's compound done)
//
// Each fires EXACTLY once, in that order. The completion is driven by TWO events,
// reflecting the engine's dispatch: an event is delivered to the DEEPEST active
// parallel shared by the live leaves (here par1), so a single event cannot also
// advance par0's sibling region "A". The inner-completing event "e" drives par1 to
// final (emitting P1done then Bdone, since bcomp becomes stateComplete); the
// outer-completing event "f" — which par1's now-final regions decline, so it
// bubbles outward to par0's region A — advances "a1" to "af", at which point par0
// is stateComplete and par0.OnDone fires. Collected across both fires the cascade
// must be innermost-first: P1done, then Bdone, then P0done.
//
// This pins the recursive upward cascade (the settleParallelDone /
// settleEnclosingDone path) across a parallel nested inside a compound nested
// inside a parallel — the deepest composite the v1.0 freeze must get right. The
// cascade gates on stateComplete (a parallel is never IsFinal), so a nested
// parallel propagates done to its enclosing compound and on up to the enclosing
// parallel exactly as a nested compound does.
//
// REGRESSION PIN: matches the cascade contract documented in the CHANGELOG
// ("a parallel state that completes inside an enclosing compound now cascades that
// compound's OnDone up the spine ... the parallel's own OnDone still fires exactly
// once"). It must NOT be weakened to a dropped (0-count) enclosing OnDone.
func TestParallelInParallel_NestedDone_CascadesInnermostFirst(t *testing.T) {
	note := func(s string) state.ActionFn[prCtx] {
		return func(state.ActionCtx[prCtx]) (state.Effect, error) { return s, nil }
	}
	m := state.Forge[string, string, prCtx]("par-in-par").
		Action("P1done", note("P1done")).
		Action("Bdone", note("Bdone")).
		Action("P0done", note("P0done")).
		State("off").
		Transition("off").On("go").GoTo("par0").
		// Outer parallel par0: region A goes final directly; region B holds a
		// compound whose content is the inner parallel par1.
		SuperState("par0").OnDone("P0done").
		Region("A").
		Initial("a1").
		SubState("a1").
		SubState("af").Final().
		EndRegion().
		Region("B").
		Initial("bcomp").
		SuperState("bcomp").OnDone("Bdone").
		Initial("par1").
		SuperState("par1").OnDone("P1done").
		Region("x").Initial("x1").SubState("x1").SubState("xf").Final().EndRegion().
		Region("y").Initial("y1").SubState("y1").SubState("yf").Final().EndRegion().
		EndSuperState(). // close par1 (inner parallel)
		EndSuperState(). // close bcomp (compound in region B)
		EndRegion().     // close region B
		EndSuperState(). // close par0 (outer parallel)
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		// "e" completes the inner parallel par1's regions; "f" completes par0's
		// sibling region A (it bubbles outward once par1's regions decline it).
		Transition("a1").On("f").GoTo("af").
		Transition("x1").On("e").GoTo("xf").
		Transition("y1").On("e").GoTo("yf").
		Quench()

	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering outer parallel: %v", res.Err)
	}

	// First event: completes the inner parallel par1. Its OnDone fires, and bcomp
	// (par1's enclosing compound) becomes stateComplete, so Bdone fires too. par0 is
	// NOT yet complete — its region A is still in "a1" — so P0done must not fire yet.
	resE := inst.Fire(ctx, "e")
	if resE.Err != nil {
		t.Fatalf("e errored: %v (config=%v)", resE.Err, inst.Configuration())
	}
	if got := countEffect(resE.Effects, "P1done"); got != 1 {
		t.Fatalf("after e: P1done fired %d times, want 1 (effects=%v)", got, stringEffects(resE.Effects))
	}
	if got := countEffect(resE.Effects, "Bdone"); got != 1 {
		t.Fatalf("after e: Bdone fired %d times, want 1 — the inner parallel's enclosing compound must settle (effects=%v)",
			got, stringEffects(resE.Effects))
	}
	if got := countEffect(resE.Effects, "P0done"); got != 0 {
		t.Fatalf("after e: P0done fired %d times, want 0 — par0's region A is still active (effects=%v)",
			got, stringEffects(resE.Effects))
	}

	// Second event: completes par0's region A (a1 -> af). par0 is now stateComplete,
	// so par0.OnDone fires — exactly once. The inner OnDones must not re-fire.
	resF := inst.Fire(ctx, "f")
	if resF.Err != nil {
		t.Fatalf("f errored: %v (config=%v)", resF.Err, inst.Configuration())
	}
	if got := countEffect(resF.Effects, "P0done"); got != 1 {
		t.Fatalf("after f: P0done fired %d times, want exactly 1 — the outer parallel must settle once its last region completes (effects=%v)",
			got, stringEffects(resF.Effects))
	}
	for _, stale := range []string{"P1done", "Bdone"} {
		if got := countEffect(resF.Effects, stale); got != 0 {
			t.Fatalf("after f: %s fired %d times, want 0 — already-settled inner OnDones must not re-fire (effects=%v)",
				stale, got, stringEffects(resF.Effects))
		}
	}

	// Innermost-first across the whole sequence: P1done (e) before Bdone (e) before
	// P0done (f).
	combined := append(stringEffects(resE.Effects), stringEffects(resF.Effects)...)
	idx := map[string]int{"P1done": -1, "Bdone": -1, "P0done": -1}
	for i, e := range combined {
		if _, ok := idx[e]; ok {
			idx[e] = i
		}
	}
	if idx["P1done"] >= idx["Bdone"] || idx["Bdone"] >= idx["P0done"] {
		t.Fatalf("effect order = %v, want P1done < Bdone < P0done (innermost-first up the spine)", combined)
	}
}
