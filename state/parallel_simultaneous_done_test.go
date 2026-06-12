package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// countEffect counts how many string effects equal want.
func countEffect(effects []state.Effect, want string) int {
	n := 0
	for _, e := range effects {
		if s, ok := e.(string); ok && s == want {
			n++
		}
	}
	return n
}

// stringEffects extracts the ordered string-typed effects, preserving emission
// order, so a test can lock the innermost-first / declaration-order sequence.
func stringEffects(effects []state.Effect) []string {
	out := make([]string, 0, len(effects))
	for _, e := range effects {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// TestParallel_SimultaneousRegionDone_FiresOnDoneOnce pins the done semantics of
// a parallel state whose TWO regions reach their final leaf within the SAME
// macrostep: P.OnDone must run EXACTLY ONCE, never zero (regions did finish) and
// never twice (settleInteriorDone is bounded at the region boundary, so it must
// not also run the parallel's OnDone — settleParallelDone owns that).
//
// Machine: parallel "par"; region a (a1 -e-> af[final]) and region b
// (b1 -e-> bf[final]); both regions transition on the SAME event "e", so a single
// Fire("e") drives BOTH regions final in ONE macrostep. par.OnDone emits "Pdone".
// Each region transition emits its own effect ("a-eff" / "b-eff") so the locked
// ordering is observable: region-a effect, region-b effect (declaration order),
// then the single "Pdone".
//
// This is a regression PIN for settleParallelDone (parallel.go:521): the
// simultaneous-completion interleaving was the untested branch flagged by
// coverage. The double-emit guard between settleInteriorDone and
// settleParallelDone must hold here.
func TestParallel_SimultaneousRegionDone_FiresOnDoneOnce(t *testing.T) {
	note := func(s string) state.ActionFn[prCtx] {
		return func(state.ActionCtx[prCtx]) (state.Effect, error) { return s, nil }
	}
	m := state.Forge[string, string, prCtx]("par-simultaneous-done").
		Action("Pdone", note("Pdone")).
		Action("aEff", note("a-eff")).
		Action("bEff", note("b-eff")).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").OnDone("Pdone").
		Region("a").Initial("a1").SubState("a1").SubState("af").Final().EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("bf").Final().EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("e").GoTo("af").Do("aEff").
		Transition("b1").On("e").GoTo("bf").Do("bEff").
		Quench()

	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "e")
	if res.Err != nil {
		t.Fatalf("e errored: %v (config=%v)", res.Err, inst.Configuration())
	}

	// Core PIN: OnDone fires EXACTLY ONCE in the same-macrostep case.
	if got := countEffect(res.Effects, "Pdone"); got != 1 {
		t.Fatalf("par.OnDone fired %d times in same-macrostep completion, want exactly 1 (effects=%v)",
			got, stringEffects(res.Effects))
	}

	// Locked ordering: each region's transition effect (declaration order: a then
	// b) precedes the single parallel-level Pdone, which settles after both
	// regions have advanced to final.
	wantOrder := []string{"a-eff", "b-eff", "Pdone"}
	got := stringEffects(res.Effects)
	if len(got) != len(wantOrder) {
		t.Fatalf("effect order = %v, want %v", got, wantOrder)
	}
	for i := range wantOrder {
		if got[i] != wantOrder[i] {
			t.Fatalf("effect order = %v, want %v (innermost region effects in declaration order, then parallel OnDone)", got, wantOrder)
		}
	}

	// Final configuration: both regions rest at their final leaves; the parallel
	// state has no cross-cutting done transition, so it is not auto-exited.
	cfg := sortedConfig(inst)
	want := []string{"af", "bf"}
	if len(cfg) != len(want) || cfg[0] != want[0] || cfg[1] != want[1] {
		t.Fatalf("config after e = %v, want %v", inst.Configuration(), want)
	}
}

// TestParallel_StaggeredRegionDone_FiresOnDoneOnce guards the non-regression
// twin: when the two regions reach final in DIFFERENT macrosteps, the parallel's
// OnDone must still fire EXACTLY ONCE, and only once the LAST region completes —
// not on the earlier macrostep when only one region is final. This locks that the
// simultaneous-case fix (if any) does not perturb the staggered path.
//
// Machine: region a (a1 -ea-> af[final]) and region b (b1 -eb-> bf[final]) finish
// on DISTINCT events. Fire("ea") makes only region a final: stateComplete(par) is
// false, so no Pdone. Fire("eb") completes region b: now par is complete and
// Pdone fires once.
func TestParallel_StaggeredRegionDone_FiresOnDoneOnce(t *testing.T) {
	note := func(s string) state.ActionFn[prCtx] {
		return func(state.ActionCtx[prCtx]) (state.Effect, error) { return s, nil }
	}
	m := state.Forge[string, string, prCtx]("par-staggered-done").
		Action("Pdone", note("Pdone")).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").OnDone("Pdone").
		Region("a").Initial("a1").SubState("a1").SubState("af").Final().EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("bf").Final().EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("ea").GoTo("af").
		Transition("b1").On("eb").GoTo("bf").
		Quench()

	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	// First macrostep: only region a finishes. par is NOT complete; no Pdone.
	resA := inst.Fire(ctx, "ea")
	if resA.Err != nil {
		t.Fatalf("ea errored: %v", resA.Err)
	}
	if got := countEffect(resA.Effects, "Pdone"); got != 0 {
		t.Fatalf("par.OnDone fired %d times after only region a finished, want 0 (par not yet complete)", got)
	}

	// Second macrostep: region b finishes, completing par. Pdone fires once.
	resB := inst.Fire(ctx, "eb")
	if resB.Err != nil {
		t.Fatalf("eb errored: %v", resB.Err)
	}
	if got := countEffect(resB.Effects, "Pdone"); got != 1 {
		t.Fatalf("par.OnDone fired %d times when the last region finished, want exactly 1 (effects=%v)",
			got, stringEffects(resB.Effects))
	}
}
