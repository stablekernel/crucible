package state_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// prCtx is a trivial context for the parallel-region regression tests.
type prCtx struct{ N int }

// firstStringEffect returns the first string-typed effect, or "?" when none is
// present. Region observer actions emit "N=<value>" strings.
func firstStringEffect(effects []state.Effect) string {
	for _, e := range effects {
		if s, ok := e.(string); ok {
			return s
		}
	}
	return "?"
}

// sortedConfig returns the instance configuration sorted for order-independent
// comparison of the orthogonal leaf set.
func sortedConfig(inst interface{ Configuration() []string }) []string {
	cfg := append([]string(nil), inst.Configuration()...)
	sort.Strings(cfg)
	return cfg
}

// TestRegionTransition_RaiseIsDelivered proves that a Raise declared on a
// region-internal transition enqueues its internal event so the run-to-completion
// loop delivers it to a sibling region (probe T2).
//
// Machine: parallel "par" with regions a (a1->a2 on "tick", raising "boost") and
// b (b1->b2 on "boost"). After Fire("tick") the config must be {a2, b2}: the
// region transition's Raise must reach region b. The pre-fix kernel dropped the
// Raise (applyRegionTransition never called enqueueRaised), leaving {a2, b1}.
func TestRegionTransition_RaiseIsDelivered(t *testing.T) {
	m := state.Forge[string, string, prCtx]("pr-raise").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").Initial("a1").SubState("a1").SubState("a2").EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("b2").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("tick").GoTo("a2").Raise("boost").
		Transition("b1").On("boost").GoTo("b2").
		Quench()

	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "tick")
	if res.Err != nil {
		t.Fatalf("tick errored: %v", res.Err)
	}

	got := sortedConfig(inst)
	want := []string{"a2", "b2"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("config after tick = %v, want %v (region Raise must reach sibling region b)", inst.Configuration(), want)
	}
}

// TestRegionTransition_GuardFailBubblesToParallelHandler proves a region
// candidate whose guard predicate returns FALSE does not consume the event:
// the event falls through to the parallel-state-level handler, matching the
// compound-state analog (probe T4).
//
// Machine: parallel "par" with a region transition a1 -evt-> a2 guarded by "no"
// (always false), and a parallel-level transition par -evt-> done. On Fire("evt")
// the false guard must NOT mask the parallel handler: the machine must reach
// "done" with no error, exactly as the equivalent compound machine does. The
// pre-fix kernel returned handled=true on guard-false, blocking the bubble and
// surfacing a GuardFailedError.
func TestRegionTransition_GuardFailBubblesToParallelHandler(t *testing.T) {
	ctx := context.Background()

	mPar := state.Forge[string, string, prCtx]("pr-guardfail-par").
		Guard("no", func(state.GuardCtx[prCtx]) bool { return false }).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Transition("par").On("evt").GoTo("done").
		Region("a").Initial("a1").SubState("a1").SubState("a2").EndRegion().
		Region("b").Initial("b1").SubState("b1").EndRegion().
		EndSuperState().
		State("done").Final().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("evt").GoTo("a2").When("no").
		Quench()
	ip := mPar.Cast(prCtx{}, state.WithInitialState("off"))
	if res := ip.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}
	resP := ip.Fire(ctx, "evt")

	// Compound analog: an identical guard-false candidate inside a compound state
	// bubbles to the parent handler. The parallel case must behave the same.
	mCmp := state.Forge[string, string, prCtx]("pr-guardfail-cmp").
		Guard("no", func(state.GuardCtx[prCtx]) bool { return false }).
		State("off").
		Transition("off").On("go").GoTo("k").
		SuperState("k").
		Transition("k").On("evt").GoTo("done").
		SubState("c1").
		SubState("c2").
		Initial("c1").
		EndSuperState().
		State("done").Final().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("c1").On("evt").GoTo("c2").When("no").
		Quench()
	ic := mCmp.Cast(prCtx{}, state.WithInitialState("off"))
	if res := ic.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering compound: %v", res.Err)
	}
	resC := ic.Fire(ctx, "evt")

	if resC.Err != nil || resC.NewState != "done" {
		t.Fatalf("compound analog: state=%v err=%v, want done/nil", resC.NewState, resC.Err)
	}
	if resP.Err != nil {
		t.Fatalf("parallel: guard-false candidate masked the parallel handler: err=%v", resP.Err)
	}
	if resP.NewState != "done" {
		t.Fatalf("parallel: state=%v, want done (event must bubble past the failed guard)", resP.NewState)
	}
}

// TestRegionTransition_ActionSeesExitAssignFold proves a region transition's
// bound action observes the context folded by the SOURCE state's exit assign,
// not the pre-fire snapshot (probe T10, finding K4).
//
// Machine: parallel "par"; region a has a1 (OnExitAssign "inc": N++) -> a2 on
// "t", and the a1 transition's Do("obs") emits "N=<N>". After Fire("t") the
// observer must see N=1 — the exit-assign fold must be visible to the later
// transition action, exactly as it is on the compound (main) commit path. The
// pre-fix region path passed the frozen entity to every runActions, so the
// observer saw the stale N=0.
func TestRegionTransition_ActionSeesExitAssignFold(t *testing.T) {
	inc := func(c state.AssignCtx[prCtx]) prCtx { c.Entity.N++; return c.Entity }
	obs := func(c state.ActionCtx[prCtx]) (state.Effect, error) {
		return fmt.Sprintf("N=%d", c.Entity.N), nil
	}
	ctx := context.Background()

	mPar := state.Forge[string, string, prCtx]("pr-fold-par").
		Reducer("inc", inc).
		Action("obs", obs).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").Initial("a1").SubState("a1").OnExitAssign("inc").SubState("a2").EndRegion().
		Region("b").Initial("b1").SubState("b1").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("a1").On("t").GoTo("a2").Do("obs").
		Quench()
	ip := mPar.Cast(prCtx{}, state.WithInitialState("off"))
	if res := ip.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}
	resP := ip.Fire(ctx, "t")
	if resP.Err != nil {
		t.Fatalf("t errored: %v", resP.Err)
	}

	// Compound analog: identical shape on the main commit path; the observer sees
	// the exit-assign fold (N=1). The region path must match.
	mCmp := state.Forge[string, string, prCtx]("pr-fold-cmp").
		Reducer("inc", inc).
		Action("obs", obs).
		State("off").
		Transition("off").On("go").GoTo("k").
		SuperState("k").
		SubState("c1").OnExitAssign("inc").
		SubState("c2").
		Initial("c1").
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("c1").On("t").GoTo("c2").Do("obs").
		Quench()
	ic := mCmp.Cast(prCtx{}, state.WithInitialState("off"))
	if res := ic.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering compound: %v", res.Err)
	}
	resC := ic.Fire(ctx, "t")
	if resC.Err != nil {
		t.Fatalf("compound t errored: %v", resC.Err)
	}

	if got := firstStringEffect(resC.Effects); got != "N=1" {
		t.Fatalf("compound analog observed %s, want N=1 (sanity: main commit path folds exit assign)", got)
	}
	if got := firstStringEffect(resP.Effects); got != "N=1" {
		t.Fatalf("region action observed %s, want N=1 (region path must thread the exit-assign fold)", got)
	}
}

// TestRegionTransition_InteriorCompoundOnDone proves a compound nested inside a
// region emits its OnDone when its leaf goes final via a region transition
// (probe T12, finding K3).
//
// Machine: parallel "par"; region a holds compound "K" (OnDone "kdone") with
// children k1, kf (final). A region transition k1 -t-> kf drives K's leaf final.
// After Fire("t") the effects must include "kdone": settleDone must run for the
// interior compound, stopping at the region boundary. The pre-fix region path
// ran neither settleDone nor settleParallelDone for interiors, so OnDone was
// skipped.
func TestRegionTransition_InteriorCompoundOnDone(t *testing.T) {
	note := func(s string) state.ActionFn[prCtx] {
		return func(state.ActionCtx[prCtx]) (state.Effect, error) { return s, nil }
	}
	m := state.Forge[string, string, prCtx]("pr-interior-done").
		Action("kdone", note("kdone")).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").
		Initial("K").
		SuperState("K").OnDone("kdone").
		SubState("k1").
		SubState("kf").Final().
		Initial("k1").
		EndSuperState().
		EndRegion().
		Region("b").Initial("b1").SubState("b1").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("k1").On("t").GoTo("kf").
		Quench()
	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}
	res := inst.Fire(ctx, "t")
	if res.Err != nil {
		t.Fatalf("t errored: %v", res.Err)
	}
	saw := false
	for _, e := range res.Effects {
		if e == "kdone" {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("effects=%v config=%v: interior compound OnDone (kdone) must run on region-transition completion",
			res.Effects, inst.Configuration())
	}
}

// TestRegionTransition_SameRegionHistoryRestore proves a region-internal
// transition targeting a history pseudostate owned by a compound nested in the
// SAME region resolves through resolveHistory/recordHistory (probe T8, finding
// K2-restore).
//
// Machine: parallel "par"; region a holds "idle" and compound "K" (deep history
// "Khist", children k1, k2). The sequence enterK, adv, leave drives K to k2 then
// exits back to idle (recording K's deep history). Fire("back") targets Khist:
// deep history must restore k2 — Khist must NOT become an active leaf, and the
// instance must not be stuck. The pre-fix region path used the raw target, so
// the pseudostate entered as a permanent leaf.
func TestRegionTransition_SameRegionHistoryRestore(t *testing.T) {
	m := state.Forge[string, string, prCtx]("pr-hist").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").
		Initial("idle").
		SubState("idle").
		SuperState("K").
		SubState("k1").
		SubState("k2").
		Initial("k1").
		History("Khist", state.HistoryDeep).
		EndSuperState().
		EndRegion().
		Region("b").Initial("b1").SubState("b1").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(prCtx) string { return "off" }).
		Transition("idle").On("enterK").GoTo("K").
		Transition("k1").On("adv").GoTo("k2").
		Transition("k2").On("leave").GoTo("idle").
		Transition("idle").On("back").GoTo("Khist").
		Quench()
	inst := m.Cast(prCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	for _, ev := range []string{"go", "enterK", "adv", "leave"} {
		if res := inst.Fire(ctx, ev); res.Err != nil {
			t.Fatalf("setup %s: %v (config=%v)", ev, res.Err, inst.Configuration())
		}
	}

	res := inst.Fire(ctx, "back")
	if res.Err != nil {
		t.Fatalf("back errored: %v (config=%v)", res.Err, inst.Configuration())
	}
	cfg := sortedConfig(inst)
	for _, l := range cfg {
		if l == "Khist" {
			t.Fatalf("config=%v: history pseudostate Khist must not be an active leaf", cfg)
		}
	}
	restoredK2 := false
	for _, l := range cfg {
		if l == "k2" {
			restoredK2 = true
		}
	}
	if !restoredK2 {
		t.Fatalf("config=%v: deep-history restore must reactivate the recorded leaf k2", cfg)
	}

	// The instance must not be stuck: a subsequent region event still advances.
	res2 := inst.Fire(ctx, "leave")
	if res2.Err != nil {
		t.Fatalf("post-restore leave errored: %v (config=%v)", res2.Err, inst.Configuration())
	}
	stillStuck := true
	for _, l := range inst.Configuration() {
		if l == "idle" {
			stillStuck = false
		}
	}
	if stillStuck {
		t.Fatalf("post-restore config=%v: instance is stuck (leave did not return to idle)", inst.Configuration())
	}
}
