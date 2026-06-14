package state

import (
	"context"
	"testing"
)

// eventlessCtx is the entity threaded through the eventless region tests; the N
// field lets a region transition's assign prove the region commit path threads
// context for eventless transitions exactly as it does for evented ones.
type eventlessCtx struct{ N int }

// configHas reports whether the settled configuration contains the named leaf.
func configHas[S comparable](t *testing.T, cfg []S, want S) bool {
	t.Helper()
	for _, l := range cfg {
		if l == want {
			return true
		}
	}
	return false
}

// TestSelectEventless_SecondRegion_Settles is K5/T1a: an Always transition in a
// non-first parallel region must fire. Before the per-leaf scan, eventless
// selection walked only config[0]'s spine, so the second region's b1->b2 was
// starved and the configuration retained b1.
func TestSelectEventless_SecondRegion_Settles(t *testing.T) {
	m := ForgeFor[eventlessCtx]("t1a").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").Initial("aIdle").SubState("aIdle").EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("b2").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(eventlessCtx) string { return "off" }).
		Transition("b1").Always().GoTo("b2").
		Quench()
	inst := m.Cast(eventlessCtx{}, WithInitialState("off"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("go: %v", res.Err)
	}
	cfg := inst.Configuration()
	if configHas(t, cfg, "b1") {
		t.Errorf("second-region Always starved: config = %v, want b2 not b1", cfg)
	}
	if !configHas(t, cfg, "b2") {
		t.Errorf("second-region Always did not settle: config = %v, want b2", cfg)
	}
	if !configHas(t, cfg, "aIdle") {
		t.Errorf("sibling region leaf dropped: config = %v, want aIdle", cfg)
	}
}

// TestSelectEventless_FirstRegion_PreservesSibling is K5/T1b: an Always in the
// FIRST region must advance only that region's leaf and keep the sibling
// region's leaf. Before the region-aware dispatch the eventless transition went
// through the whole-config commit path, which replaced the entire configuration
// and dropped the sibling leaf.
func TestSelectEventless_FirstRegion_PreservesSibling(t *testing.T) {
	m := ForgeFor[eventlessCtx]("t1b").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").Initial("a1").SubState("a1").SubState("a2").EndRegion().
		Region("b").Initial("bIdle").SubState("bIdle").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(eventlessCtx) string { return "off" }).
		Transition("a1").Always().GoTo("a2").
		Quench()
	inst := m.Cast(eventlessCtx{}, WithInitialState("off"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("go: %v", res.Err)
	}
	cfg := inst.Configuration()
	if !configHas(t, cfg, "a2") {
		t.Errorf("first-region Always did not advance: config = %v, want a2", cfg)
	}
	if !configHas(t, cfg, "bIdle") {
		t.Errorf("first-region Always collapsed config: config = %v, want sibling bIdle retained", cfg)
	}
}

// TestSelectEventless_RegionThreadsContextNoRaise asserts an eventless region
// transition flows through the region commit path: its assign folds the live
// context (K4), and it raises nothing spurious / produces no done regression.
// The eventless a1->a2 carries an OnEntryAssign that increments N; observing
// N==1 proves the region path committed the fold rather than discarding it.
func TestSelectEventless_RegionThreadsContextNoRaise(t *testing.T) {
	bump := func(c AssignCtx[eventlessCtx]) eventlessCtx { c.Entity.N++; return c.Entity }
	m := ForgeFor[eventlessCtx]("t1ctx").
		Reducer("bump", bump).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").Initial("a1").SubState("a1").SubState("a2").OnEntryAssign("bump").EndRegion().
		Region("b").Initial("bIdle").SubState("bIdle").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(eventlessCtx) string { return "off" }).
		Transition("a1").Always().GoTo("a2").
		Quench()
	inst := m.Cast(eventlessCtx{}, WithInitialState("off"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("go: %v", res.Err)
	}
	if got := inst.Entity().N; got != 1 {
		t.Errorf("eventless region entry assign not committed: N = %d, want 1", got)
	}
	cfg := inst.Configuration()
	if !configHas(t, cfg, "a2") || !configHas(t, cfg, "bIdle") {
		t.Errorf("config after eventless region transition = %v, want [a2 bIdle]", cfg)
	}
}

// TestSelectEventless_RegionOrderingAcrossMicrosteps asserts that when both
// regions hold an enabled Always, they settle in declaration order across
// successive microsteps (one eventless transition per microstep), matching the
// RTC determinism contract. Region a settles first (a1->a2), then region b
// (b1->b2); both leaves survive.
func TestSelectEventless_RegionOrderingAcrossMicrosteps(t *testing.T) {
	m := ForgeFor[eventlessCtx]("t1order").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").Initial("a1").SubState("a1").SubState("a2").EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("b2").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(eventlessCtx) string { return "off" }).
		Transition("a1").Always().GoTo("a2").
		Transition("b1").Always().GoTo("b2").
		Quench()
	inst := m.Cast(eventlessCtx{}, WithInitialState("off"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("go: %v", res.Err)
	}
	cfg := inst.Configuration()
	if !configHas(t, cfg, "a2") || !configHas(t, cfg, "b2") {
		t.Errorf("both regions did not settle: config = %v, want [a2 b2]", cfg)
	}
}

// TestSelectEventless_RegionSelfCycleOverflows guards macrostep termination: a
// self-cycling eventless transition inside a region must overflow the microstep
// bound with a typed MicrostepOverflowError rather than spin forever. The
// per-leaf scan must not break the overflow guard.
func TestSelectEventless_RegionSelfCycleOverflows(t *testing.T) {
	m := ForgeFor[eventlessCtx]("t1cycle").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("a").Initial("a1").SubState("a1").SubState("a2").EndRegion().
		Region("b").Initial("bIdle").SubState("bIdle").EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(eventlessCtx) string { return "off" }).
		Transition("a1").Always().GoTo("a2").
		Transition("a2").Always().GoTo("a1").
		Quench()
	inst := m.Cast(eventlessCtx{}, WithInitialState("off"))
	res := inst.Fire(context.Background(), "go")
	if res.Err == nil {
		t.Fatalf("expected microstep overflow, got nil err, config = %v", inst.Configuration())
	}
	var overflow *MicrostepOverflowError
	if !as(res.Err, &overflow) {
		t.Fatalf("expected *MicrostepOverflowError, got %T: %v", res.Err, res.Err)
	}
}
