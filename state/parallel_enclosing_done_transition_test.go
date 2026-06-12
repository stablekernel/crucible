package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file pins the done-TRANSITION sub-case adjacent to commit 94f3302, which
// fixed the enclosing-compound OnDone ACTION cascade (settleEnclosingDone in
// parallel.go) when a parallel completes inside a compound. Here we verify the
// companion: a done-TRANSITION declared on the ENCLOSING COMPOUND is taken when
// the parallel inside it completes, in the SAME macrostep, and exactly as it is
// for the already-working nested-compound case.
//
// crucible/state has no SCXML-style done.state event or implicit done-transition.
// A compound's "done" is settled as an OnDone ACTION cascade (settleParallelDone
// -> settleEnclosingDone), and a state change ON completion is expressed
// idiomatically as an eventless (Always) transition declared on the compound,
// guarded so it fires only once that compound is complete. The completion gate is
// the guard: each region's final state bumps a counter, and the guard passes only
// when every region has completed. selectEventless (fire.go) scans every active
// config leaf and bubbles up its ancestor spine, so once the parallel completes
// the final-leaf spines reach the enclosing compound and its guarded Always is
// reachable. These tests lock that the done-transition (1) is taken once the
// parallel completes, in the same macrostep, alongside par.OnDone and
// outer.OnDone; and (2) is NOT taken while a region is still incomplete.

// edtCtx threads a completion counter the region finals increment; the enclosing
// compound's done-transition guards on it so it fires only once both regions are
// final.
type edtCtx struct {
	done    int
	current string
}

// TestParallel_EnclosingDoneTransition_TakenOnParallelComplete drives the core
// sub-case: a compound "outer" holds a parallel "par" (regions a, b) plus a
// guarded done-transition outer --(Always, bothDone)--> next. When both regions
// reach final in one macrostep, the run-to-completion loop must take the
// done-transition to "next" within the SAME macrostep, and both par.OnDone
// ("Pdone") and outer.OnDone ("Odone") must have fired exactly once.
//
// This is the transition analog of the OnDone cascade fixed in 94f3302: it
// proves a parallel completing inside a compound routes through the same eventless
// selection that a nested compound reaching final already used, so a done-
// transition on the enclosing compound is not starved for the parallel case.
func TestParallel_EnclosingDoneTransition_TakenOnParallelComplete(t *testing.T) {
	note := func(s string) state.ActionFn[edtCtx] {
		return func(state.ActionCtx[edtCtx]) (state.Effect, error) { return s, nil }
	}
	bump := func(c state.AssignCtx[edtCtx]) edtCtx { c.Entity.done++; return c.Entity }
	bothDone := func(c state.GuardCtx[edtCtx]) bool { return c.Entity.done >= 2 }

	m := state.Forge[string, string, edtCtx]("par-enclosing-done-transition").
		Action("Pdone", note("Pdone")).
		Action("Odone", note("Odone")).
		Reducer("bump", bump).
		Guard("bothDone", bothDone).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("outer").OnDone("Odone").
		Initial("par").
		SuperState("par").OnDone("Pdone").
		Region("a").Initial("a1").SubState("a1").SubState("af").Final().OnEntryAssign("bump").EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("bf").Final().OnEntryAssign("bump").EndRegion().
		EndSuperState(). // close par
		// The done-TRANSITION on the enclosing compound: an eventless transition
		// guarded on completion. It is reachable only after the parallel completes,
		// so taking it proves the enclosing compound's done-transition fires for the
		// parallel-in-compound shape.
		Transition("outer").Always().When("bothDone").GoTo("next").
		EndSuperState(). // close outer
		State("next").
		Initial("off").
		CurrentStateFn(func(c edtCtx) string { return c.current }).
		Transition("a1").On("e").GoTo("af").
		Transition("b1").On("e").GoTo("bf").
		Quench()

	inst := m.Cast(edtCtx{current: "off"}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering nested parallel: %v", res.Err)
	}

	// Both regions reach final on the same event; the parallel completes, its
	// OnDone and the enclosing compound's OnDone settle, and the enclosing
	// compound's guarded done-transition must then fire to "next" — all in this one
	// macrostep.
	res := inst.Fire(ctx, "e")
	if res.Err != nil {
		t.Fatalf("e errored: %v (config=%v)", res.Err, inst.Configuration())
	}

	// par.OnDone and outer.OnDone must each have fired exactly once.
	if got := countEffect(res.Effects, "Pdone"); got != 1 {
		t.Fatalf("par.OnDone fired %d times, want exactly 1 (effects=%v)", got, stringEffects(res.Effects))
	}
	if got := countEffect(res.Effects, "Odone"); got != 1 {
		t.Fatalf("outer.OnDone fired %d times, want exactly 1 (effects=%v)", got, stringEffects(res.Effects))
	}

	// The done-transition was taken: the machine has left "outer" for "next".
	if got := inst.Configuration(); !configContains(got, "next") {
		t.Fatalf("done-transition not taken: config = %v, want [next]\n"+
			"the enclosing compound's guarded Always must fire once the parallel inside it completes",
			got)
	}
	if got := inst.Configuration(); configContains(got, "af") || configContains(got, "bf") || configContains(got, "par") || configContains(got, "outer") {
		t.Fatalf("done-transition left stale parallel leaves active: config = %v, want only [next]", got)
	}
}

// TestParallel_EnclosingDoneTransition_NotTakenWhileIncomplete is the control: the
// same shape, but only region a is driven to final. The parallel is therefore NOT
// complete, the guard stays false, and the enclosing compound's done-transition
// must NOT be taken — the machine stays inside "par"/"outer", and neither
// par.OnDone nor outer.OnDone fires. This guards against an unguarded or
// completion-blind done-transition firing prematurely.
func TestParallel_EnclosingDoneTransition_NotTakenWhileIncomplete(t *testing.T) {
	note := func(s string) state.ActionFn[edtCtx] {
		return func(state.ActionCtx[edtCtx]) (state.Effect, error) { return s, nil }
	}
	bump := func(c state.AssignCtx[edtCtx]) edtCtx { c.Entity.done++; return c.Entity }
	bothDone := func(c state.GuardCtx[edtCtx]) bool { return c.Entity.done >= 2 }

	m := state.Forge[string, string, edtCtx]("par-enclosing-done-incomplete").
		Action("Pdone", note("Pdone")).
		Action("Odone", note("Odone")).
		Reducer("bump", bump).
		Guard("bothDone", bothDone).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("outer").OnDone("Odone").
		Initial("par").
		SuperState("par").OnDone("Pdone").
		Region("a").Initial("a1").SubState("a1").SubState("af").Final().OnEntryAssign("bump").EndRegion().
		Region("b").Initial("b1").SubState("b1").SubState("bf").Final().OnEntryAssign("bump").EndRegion().
		EndSuperState().
		Transition("outer").Always().When("bothDone").GoTo("next").
		EndSuperState().
		State("next").
		Initial("off").
		CurrentStateFn(func(c edtCtx) string { return c.current }).
		Transition("a1").On("ea").GoTo("af").
		Transition("b1").On("eb").GoTo("bf").
		Quench()

	inst := m.Cast(edtCtx{current: "off"}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering nested parallel: %v", res.Err)
	}

	// Only region a reaches final. The parallel is not complete; the done-guard
	// stays false.
	res := inst.Fire(ctx, "ea")
	if res.Err != nil {
		t.Fatalf("ea errored: %v (config=%v)", res.Err, inst.Configuration())
	}

	if got := countEffect(res.Effects, "Pdone"); got != 0 {
		t.Fatalf("par.OnDone fired %d times while region b is still active, want 0 (effects=%v)", got, stringEffects(res.Effects))
	}
	if got := countEffect(res.Effects, "Odone"); got != 0 {
		t.Fatalf("outer.OnDone fired %d times while the parallel is incomplete, want 0 (effects=%v)", got, stringEffects(res.Effects))
	}
	if got := inst.Configuration(); configContains(got, "next") {
		t.Fatalf("done-transition taken prematurely: config = %v, must not contain next while region b is still active", got)
	}
	if got := inst.Configuration(); !configContains(got, "af") || !configContains(got, "b1") {
		t.Fatalf("config = %v, want region a at final af and region b still at b1", got)
	}
}

// configContains reports whether the settled configuration contains the named
// leaf.
func configContains(cfg []string, want string) bool {
	for _, l := range cfg {
		if l == want {
			return true
		}
	}
	return false
}
