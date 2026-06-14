package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// nteCtx is the value context for the non-transactional-effects regression.
type nteCtx struct{}

// TestFire_FailedExitCascade_EmitsNoEffects pins the effect-transactionality
// contract (NTE): when a multi-state exit cascade errors partway through, the
// failed Fire returns the error and ZERO effects — the earlier (successful)
// exit's effect must NOT leak into the result, so a host replaying a failed
// Fire cannot double-apply it.
//
// The machine exits a compound "k" (child "c1") to "done". The exit cascade is
// innermost-first: c1 runs first and emits "e1"; k's OnExit then errors. The
// pre-fix behavior returned [e1] alongside the error; the contract requires no
// effects on failure.
func TestFire_FailedExitCascade_EmitsNoEffects(t *testing.T) {
	boom := errors.New("boom")
	emit := func(s string) state.ActionFn[nteCtx] {
		return func(state.ActionCtx[nteCtx]) (state.Effect, error) { return s, nil }
	}
	fail := func(state.ActionCtx[nteCtx]) (state.Effect, error) { return nil, boom }

	m := state.ForgeFor[nteCtx]("nte").
		Action("e1", emit("e1")).
		Action("explode", fail).
		State("off").
		Transition("off").On("go").GoTo("k").
		SuperState("k").OnExit("explode").
		SubState("c1").OnExit("e1").
		Initial("c1").
		Transition("k").On("leave").GoTo("done").
		EndSuperState().
		State("done").Final().
		Initial("off").
		CurrentStateFn(func(nteCtx) string { return "off" }).
		Quench()

	inst := m.Cast(nteCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("setup: %v", res.Err)
	}

	res := inst.Fire(ctx, "leave")
	if res.Err == nil {
		t.Fatalf("expected the failed exit cascade to error, got nil (state=%v)", res.NewState)
	}
	if !errors.Is(res.Err, boom) {
		t.Fatalf("err = %v, want it to wrap boom", res.Err)
	}
	if len(res.Effects) != 0 {
		t.Fatalf("effects = %v; want zero effects on a failed Fire (the earlier exit's effect must not leak)", res.Effects)
	}
}

// TestFire_SuccessfulCascade_EmitsAllEffects is the control: a successful Fire
// of the same shape emits the full effect buffer, proving the transactional
// guard suppresses effects only on the error path.
func TestFire_SuccessfulCascade_EmitsAllEffects(t *testing.T) {
	emit := func(s string) state.ActionFn[nteCtx] {
		return func(state.ActionCtx[nteCtx]) (state.Effect, error) { return s, nil }
	}

	m := state.ForgeFor[nteCtx]("nte-ok").
		Action("e1", emit("e1")).
		Action("ek", emit("ek")).
		State("off").
		Transition("off").On("go").GoTo("k").
		SuperState("k").OnExit("ek").
		SubState("c1").OnExit("e1").
		Initial("c1").
		Transition("k").On("leave").GoTo("done").
		EndSuperState().
		State("done").Final().
		Initial("off").
		CurrentStateFn(func(nteCtx) string { return "off" }).
		Quench()

	inst := m.Cast(nteCtx{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("setup: %v", res.Err)
	}

	res := inst.Fire(ctx, "leave")
	if res.Err != nil {
		t.Fatalf("leave: %v", res.Err)
	}
	var got []string
	for _, e := range res.Effects {
		if s, ok := e.(string); ok {
			got = append(got, s)
		}
	}
	want := []string{"e1", "ek"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("effects = %v; want %v on success", got, want)
	}
}
