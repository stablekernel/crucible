package state_test

// This file pins the macrostep fan-in of Trace.AssignsApplied: a multi-microstep
// macrostep (a raise-driven cascade) must aggregate the assigns folded in EVERY
// microstep, in order — not just those of the triggering microstep. It guards the
// frozen Trace from under-reporting applied assigns across the run-to-completion
// loop.

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

type maState int

const (
	maStart maState = iota
	maMid
	maEnd
)

type maEvent int

const (
	maGo   maEvent = iota // external trigger
	maKick                // raised internal event
)

// buildRaiseCascadeMachine returns a machine whose single Go macrostep spans
// three microsteps that each fold a distinct assign:
//
//	Start --Go [Assign aStart, Raise Kick]--> Mid
//	Mid   --Kick [Assign aKick]------------->  (stays, eventless next)
//	Mid   --(always) [Assign aAlways]-------->  End
//
// The triggering Go microstep folds aStart; the raised Kick microstep folds
// aKick; the eventless ("always") microstep folds aAlways. A correct macrostep
// Trace reports all three AssignsApplied in microstep order.
func buildRaiseCascadeMachine() *state.Machine[maState, maEvent, any] {
	return state.Forge[maState, maEvent, any]("raiseCascade").
		Reducer("aStart", func(ctx state.AssignCtx[any]) any { return ctx.Entity }).
		Reducer("aKick", func(ctx state.AssignCtx[any]) any { return ctx.Entity }).
		Reducer("aAlways", func(ctx state.AssignCtx[any]) any { return ctx.Entity }).
		State(maStart).
		Transition(maStart).On(maGo).GoTo(maMid).Assign("aStart").Raise(maKick).
		State(maMid).
		Transition(maMid).On(maKick).GoTo(maMid).Assign("aKick").
		Transition(maMid).Always().GoTo(maEnd).Assign("aAlways").
		State(maEnd).
		Initial(maStart).
		CurrentStateFn(func(any) maState { return maStart }).
		Quench()
}

// TestMacrostep_AssignsApplied_AggregatesAllMicrosteps asserts that a multi-
// microstep macrostep's Trace.AssignsApplied includes the assigns folded in every
// microstep (triggering, raised, and eventless), in order — locking the fan-in fix
// so the frozen Trace no longer drops assigns applied after the first microstep.
func TestMacrostep_AssignsApplied_AggregatesAllMicrosteps(t *testing.T) {
	m := buildRaiseCascadeMachine()
	ctx := context.Background()

	inst := m.Cast(nil,
		state.WithInitialState[maState](maStart),
		state.WithFullTrace[maState](),
	)
	res := inst.Fire(ctx, maGo)
	if res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}
	if got, want := inst.Current(), maEnd; got != want {
		t.Fatalf("Current = %v, want %v (cascade did not complete)", got, want)
	}

	want := []string{"aStart", "aKick", "aAlways"}
	got := res.Trace.AssignsApplied
	if len(got) != len(want) {
		t.Fatalf("AssignsApplied = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("AssignsApplied = %v, want %v (order/content mismatch)", got, want)
		}
	}
}
