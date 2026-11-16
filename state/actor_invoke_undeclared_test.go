package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// spawnEffect returns the SpawnActor entering supervising emits for m.
func spawnEffect[E comparable](t *testing.T, m *state.Machine[string, E, *trec], start E) state.SpawnActor {
	t.Helper()
	res := m.Cast(&trec{}, state.WithInitialState("idle")).Fire(context.Background(), start)
	for _, eff := range res.Effects {
		if sp, ok := eff.(state.SpawnActor); ok {
			return sp
		}
	}
	t.Fatal("entering supervising spawned no actor")
	return state.SpawnActor{}
}

func invokeParent(opts ...state.InvokeOption) *state.Machine[string, string, *trec] {
	return state.ForgeFor[*trec]("parent").
		Actor("child").
		State("idle").
		State("supervising").InvokeActor("child", opts...).
		Initial("idle").
		Transition("idle").On("start").GoTo("supervising").
		Quench()
}

// TestInvokeActor_NoOnError_Flags pins SpawnActor.NoOnError: set when the invoke
// declared no onError, clear when it declared one.
func TestInvokeActor_NoOnError_Flags(t *testing.T) {
	if sp := spawnEffect(t, invokeParent(), "start"); !sp.NoOnError {
		t.Fatal("NoOnError = false for an invoke with no onError")
	}
	if sp := spawnEffect(t, invokeParent(state.WithInvokeOnError("failed")), "start"); sp.NoOnError {
		t.Fatal("NoOnError = true for an invoke with an onError")
	}
}

// TestInvokeActor_ZeroValueOnError_IsDeclared pins that an onError that is the zero
// event, as the first constant of an iota enum is, still counts as declared.
func TestInvokeActor_ZeroValueOnError_IsDeclared(t *testing.T) {
	const (
		failed = iota
		start
	)
	m := state.Forge[string, int, *trec]("parent").
		Actor("child").
		State("idle").
		State("supervising").InvokeActor("child", state.WithInvokeOnError(failed)).
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On(start).GoTo("supervising").
		Quench()
	if sp := spawnEffect(t, m, start); sp.NoOnError {
		t.Fatal("NoOnError = true for an onError that is the zero event")
	}
}

// TestInvokeActor_NoOnError_ParentFireReportsIt pins that the parent Fire routing a
// failed spawn's onError reports NoOnError as its SpawnActor did, while the zero event
// still fires at the parent, as v1 documents.
func TestInvokeActor_NoOnError_ParentFireReportsIt(t *testing.T) {
	for _, declared := range []bool{false, true} {
		var opts []state.InvokeOption
		if declared {
			opts = append(opts, state.WithInvokeOnError("failed"))
		}
		parent := invokeParent(opts...).Cast(&trec{}, state.WithInitialState("idle"))
		var fires []state.ParentFire[string]
		sys := state.NewActorSystem(parent).
			Register("child", func(map[string]any) (state.ActorInstance, error) {
				return nil, errors.New("the child can't start")
			}).
			WithParentFireObserver(func(pf state.ParentFire[string]) { fires = append(fires, pf) })
		ctx := context.Background()
		sys.Absorb(ctx, parent.Fire(ctx, "start").Effects)
		if len(fires) != 1 || !fires[0].SpawnFailed {
			t.Fatalf("declared=%v: parent Fires = %+v, want the failed spawn's onError", declared, fires)
		}
		if fires[0].NoOnError == declared {
			t.Fatalf("declared=%v: ParentFire.NoOnError = %v", declared, fires[0].NoOnError)
		}
	}
}
