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

// TestInvokeActor_NoOnError_FailuresEscalate pins that an invoked actor with no onError
// declared escalates its failure, to start or after it ran, as a spawned actor with none
// does (G3), and fires nothing at the parent; one with an onError routes it.
func TestInvokeActor_NoOnError_FailuresEscalate(t *testing.T) {
	ctx := context.Background()
	for _, declared := range []bool{false, true} {
		for _, when := range []string{"at spawn", "after it ran"} {
			var opts []state.InvokeOption
			if declared {
				opts = append(opts, state.WithInvokeOnError("failed"))
			}
			parent := invokeParent(opts...).Cast(&trec{}, state.WithInitialState("idle"))
			var fires []state.ParentFire[string]
			cause := errors.New("the child failed")
			sys := state.NewActorSystem(parent).
				WithParentFireObserver(func(pf state.ParentFire[string]) { fires = append(fires, pf) })
			if when == "at spawn" {
				sys.Register("child", func(map[string]any) (state.ActorInstance, error) { return nil, cause })
			} else {
				sys.Register("child", childBehavior())
			}
			sys.Absorb(ctx, parent.Fire(ctx, "start").Effects)
			if when == "after it ran" {
				ids := sys.IDs()
				if len(ids) != 1 {
					t.Fatalf("declared=%v %s: running %v, want the child", declared, when, ids)
				}
				sys.SettleError(ctx, ids[0], cause)
			}
			esc := sys.LastEscalation()
			if declared {
				if len(fires) != 1 || esc != nil {
					t.Fatalf("declared=%v %s: parent Fires %d, escalation %v; want the onError routed, no escalation", declared, when, len(fires), esc)
				}
				continue
			}
			if len(fires) != 0 || esc == nil || !errors.Is(esc, cause) {
				t.Fatalf("declared=%v %s: parent Fires %d, escalation %v; want no Fire and the failure escalated", declared, when, len(fires), esc)
			}
		}
	}
}
