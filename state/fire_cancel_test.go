package state_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// The cancellation suite drives a machine whose single macrostep settles across
// several microsteps, so a context canceled at (or before) a microstep boundary
// must abort the run-to-completion loop and roll the instance back to its pre-Fire
// configuration — leaving a clean no-op on persisted state.

type cancelState int

const (
	cancelStart cancelState = iota
	cancelMid
	cancelStep2
	cancelStep3
	cancelFinal
)

type cancelEvent int

const (
	cancelGo cancelEvent = iota
	cancelKick
)

// cancelCtx records each effect tag the macrostep emits, so a test can confirm a
// rolled-back Fire left no committed context change and a normal Fire folded the
// full chain.
type cancelCtx struct {
	Log []string
}

// tagCancelEffect returns an action that emits its tag as a string effect.
func tagCancelEffect(tag string) func(state.ActionCtx[cancelCtx]) (state.Effect, error) {
	return func(state.ActionCtx[cancelCtx]) (state.Effect, error) {
		return tag, nil
	}
}

// buildCancelMachine forges a machine with a multi-microstep macrostep:
//
//	Start --Go (raises Kick)--> Mid --always--> Step2 --always--> Step3 --always--> Final
//
// The Go transition raises an internal Kick (drained as the first RTC microstep)
// and lands in Mid, whose eventless chain then walks Step2 -> Step3 -> Final. One
// Fire(Go) therefore settles across multiple microsteps, giving cancellation a
// boundary to take effect at.
func buildCancelMachine() *state.Machine[cancelState, cancelEvent, cancelCtx] {
	return state.Forge[cancelState, cancelEvent, cancelCtx]("cancel").
		Action("atStart", tagCancelEffect("atStart")).
		Action("atMid", tagCancelEffect("atMid")).
		Action("atStep2", tagCancelEffect("atStep2")).
		Action("atStep3", tagCancelEffect("atStep3")).
		Action("atFinal", tagCancelEffect("atFinal")).
		State(cancelStart).
		Transition(cancelStart).On(cancelGo).GoTo(cancelMid).
		Do("atStart", state.P{}).
		Raise(cancelKick).
		State(cancelMid).
		OnEntry("atMid", state.P{}).
		Always().GoTo(cancelStep2).
		State(cancelStep2).
		OnEntry("atStep2", state.P{}).
		Always().GoTo(cancelStep3).
		State(cancelStep3).
		OnEntry("atStep3", state.P{}).
		Always().GoTo(cancelFinal).
		State(cancelFinal).
		OnEntry("atFinal", state.P{}).
		Initial(cancelStart).
		Quench()
}

// newCancelInstance casts a fresh instance of the cancel machine pinned at Start
// with full trace, the shape the cancellation tests assert against.
func newCancelInstance(t *testing.T) *state.Instance[cancelState, cancelEvent, cancelCtx] {
	t.Helper()
	m := buildCancelMachine()
	return m.Cast(cancelCtx{},
		state.WithInitialState[cancelState](cancelStart),
		state.WithFullTrace[cancelState](),
	)
}

// TestFire_AlreadyCancelledContext_RollsBack asserts that firing with a context
// that is already canceled is a clean no-op: it surfaces the cancellation cause,
// leaves the instance at its pre-Fire configuration, and emits no effects.
func TestFire_AlreadyCancelledContext_RollsBack(t *testing.T) {
	tests := []struct {
		name    string
		ctxFn   func() (context.Context, context.CancelFunc)
		wantErr error
	}{
		{
			name: "canceled",
			ctxFn: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			wantErr: context.Canceled,
		},
		{
			name: "deadline_exceeded",
			ctxFn: func() (context.Context, context.CancelFunc) {
				// A deadline already in the past makes ctx.Err report DeadlineExceeded
				// immediately, so the first boundary check aborts the macrostep.
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
				return ctx, cancel
			},
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inst := newCancelInstance(t)
			ctx, cancel := tc.ctxFn()
			defer cancel()

			res := inst.Fire(ctx, cancelGo)

			if !errors.Is(res.Err, tc.wantErr) {
				t.Fatalf("Fire err = %v, want errors.Is(..., %v)", res.Err, tc.wantErr)
			}
			if res.Effects != nil {
				t.Fatalf("canceled Fire emitted effects %v, want nil", res.Effects)
			}
			if res.NewState != cancelStart {
				t.Fatalf("canceled Fire NewState = %v, want %v (rolled back)", res.NewState, cancelStart)
			}
			if got := inst.Configuration(); len(got) != 1 || got[0] != cancelStart {
				t.Fatalf("canceled Fire left configuration %v, want [%v]", got, cancelStart)
			}
			if entity := inst.Entity(); len(entity.Log) != 0 {
				t.Fatalf("canceled Fire folded context %v, want empty", entity.Log)
			}
		})
	}
}

// TestFire_CancelDuringMacrostep_RollsBack cancels the context in the middle of an
// action the macrostep runs, so cancellation is observable only at the NEXT
// microstep boundary — not mid-action. The whole macrostep must still roll back:
// the in-flight microstep completes, then the boundary check aborts and unwinds to
// Start with no effects, proving cancellation never tears a single microstep apart.
func TestFire_CancelDuringMacrostep_RollsBack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The first transition's action cancels the context as a side effect. Because the
	// boundary check sits between microsteps, the triggering microstep still finishes
	// in full (it has begun and is never interrupted); the run-to-completion loop then
	// observes the cancellation at its next boundary and aborts, rolling the macrostep
	// back to Start.
	m := state.Forge[cancelState, cancelEvent, cancelCtx]("cancel-midflight").
		Action("cancelNow", func(state.ActionCtx[cancelCtx]) (state.Effect, error) {
			cancel()
			return "cancelNow", nil
		}).
		Action("atMid", tagCancelEffect("atMid")).
		Action("atStep2", tagCancelEffect("atStep2")).
		State(cancelStart).
		Transition(cancelStart).On(cancelGo).GoTo(cancelMid).
		Do("cancelNow", state.P{}).
		Raise(cancelKick).
		State(cancelMid).
		OnEntry("atMid", state.P{}).
		Always().GoTo(cancelStep2).
		State(cancelStep2).
		OnEntry("atStep2", state.P{}).
		Initial(cancelStart).
		Quench()

	inst := m.Cast(cancelCtx{},
		state.WithInitialState[cancelState](cancelStart),
		state.WithFullTrace[cancelState](),
	)

	res := inst.Fire(ctx, cancelGo)

	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("Fire err = %v, want errors.Is(..., context.Canceled)", res.Err)
	}
	if res.Effects != nil {
		t.Fatalf("canceled Fire emitted effects %v, want nil", res.Effects)
	}
	if res.NewState != cancelStart {
		t.Fatalf("canceled Fire NewState = %v, want %v (rolled back)", res.NewState, cancelStart)
	}
	if got := inst.Configuration(); len(got) != 1 || got[0] != cancelStart {
		t.Fatalf("canceled Fire left configuration %v, want [%v]", got, cancelStart)
	}
	if entity := inst.Entity(); len(entity.Log) != 0 {
		t.Fatalf("canceled Fire folded context %v, want empty", entity.Log)
	}
}

// TestFire_NormalContext_Unaffected confirms the happy path is untouched: a
// non-canceled Fire settles the full eventless chain to Final and folds every
// effect in emission order.
func TestFire_NormalContext_Unaffected(t *testing.T) {
	inst := newCancelInstance(t)

	res := inst.Fire(context.Background(), cancelGo)

	if res.Err != nil {
		t.Fatalf("normal Fire err = %v, want nil", res.Err)
	}
	if res.NewState != cancelFinal {
		t.Fatalf("normal Fire NewState = %v, want %v", res.NewState, cancelFinal)
	}
	if got := inst.Configuration(); len(got) != 1 || got[0] != cancelFinal {
		t.Fatalf("normal Fire configuration = %v, want [%v]", got, cancelFinal)
	}
	want := []string{"atStart", "atMid", "atStep2", "atStep3", "atFinal"}
	got := effectStrings(res.Effects)
	if len(got) != len(want) {
		t.Fatalf("normal Fire effects = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("normal Fire effects = %v, want %v", got, want)
		}
	}
}

// effectStrings projects the string effects of a result in emission order.
func effectStrings(effs []state.Effect) []string {
	out := make([]string, 0, len(effs))
	for _, e := range effs {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
