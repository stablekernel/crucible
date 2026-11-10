package state

import (
	"context"
	"errors"
	"testing"
)

// ctxTagKey tags the context a test passes to Fire.
type ctxTagKey struct{}

// tagGuard is a GuardBinding that records the tag on the context EvalGuard gets.
type tagGuard struct{ seen *string }

func (g tagGuard) EvalGuard(ctx context.Context, _ GuardRequest[bindOrder]) (GuardResult, error) {
	*g.seen, _ = ctx.Value(ctxTagKey{}).(string)
	return GuardResult{OK: true}, nil
}

// TestBindGuard_GetsTheFiresContext proves a guard registered with BindGuard gets the
// Fire's context through EvalGuard, not a background one.
func TestBindGuard_GetsTheFiresContext(t *testing.T) {
	var seen string
	b := ForgeFor[bindOrder]("bound")
	b.reg.BindGuard("bound", tagGuard{seen: &seen})
	m := b.
		State("from").
		Transition("from").On("go").GoTo("to").When("bound").
		State("to").
		Initial("from").
		Quench()
	ctx := context.WithValue(context.Background(), ctxTagKey{}, "fire")
	if res := m.Cast(bindOrder{}, WithInitialState("from")).Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("Fire: %v", res.Err)
	}
	if seen != "fire" {
		t.Fatalf("the bound guard's EvalGuard saw %q, want fire", seen)
	}
}

// cancelingGuard is a GuardBinding that ends the Fire's context while it evaluates,
// then fails, as a binding that honors cancellation would.
type cancelingGuard struct{ cancel context.CancelFunc }

func (g cancelingGuard) EvalGuard(ctx context.Context, _ GuardRequest[bindOrder]) (GuardResult, error) {
	g.cancel()
	return GuardResult{}, ctx.Err()
}

// TestBindGuard_CanceledMidEvaluation_FailsTheFire proves a bound guard that fails
// because the Fire's context ended stops the Fire with the context's error, on an
// event-driven transition and on an eventless one. The fallback transition's reducer
// never runs: the guard isn't read as false. (The cancellation check after the
// microstep would roll a committed fallback back, but only after its callbacks ran.)
func TestBindGuard_CanceledMidEvaluation_FailsTheFire(t *testing.T) {
	tests := []struct {
		name  string
		build func(*Builder[string, string, bindOrder]) *Builder[string, string, bindOrder]
	}{
		{"event-driven", func(b *Builder[string, string, bindOrder]) *Builder[string, string, bindOrder] {
			return b.
				State("from").
				Transition("from").On("go").GoTo("guarded").When("bound").
				Transition("from").On("go").GoTo("fallback").Assign("ranFallback").
				State("guarded").
				State("fallback")
		}},
		{"eventless", func(b *Builder[string, string, bindOrder]) *Builder[string, string, bindOrder] {
			return b.
				State("from").
				Transition("from").On("go").GoTo("deciding").
				State("deciding").
				Always().GoTo("guarded").When("bound").
				Always().GoTo("fallback").Assign("ranFallback").
				State("guarded").
				State("fallback")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b := ForgeFor[bindOrder]("canceled")
			b.reg.BindGuard("bound", cancelingGuard{cancel: cancel})
			fallbackRan := false
			b.Reducer("ranFallback", func(in AssignCtx[bindOrder]) bindOrder {
				fallbackRan = true
				return in.Entity
			})
			m := tt.build(b).Initial("from").Quench()
			inst := m.Cast(bindOrder{}, WithInitialState("from"))
			res := inst.Fire(ctx, "go")
			if !errors.Is(res.Err, context.Canceled) {
				t.Fatalf("Fire err = %v, want context.Canceled", res.Err)
			}
			if got := inst.Current(); got != "from" {
				t.Fatalf("state %q after a canceled Fire, want from", got)
			}
			if fallbackRan {
				t.Fatal("the fallback transition's reducer ran under the ended context")
			}
		})
	}
}
