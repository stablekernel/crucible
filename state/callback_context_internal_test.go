package state

import (
	"context"
	"errors"
	"strings"
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
// then fails with err, or with the context's error when err is nil, as a binding that
// honors cancellation would.
type cancelingGuard struct {
	cancel context.CancelFunc
	err    error
}

func (g cancelingGuard) EvalGuard(ctx context.Context, _ GuardRequest[bindOrder]) (GuardResult, error) {
	g.cancel()
	if g.err != nil {
		return GuardResult{}, g.err
	}
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

// ctxGuard is a GuardBinding that fails once its context has ended and passes
// otherwise, as a binding that honors cancellation would.
type ctxGuard struct{}

func (ctxGuard) EvalGuard(ctx context.Context, _ GuardRequest[bindOrder]) (GuardResult, error) {
	if err := ctx.Err(); err != nil {
		return GuardResult{}, err
	}
	return GuardResult{OK: true}, nil
}

// TestCast_WithAnEndedContext_SettlesAsWithALiveOne proves Cast's callbacks don't see
// WithContext's context end: an eventless transition behind a bound guard that honors
// cancellation settles the same way with an ended context as with a live one, instead
// of falling through to the next transition.
func TestCast_WithAnEndedContext_SettlesAsWithALiveOne(t *testing.T) {
	m := func() *Machine[string, string, bindOrder] {
		b := ForgeFor[bindOrder]("castctx")
		b.reg.BindGuard("bound", ctxGuard{})
		return b.
			State("deciding").
			Always().GoTo("guarded").When("bound").
			Always().GoTo("fallback").
			State("guarded").
			State("fallback").
			Initial("deciding").
			Quench()
	}
	ended, cancel := context.WithCancel(context.WithValue(context.Background(), ctxTagKey{}, "kept"))
	cancel()
	live := m().Cast(bindOrder{}, WithInitialState("deciding"))
	dead := m().Cast(bindOrder{}, WithInitialState("deciding"), WithContext[string](ended))
	if live.Current() != "guarded" || dead.Current() != "guarded" {
		t.Fatalf("Cast settled in %q with a live context and %q with an ended one, want guarded for both", live.Current(), dead.Current())
	}
}

// fireCanceled builds a machine whose "bound" guard ends the Fire's context and fails
// with bindErr (or the context's error), fires "go" from its initial state, and returns
// the instance and result.
func fireCanceled(t *testing.T, bindErr error, build func(*Builder[string, string, bindOrder]) *Builder[string, string, bindOrder], initial string) (*Instance[string, string, bindOrder], FireResult[string]) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b := ForgeFor[bindOrder]("canceled")
	b.reg.BindGuard("bound", cancelingGuard{cancel: cancel, err: bindErr})
	inst := build(b).Initial(initial).Quench().Cast(bindOrder{}, WithInitialState(initial))
	return inst, inst.Fire(ctx, "go")
}

// TestCanceledGuard_IsNotReportedAsAGuardFailure proves a guard stopped by its ended
// context fails the Fire with the context's error and leaves the Trace's outcome as a
// Fire on an ended context does, wherever the guard sits: a named guard, a guard
// expression, a transition inside a parallel region, or one on the parallel state.
func TestCanceledGuard_IsNotReportedAsAGuardFailure(t *testing.T) {
	tests := []struct {
		name    string
		initial string
		build   func(*Builder[string, string, bindOrder]) *Builder[string, string, bindOrder]
	}{
		{"named guard", "from", func(b *Builder[string, string, bindOrder]) *Builder[string, string, bindOrder] {
			return b.State("from").Transition("from").On("go").GoTo("to").When("bound").State("to")
		}},
		{"guard expression", "from", func(b *Builder[string, string, bindOrder]) *Builder[string, string, bindOrder] {
			return b.State("from").Transition("from").On("go").GoTo("to").WhenExpr(Guard[string]("bound")).State("to")
		}},
		{"parallel region", "live", func(b *Builder[string, string, bindOrder]) *Builder[string, string, bindOrder] {
			return b.SuperState("live").
				Region("r1").Initial("r1a").
				SubState("r1a").On("go").GoTo("r1b").When("bound").
				SubState("r1b").
				EndRegion().
				Region("r2").Initial("r2a").SubState("r2a").EndRegion().
				EndSuperState()
		}},
		{"parallel state", "live", func(b *Builder[string, string, bindOrder]) *Builder[string, string, bindOrder] {
			return b.SuperState("live").
				Region("r1").Initial("r1a").SubState("r1a").EndRegion().
				Region("r2").Initial("r2a").SubState("r2a").EndRegion().
				EndSuperState().
				Transition("live").On("go").GoTo("out").When("bound").
				State("out")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst, res := fireCanceled(t, nil, tt.build, tt.initial)
			if !errors.Is(res.Err, context.Canceled) {
				t.Fatalf("Fire err = %v, want context.Canceled", res.Err)
			}
			if o := res.Trace.Outcome; o == OutcomeGuardPanic || o == OutcomeGuardFailed {
				t.Fatalf("Trace outcome %v reports a guard failure, want the outcome of a canceled Fire", o)
			}
			if leaves := inst.Configuration(); len(leaves) == 0 || (leaves[0] != "from" && leaves[0] != "r1a") {
				t.Fatalf("configuration %v after a canceled Fire, want it unchanged", leaves)
			}
		})
	}
}

// TestCanceledGuard_KeepsTheBindingsError proves the Fire's error names the guard and
// wraps both the binding's own error and the context's, so errors.Is matches either, on an
// event-driven transition and on an eventless one.
func TestCanceledGuard_KeepsTheBindingsError(t *testing.T) {
	tests := []struct {
		name  string
		build func(*Builder[string, string, bindOrder]) *Builder[string, string, bindOrder]
	}{
		{"event-driven", func(b *Builder[string, string, bindOrder]) *Builder[string, string, bindOrder] {
			return b.State("from").Transition("from").On("go").GoTo("to").When("bound").State("to")
		}},
		{"eventless", func(b *Builder[string, string, bindOrder]) *Builder[string, string, bindOrder] {
			return b.State("from").
				Transition("from").On("go").GoTo("deciding").
				State("deciding").
				Always().GoTo("to").When("bound").
				State("to")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookupErr := errors.New("lookup failed")
			_, res := fireCanceled(t, lookupErr, tt.build, "from")
			if !errors.Is(res.Err, context.Canceled) || !errors.Is(res.Err, lookupErr) {
				t.Fatalf("Fire err = %v, want it to match context.Canceled and the binding's error", res.Err)
			}
			if msg := res.Err.Error(); !strings.Contains(msg, "lookup failed") || !strings.Contains(msg, `"bound"`) {
				t.Fatalf("Fire err %q doesn't name the guard and keep its error", msg)
			}
		})
	}
}

// TestCanceledEventlessGuard_WithNothingChosen_FailsTheFire proves a Fire whose context
// ended while its only eventless transition's guard ran fails and leaves the instance
// as it was, rather than settling where the triggering transition left it.
func TestCanceledEventlessGuard_WithNothingChosen_FailsTheFire(t *testing.T) {
	inst, res := fireCanceled(t, nil, func(b *Builder[string, string, bindOrder]) *Builder[string, string, bindOrder] {
		return b.State("from").
			Transition("from").On("go").GoTo("deciding").
			State("deciding").
			Always().GoTo("guarded").When("bound").
			State("guarded")
	}, "from")
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("Fire err = %v, want context.Canceled", res.Err)
	}
	if got := inst.Current(); got != "from" {
		t.Fatalf("state %q after a canceled Fire, want from", got)
	}
}

// valueGuard is a GuardBinding that records the tag and the error its context carries.
type valueGuard struct {
	tag *string
	err *error
}

func (g valueGuard) EvalGuard(ctx context.Context, _ GuardRequest[bindOrder]) (GuardResult, error) {
	*g.tag, _ = ctx.Value(ctxTagKey{}).(string)
	*g.err = ctx.Err()
	return GuardResult{OK: true}, nil
}

// TestCast_WithAnEndedContext_KeepsItsValues proves Cast's callbacks get WithContext's
// values, such as a host's step marks, but not its cancellation.
func TestCast_WithAnEndedContext_KeepsItsValues(t *testing.T) {
	var tag string
	var ctxErr error
	b := ForgeFor[bindOrder]("castvalues")
	b.reg.BindGuard("bound", valueGuard{tag: &tag, err: &ctxErr})
	m := b.
		State("deciding").
		Always().GoTo("done").When("bound").
		State("done").
		Initial("deciding").
		Quench()
	ended, cancel := context.WithCancel(context.WithValue(context.Background(), ctxTagKey{}, "kept"))
	cancel()
	m.Cast(bindOrder{}, WithInitialState("deciding"), WithContext[string](ended))
	if tag != "kept" || ctxErr != nil {
		t.Fatalf("the guard saw tag %q and context error %v, want kept and none", tag, ctxErr)
	}
}
