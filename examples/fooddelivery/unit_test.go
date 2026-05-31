package fooddelivery_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
)

// TestModel_GuardAdmitsAndBlocks drives the authorization guard expression directly
// through Fire — exercising the real Rich (CEL) guard and the Core compare/membership
// branches, not the placeholder. It checks the four corners: a generous total admits
// via the CEL branch; a big fast-lane basket admits via the Core branch; a small or
// non-fast order is blocked and stays in Authorizing.
func TestModel_GuardAdmitsAndBlocks(t *testing.T) {
	cases := []struct {
		name  string
		order fooddelivery.Order
		admit bool
	}{
		{"generous via CEL", fooddelivery.Order{Subtotal: 3000, Tip: 3500, Priority: "standard"}, true},
		{"big fast-lane basket via Core", fooddelivery.Order{Subtotal: 5500, Tip: 0, Priority: "express"}, true},
		{"small order blocked", fooddelivery.Order{Subtotal: 1000, Tip: 100, Priority: "fast"}, false},
		{"big but not fast-lane blocked", fooddelivery.Order{Subtotal: 5500, Tip: 0, Priority: "standard"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rig, err := fooddelivery.NewRig(fooddelivery.WithOrder(tc.order))
			if err != nil {
				t.Fatalf("NewRig: %v", err)
			}
			rig.Submit(ctx)
			rig.SettleAuthorization(ctx, true)

			got := rig.Configuration()[0]
			if tc.admit && got != fooddelivery.Cooking {
				t.Fatalf("%s: expected admission to Cooking, got %v", tc.name, got)
			}
			if !tc.admit && got != fooddelivery.Authorizing {
				t.Fatalf("%s: expected to stay Authorizing (blocked), got %v", tc.name, got)
			}
		})
	}
}

// TestServices_AuthorizeAndRefund exercises the payment service function bodies
// (authorizeFn/refundFn) by RUNNING them through the rig's ServiceRunner rather than
// settling them with fixed values: a positive basket authorizes and admits; an empty
// basket is declined to Rejected; and the refund service reverses a held
// authorization and lands the saga in Canceled.
func TestServices_AuthorizeAndRefund(t *testing.T) {
	ctx := context.Background()

	t.Run("authorize admits a positive basket", func(t *testing.T) {
		rig, err := fooddelivery.NewRig(fooddelivery.WithOrder(fooddelivery.Order{Subtotal: 8000, Tip: 0, Priority: "fast"}))
		if err != nil {
			t.Fatalf("NewRig: %v", err)
		}
		rig.Submit(ctx)
		if _, ran := rig.RunAuthorization(ctx); !ran {
			t.Fatal("expected the authorize service to run")
		}
		if got := rig.Configuration()[0]; got != fooddelivery.Cooking {
			t.Fatalf("authorize should admit a positive basket; got %v", got)
		}
	})

	t.Run("authorize declines an empty basket", func(t *testing.T) {
		rig, err := fooddelivery.NewRig(fooddelivery.WithOrder(fooddelivery.Order{Subtotal: 0}))
		if err != nil {
			t.Fatalf("NewRig: %v", err)
		}
		rig.Submit(ctx)
		if _, ran := rig.RunAuthorization(ctx); !ran {
			t.Fatal("expected the authorize service to run")
		}
		if got := rig.Configuration()[0]; got != fooddelivery.Rejected {
			t.Fatalf("authorize should decline an empty basket; got %v", got)
		}
	})

	t.Run("refund reverses a held authorization", func(t *testing.T) {
		rig, err := fooddelivery.NewRig(fooddelivery.WithOrder(fooddelivery.Order{Subtotal: 5000, Tip: 1500, Priority: "fast"}))
		if err != nil {
			t.Fatalf("NewRig: %v", err)
		}
		rig.Submit(ctx)
		rig.RunAuthorization(ctx) // hold funds
		rig.Cancel(ctx)           // open the saga, arming refund
		if _, ran := rig.RunRefund(ctx); !ran {
			t.Fatal("expected the refund service to run")
		}
		if got := rig.Configuration()[0]; got != fooddelivery.Canceled {
			t.Fatalf("refund should land the saga in Canceled; got %v", got)
		}
		if got := rig.Order().Refund; got != 6500 {
			t.Fatalf("refund should reverse subtotal+tip=6500; got %d", got)
		}
	})
}

// TestRig_RestoreAndAccessors covers the rig's restore-from-snapshot path and the
// read-only accessors, including WithClockStart and RunningActors.
func TestRig_RestoreAndAccessors(t *testing.T) {
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(
		fooddelivery.WithOrder(fooddelivery.Order{Subtotal: 9000, Tip: 0, Priority: "fast"}),
		fooddelivery.WithClockStart(time.Unix(1000, 0).UTC()),
	)
	if err != nil {
		t.Fatalf("NewRig: %v", err)
	}

	rig.Submit(ctx)
	rig.SettleAuthorization(ctx, true)
	if rig.RunningActors() != 1 {
		t.Fatalf("an active order should run one kitchen actor; running=%d", rig.RunningActors())
	}

	snap := rig.Snapshot()
	restored, err := fooddelivery.RestoreRig(snap, fooddelivery.WithClockStart(time.Unix(2000, 0).UTC()))
	if err != nil {
		t.Fatalf("RestoreRig: %v", err)
	}
	if got := restored.Configuration()[0]; got != fooddelivery.Cooking {
		t.Fatalf("restored order should resume in Cooking; got %v", got)
	}
	if restored.RunningActors() != 1 {
		t.Fatalf("restored order should resume its kitchen actor; running=%d", restored.RunningActors())
	}
}

// TestStage_StringFallback covers the String fallback arms for out-of-range values.
func TestStage_StringFallback(t *testing.T) {
	if got := fooddelivery.Stage(-1).String(); got != "Stage?" {
		t.Fatalf("Stage(-1).String() = %q, want Stage?", got)
	}
	if got := fooddelivery.Signal(-1).String(); got != "Signal?" {
		t.Fatalf("Signal(-1).String() = %q, want Signal?", got)
	}
	// Spot-check that every defined stage and signal renders non-empty and not the
	// fallback, so the String tables stay in sync with the const blocks.
	for s := fooddelivery.Placed; s <= fooddelivery.Rejected; s++ {
		if got := s.String(); got == "" || got == "Stage?" {
			t.Fatalf("stage %d renders %q", s, got)
		}
	}
	for e := fooddelivery.Submit; e <= fooddelivery.Refunded; e++ {
		if got := e.String(); got == "" || got == "Signal?" {
			t.Fatalf("signal %d renders %q", e, got)
		}
	}
}

// driveAuthorized casts the model at Placed, fires Submit to arm authorization, then
// settles the authorize service with a fixed hold token so the Authorized transition's
// guard is evaluated. It returns the resting stage: Cooking when the order was admitted
// into the Active fulfillment configuration, or Authorizing when the guard blocked it.
func driveAuthorized(t *testing.T, m *state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], order fooddelivery.Order) fooddelivery.Stage {
	t.Helper()
	ctx := context.Background()
	inst := m.Cast(order, state.WithInitialState(fooddelivery.Placed))
	run := state.NewServiceRunner(inst, fooddelivery.ServiceRegistry())
	run.Absorb(ctx, inst.StartEffects())
	run.Absorb(ctx, inst.Fire(ctx, fooddelivery.Submit).Effects)
	if _, ok := run.SettleDone(ctx, state.InvokeID("order", fooddelivery.Authorizing, 0), "tok-test"); !ok {
		t.Fatal("expected an authorize service in flight to settle")
	}
	return inst.Configuration()[0]
}

// TestNewModel_DefaultGuardStillCEL asserts the additive Option seam left NewModel()
// behaving exactly as before: with no options it builds the CEL generous-order guard,
// which admits a generous, non-fast-lane order via the generous branch and blocks a
// small one — proving the default path reproduces the original rich-tier guard.
func TestNewModel_DefaultGuardStillCEL(t *testing.T) {
	m, err := fooddelivery.NewModel()
	if err != nil {
		t.Fatalf("NewModel: %v", err)
	}
	// Isolate the generous branch: standard priority and subtotal below the expedite
	// threshold, so only the generous predicate (subtotal+tip >= 6000) can admit.
	if got := driveAuthorized(t, m, fooddelivery.Order{Subtotal: 3000, Tip: 3500, Priority: "standard"}); got != fooddelivery.Cooking {
		t.Fatalf("generous order should be admitted to Cooking; got %v", got)
	}
	if got := driveAuthorized(t, m, fooddelivery.Order{Subtotal: 3000, Tip: 1000, Priority: "standard"}); got != fooddelivery.Authorizing {
		t.Fatalf("small order should stay in Authorizing; got %v", got)
	}
}

// TestNewModel_WithGenerousGuardInjects asserts WithGenerousGuard swaps the engine that
// computes the generous verdict while leaving the machine unchanged: an injected Core
// guard (subtotal >= 6000, ignoring tip) governs the Authorized decision, admitting an
// order the CEL guard would have blocked and blocking one it would have admitted —
// proving the injected node, not the default, governs the transition.
func TestNewModel_WithGenerousGuardInjects(t *testing.T) {
	// A Go-func leaf bound under the generous name: generous iff subtotal alone >= 6000
	// (ignoring tip), a deliberately different predicate so the swap is observable.
	injected := func(reg *state.Registry[fooddelivery.Order], _ state.ContextSchema) (state.GuardNode[fooddelivery.Stage], error) {
		reg.Guard(fooddelivery.GenerousGuardName, func(g state.GuardCtx[fooddelivery.Order]) bool {
			return g.Entity.Subtotal >= 6000
		})
		return state.Guard[fooddelivery.Stage](fooddelivery.GenerousGuardName), nil
	}
	m, err := fooddelivery.NewModel(fooddelivery.WithGenerousGuard(injected))
	if err != nil {
		t.Fatalf("NewModel(WithGenerousGuard): %v", err)
	}
	// subtotal 7000 alone admits under the injected guard (the CEL guard, on subtotal+tip,
	// would also admit — but this proves the injected leaf governs and resolves).
	if got := driveAuthorized(t, m, fooddelivery.Order{Subtotal: 7000, Tip: 0, Priority: "standard"}); got != fooddelivery.Cooking {
		t.Fatalf("injected guard should admit subtotal>=6000; got %v", got)
	}
	// subtotal 3000 + tip 3500 = 6500: the CEL guard WOULD admit, but the injected guard
	// (subtotal only) blocks it — the discriminating case proving the swap took effect.
	if got := driveAuthorized(t, m, fooddelivery.Order{Subtotal: 3000, Tip: 3500, Priority: "standard"}); got != fooddelivery.Authorizing {
		t.Fatalf("injected guard ignores tip and should block subtotal<6000; got %v", got)
	}
}

// TestNewModel_WithGenerousGuardError surfaces a builder error through NewModel rather
// than swallowing it, so a mis-wired engine fails loudly at construction.
func TestNewModel_WithGenerousGuardError(t *testing.T) {
	sentinel := errors.New("boom")
	_, err := fooddelivery.NewModel(fooddelivery.WithGenerousGuard(
		func(*state.Registry[fooddelivery.Order], state.ContextSchema) (state.GuardNode[fooddelivery.Stage], error) {
			return state.GuardNode[fooddelivery.Stage]{}, sentinel
		},
	))
	if !errors.Is(err, sentinel) {
		t.Fatalf("NewModel should surface the builder error; got %v", err)
	}
}

// TestGenerousGuardSource_MatchesPredicate pins the exported predicate source the
// alternate-engine builders must reproduce, so a drift in the CEL source is caught.
func TestGenerousGuardSource_MatchesPredicate(t *testing.T) {
	if got := fooddelivery.GenerousGuardSource(); got != "subtotal + tip >= 6000" {
		t.Fatalf("GenerousGuardSource() = %q, want the subtotal+tip>=6000 predicate", got)
	}
	if fooddelivery.GenerousGuardName != "generousOrder" {
		t.Fatalf("GenerousGuardName = %q, want generousOrder", fooddelivery.GenerousGuardName)
	}
}

// TestServiceRegistry_HoldsPaymentServices asserts the exported ServiceRegistry binds
// the payment services a host's runner needs, so an embedding host can run them
// rather than settling deterministically.
func TestServiceRegistry_HoldsPaymentServices(t *testing.T) {
	reg := fooddelivery.ServiceRegistry()
	if reg == nil {
		t.Fatal("ServiceRegistry returned nil")
	}
	// A runner built from it resolves and runs the authorize service end to end.
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(fooddelivery.WithOrder(fooddelivery.Order{Subtotal: 7000, Priority: "fast"}))
	if err != nil {
		t.Fatalf("NewRig: %v", err)
	}
	rig.Submit(ctx)
	if _, ran := rig.RunAuthorization(ctx); !ran {
		t.Fatal("expected ServiceRegistry-backed runner to run authorize")
	}
}
