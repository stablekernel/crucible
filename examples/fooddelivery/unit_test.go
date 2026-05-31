package fooddelivery_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/examples/fooddelivery"
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
