package fooddelivery_test

import (
	"testing"

	"github.com/stablekernel/crucible/examples/fooddelivery"
)

// TestKitchenBehavior_SpawnsUsableActor asserts the exported kitchen behavior is
// non-nil and, when invoked with an empty input map (as a host or the durable
// runner's actor palette would invoke it on spawn), yields a usable, non-nil
// ActorInstance — the proof that a consumer can register and drive the fulfillment
// actor directly without going through the example's Rig.
func TestKitchenBehavior_SpawnsUsableActor(t *testing.T) {
	behavior := fooddelivery.KitchenBehavior()
	if behavior == nil {
		t.Fatal("KitchenBehavior returned a nil behavior")
	}
	inst, err := behavior(nil)
	if err != nil {
		t.Fatalf("invoking kitchen behavior: %v", err)
	}
	if inst == nil {
		t.Fatal("kitchen behavior yielded a nil ActorInstance")
	}
}

// TestCourierBehavior_SpawnsUsableActor mirrors the kitchen assertion for the
// exported courier behavior: non-nil, and invoking it with an empty map yields a
// usable ActorInstance.
func TestCourierBehavior_SpawnsUsableActor(t *testing.T) {
	behavior := fooddelivery.CourierBehavior()
	if behavior == nil {
		t.Fatal("CourierBehavior returned a nil behavior")
	}
	inst, err := behavior(map[string]any{})
	if err != nil {
		t.Fatalf("invoking courier behavior: %v", err)
	}
	if inst == nil {
		t.Fatal("courier behavior yielded a nil ActorInstance")
	}
}

// TestActorDrivingSignals_Exported references the exported actor-driving messages,
// proving they are usable as opaque any values a host delivers to a running actor.
// They carry an unexported type by design; a consumer never names that type, only
// passes the value through the actor delivery seam.
func TestActorDrivingSignals_Exported(t *testing.T) {
	signals := []any{fooddelivery.KitchenCook, fooddelivery.CourierDrive}
	for _, sig := range signals {
		if sig == nil {
			t.Fatal("an exported actor-driving signal was nil")
		}
	}
}
