package fooddelivery_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/stablekernel/crucible/examples/fooddelivery"
)

// Example demonstrates the order lifecycle's happy path end to end: a placed order
// is authorized, cooked by the kitchen actor, delivered by the courier actor, and
// captured — driven through the example's host Rig (Scheduler + ServiceRunner +
// ActorSystem). The printed log is the value context's folded record, written only
// by Assign reducers.
func Example() {
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(
		fooddelivery.WithOrder(fooddelivery.Order{Subtotal: 5500, Tip: 500, Priority: "fast"}),
	)
	if err != nil {
		fmt.Println("build error:", err)
		return
	}

	rig.Submit(ctx)
	rig.SettleAuthorization(ctx, true) // payment hold succeeds
	rig.RunKitchen(ctx)                // kitchen actor plates the meal
	rig.PickUp(ctx)                    // courier dispatched
	rig.RunCourier(ctx)                // courier delivers; order settles

	fmt.Println("final stage:", rig.Configuration()[0])
	fmt.Println("log:", strings.Join(rig.Order().Log, " -> "))
	// Output:
	// final stage: Delivered
	// log: authorized:tok-001 -> kitchen:prepared-meal -> courier:drop-confirmed -> captured
}

// Example_refundSaga demonstrates the cancellation saga: after the payment hold, the
// order is canceled and the refund service reverses the hold. The reversed amount
// reaches the onDone reducer through the event — the recommended onDone-via-event
// result flow, with no host side channel.
func Example_refundSaga() {
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(
		fooddelivery.WithOrder(fooddelivery.Order{Subtotal: 5000, Tip: 1500, Priority: "fast"}),
	)
	if err != nil {
		fmt.Println("build error:", err)
		return
	}

	rig.Submit(ctx)
	rig.SettleAuthorization(ctx, true)
	rig.RunKitchen(ctx)
	rig.Cancel(ctx)             // open the compensation saga
	rig.SettleRefund(ctx, 6500) // refund reverses the held amount

	fmt.Println("final stage:", rig.Configuration()[0])
	fmt.Println("refunded:", rig.Order().Refund)
	// Output:
	// final stage: Canceled
	// refunded: 6500
}

// Example_slaBreach demonstrates the deterministic SLA timeout: with the kitchen
// still cooking, the fake clock advances past the delivery window, firing the
// Watchdog region's `after` timeout. The order is flagged breached but still
// delivers — the monitoring region runs orthogonally to the fulfillment spine.
func Example_slaBreach() {
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(
		fooddelivery.WithOrder(fooddelivery.Order{Subtotal: 7000, Priority: "standard"}),
	)
	if err != nil {
		fmt.Println("build error:", err)
		return
	}

	rig.Submit(ctx)
	rig.SettleAuthorization(ctx, true)
	rig.BreachSLA(ctx) // clock advances past the SLA window

	fmt.Println("breached:", rig.Order().Breached)

	rig.RunKitchen(ctx)
	rig.PickUp(ctx)
	rig.RunCourier(ctx)
	fmt.Println("final stage:", rig.Configuration()[0])
	fmt.Println("still breached:", rig.Order().Breached)
	// Output:
	// breached: true
	// final stage: Delivered
	// still breached: true
}
