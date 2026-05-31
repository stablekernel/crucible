package fooddelivery_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// fd aliases the example package for terse test references.
type (
	Stage  = fooddelivery.Stage
	Signal = fooddelivery.Signal
	Order  = fooddelivery.Order
)

// TestModel_AnalyzeClean asserts the order machine has no structural defects: no
// dead states, no unreachable states, and no nondeterministic transition sets. The
// flagship example must analyze clean — it is the proof the recommended patterns
// compose into a sound machine.
func TestModel_AnalyzeClean(t *testing.T) {
	m, err := fooddelivery.NewModel()
	if err != nil {
		t.Fatalf("NewModel: %v", err)
	}
	rep := analysis.Analyze(m)
	if !rep.Empty() {
		t.Fatalf("expected no analysis findings, got:\n%s", rep.String())
	}
}

// TestScenario_HappyPath drives an order from placement to delivery: authorize the
// payment, cook (kitchen actor), dispatch and deliver (courier actor), and capture
// the held funds, landing in the Delivered terminal. It asserts the configuration
// and the folded log at each milestone, proving value-context Assign reducers,
// invoked services, parallel regions, and actor message passing compose end to end.
func TestScenario_HappyPath(t *testing.T) {
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(
		fooddelivery.WithOrder(Order{Subtotal: 5500, Tip: 500, Priority: "fast"}),
	)
	if err != nil {
		t.Fatalf("NewRig: %v", err)
	}

	assertConfig(t, rig, "initial", fooddelivery.Placed)

	// Submit arms the authorization service; the order waits in Authorizing.
	rig.Submit(ctx)
	assertConfig(t, rig, "after submit", fooddelivery.Authorizing)

	// The authorization succeeds: onDone routes Authorized, whose guard expression
	// (the Rich generous-order CEL guard OR a big basket on the fast lane) admits the
	// order into the parallel Active configuration.
	rig.SettleAuthorization(ctx, true)
	assertConfig(t, rig, "after authorize", fooddelivery.Cooking, fooddelivery.OnTime)
	assertLogHas(t, rig, "authorized:tok-001")

	// The kitchen actor prepares the meal; its plated output re-fires PlatedUp.
	if !rig.RunKitchen(ctx) {
		t.Fatal("expected a kitchen actor to run")
	}
	assertConfig(t, rig, "after cook", fooddelivery.AwaitingCourier, fooddelivery.OnTime)
	assertLogHas(t, rig, "kitchen:prepared-meal")

	// Dispatch the courier, then deliver: the courier actor's drop confirmation is a
	// cross-cutting completion that exits the parallel state to Settling.
	rig.PickUp(ctx)
	assertConfig(t, rig, "after pickup", fooddelivery.EnRoute, fooddelivery.OnTime)
	if !rig.RunCourier(ctx) {
		t.Fatal("expected a courier actor to run")
	}
	assertLogHas(t, rig, "courier:drop-confirmed")

	// Settling captures the payment and runs to the Delivered terminal.
	assertConfig(t, rig, "after delivery", fooddelivery.Delivered)
	assertLogHas(t, rig, "captured")
	if !rig.InFinal() {
		t.Fatal("Delivered is final; order should report InFinal")
	}
	if rig.Order().Breached {
		t.Fatal("happy path should not breach the SLA")
	}
}

// TestScenario_RefundSaga drives a post-authorization cancellation through the
// compensation saga: after the payment hold is captured, the order is canceled, the
// refund service reverses the hold, and the reversed amount reaches the onDone
// reducer through the event, landing in the Canceled terminal. It proves the saga's
// compensating service and the onDone-via-event result flow.
func TestScenario_RefundSaga(t *testing.T) {
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(
		fooddelivery.WithOrder(Order{Subtotal: 5000, Tip: 1500, Priority: "fast"}),
	)
	if err != nil {
		t.Fatalf("NewRig: %v", err)
	}

	rig.Submit(ctx)
	rig.SettleAuthorization(ctx, true)
	rig.RunKitchen(ctx) // mid-fulfillment: meal plated, courier not yet dispatched
	assertConfig(t, rig, "mid-order", fooddelivery.AwaitingCourier, fooddelivery.OnTime)

	// Cancel opens the saga from the active configuration, arming the refund service.
	rig.Cancel(ctx)
	assertConfig(t, rig, "after cancel", fooddelivery.Refunding)
	if rig.PendingTimers() != 0 {
		t.Fatalf("canceling should cancel the SLA timer; pending=%d", rig.PendingTimers())
	}

	// The refund reverses the held amount (subtotal+tip); its result folds into the
	// context via the onDone event, then the order reaches the Canceled terminal.
	rig.SettleRefund(ctx, 6500)
	assertConfig(t, rig, "after refund", fooddelivery.Canceled)
	if got := rig.Order().Refund; got != 6500 {
		t.Fatalf("refund amount = %d, want 6500", got)
	}
	assertLogHas(t, rig, "refunded:6500")
	if !rig.InFinal() {
		t.Fatal("Canceled is final; order should report InFinal")
	}
}

// TestScenario_SLABreach drives the Watchdog region's `after` timeout: with the
// kitchen still cooking, the fake clock advances past the SLA window, firing
// SLABreached, which marks the order breached while the Fulfillment spine continues.
// The order still delivers; the breach is recorded for reporting. It proves the
// deterministic `after`/Scheduler timeout running orthogonally to the spine.
func TestScenario_SLABreach(t *testing.T) {
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(
		fooddelivery.WithOrder(Order{Subtotal: 7000, Tip: 0, Priority: "standard"}),
	)
	if err != nil {
		t.Fatalf("NewRig: %v", err)
	}

	rig.Submit(ctx)
	rig.SettleAuthorization(ctx, true)
	assertConfig(t, rig, "active", fooddelivery.Cooking, fooddelivery.OnTime)
	if rig.PendingTimers() != 1 {
		t.Fatalf("active order should arm one SLA timer; pending=%d", rig.PendingTimers())
	}

	// Advance the clock past the SLA window: the Watchdog region fires SLABreached,
	// landing in Overdue while Fulfillment stays in Cooking.
	ticks := rig.BreachSLA(ctx)
	if len(ticks) != 1 {
		t.Fatalf("SLA breach should fire one timer; got %d", len(ticks))
	}
	assertConfig(t, rig, "after breach", fooddelivery.Cooking, fooddelivery.Overdue)
	if !rig.Order().Breached {
		t.Fatal("SLA breach should mark the order breached")
	}
	assertLogHas(t, rig, "sla-breached")

	// The order still completes: cook, dispatch, deliver.
	rig.RunKitchen(ctx)
	rig.PickUp(ctx)
	rig.RunCourier(ctx)
	assertConfig(t, rig, "delivered late", fooddelivery.Delivered)
	if !rig.Order().Breached {
		t.Fatal("a late delivery should remain flagged breached")
	}
}

// TestScenario_DeclinedAuthorization drives the authorization-decline edge: an empty
// basket is declined, routing the order to the Rejected terminal with no fulfillment.
func TestScenario_DeclinedAuthorization(t *testing.T) {
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(fooddelivery.WithOrder(Order{Subtotal: 9000, Tip: 0, Priority: "fast"}))
	if err != nil {
		t.Fatalf("NewRig: %v", err)
	}

	rig.Submit(ctx)
	rig.SettleAuthorization(ctx, false)
	assertConfig(t, rig, "after decline", fooddelivery.Rejected)
	assertLogHas(t, rig, "declined")
	if !rig.InFinal() {
		t.Fatal("Rejected is final")
	}
}

// TestScenario_SnapshotRestoreMidOrder snapshots the order mid-fulfillment (in a live
// parallel configuration with a pending SLA timer), round-trips it through JSON, and
// restores it into a fresh Rig — proving the value context, the active configuration,
// and the pending timer all survive a simulated process restart, and the restored
// order resumes identically.
func TestScenario_SnapshotRestoreMidOrder(t *testing.T) {
	ctx := context.Background()
	rig, err := fooddelivery.NewRig(
		fooddelivery.WithOrder(Order{Subtotal: 5500, Tip: 500, Priority: "fast"}),
	)
	if err != nil {
		t.Fatalf("NewRig: %v", err)
	}

	rig.Submit(ctx)
	rig.SettleAuthorization(ctx, true)
	rig.RunKitchen(ctx)
	assertConfig(t, rig, "mid-order", fooddelivery.AwaitingCourier, fooddelivery.OnTime)

	// Persist the live runtime state, exactly as a host saving an order between steps
	// would: capture, marshal, unmarshal.
	data, err := json.Marshal(rig.Snapshot())
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	var snap state.Snapshot[Stage, Signal, Order]
	if err = json.Unmarshal(data, &snap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}

	// Restore into a fresh Rig (a recovering process). The restored configuration and
	// value context match the snapshot.
	restored, err := fooddelivery.RestoreRig(snap)
	if err != nil {
		t.Fatalf("RestoreRig: %v", err)
	}
	assertConfig(t, restored, "restored", fooddelivery.AwaitingCourier, fooddelivery.OnTime)
	if got := restored.Order().AuthHold; got != "tok-001" {
		t.Fatalf("restored auth hold = %q, want tok-001", got)
	}

	// The restored order resumes and completes: dispatch and deliver carry it to the
	// Delivered terminal, with the pre-restart fulfillment log intact.
	restored.PickUp(ctx)
	if !restored.RunCourier(ctx) {
		t.Fatal("restored order should run its courier")
	}
	assertConfig(t, restored, "restored delivered", fooddelivery.Delivered)
	assertLogHas(t, restored, "kitchen:prepared-meal")
	assertLogHas(t, restored, "courier:drop-confirmed")
}

// assertConfig fails the test unless the rig's active configuration equals want
// exactly (order-sensitive, as the engine reports it).
func assertConfig(t *testing.T, rig *fooddelivery.Rig, at string, want ...Stage) {
	t.Helper()
	got := rig.Configuration()
	if len(got) != len(want) {
		t.Fatalf("%s: configuration = %v, want %v", at, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: configuration = %v, want %v", at, got, want)
		}
	}
}

// assertLogHas fails the test unless the order's folded log contains entry.
func assertLogHas(t *testing.T, rig *fooddelivery.Rig, entry string) {
	t.Helper()
	for _, e := range rig.Order().Log {
		if e == entry {
			return
		}
	}
	t.Fatalf("order log %v missing %q", rig.Order().Log, entry)
}
