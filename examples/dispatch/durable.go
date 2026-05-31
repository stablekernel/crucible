package dispatch

import (
	"context"
	"fmt"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
)

// This file is the showcase's durable-execution capability: it runs the proven
// food-delivery order saga under the Crucible durable runtime, so an order survives
// a process crash and its lifecycle can be replayed read-only after the fact. It
// reuses the saga wholesale — the model from [fooddelivery.NewModel], the payment
// services from [fooddelivery.ServiceRegistry], and the kitchen/courier actor
// behaviors from [fooddelivery.KitchenBehavior] / [fooddelivery.CourierBehavior] —
// driving them through the durable [durable.Handle] API rather than the example's
// in-process Rig.
//
// Two durability properties are demonstrated:
//
//   - Crash and recovery, against a real on-disk [durable.FileStore]: an order is
//     driven to its live Active configuration, the runner and handle are dropped to
//     simulate a process crash, and the order is reconstructed from the store alone
//     with [durable.Recover] — its state, payment hold, and folded log intact — then
//     driven on to Delivered.
//   - Read-only time travel, against a history-retaining [durable.MemStore]: the same
//     happy path is recorded, then [durable.Steps] and [durable.StateAt] reconstruct
//     the order's state as of an earlier point in its lifecycle without re-running any
//     service or actor.

// fixedClockStart is the deterministic instant the durable runner's fake clock
// starts at, so the order's SLA `after` timer arms against a known time and never
// fires within these demonstrations (the clock is never advanced past the window).
var fixedClockStart = time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

// sampleOrder is the order both durable demonstrations drive: a generous, fast-lane
// basket that the authorization guard admits into the Active fulfillment superstate.
func sampleOrder() fooddelivery.Order {
	return fooddelivery.Order{Subtotal: 5500, Tip: 600, Priority: "fast"}
}

// startActiveOrder forges the order model, starts a durable instance on store under
// id, and drives it to its live Active fulfillment configuration: it fires Submit and
// runs the real authorize service (which produces the hold token and routes
// Authorized, whose guard admits the generous, fast-lane basket into Active). It
// returns the forged model (the caller needs it for Recover / StateAt) and the live
// Handle. Every step is recorded write-ahead to store. It is the shared live-run
// prelude both durable demonstrations build on.
func startActiveOrder(
	ctx context.Context,
	store durable.Store,
	id durable.InstanceID,
	opts []durable.Option[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order],
) (*state.Machine[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], *durable.Handle[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order], error) {
	model, err := fooddelivery.NewModel()
	if err != nil {
		return nil, nil, fmt.Errorf("dispatch: build model: %w", err)
	}

	runner := durable.NewRunner(model, store, opts...)
	h, err := runner.Start(ctx, id, sampleOrder(), state.WithInitialState(fooddelivery.Placed))
	if err != nil {
		return nil, nil, fmt.Errorf("dispatch: start order: %w", err)
	}
	if _, err = h.Fire(ctx, fooddelivery.Submit); err != nil {
		return nil, nil, fmt.Errorf("dispatch: submit order: %w", err)
	}
	if _, ran, rerr := h.RunService(ctx, state.InvokeID("order", fooddelivery.Authorizing, 0)); rerr != nil {
		return nil, nil, fmt.Errorf("dispatch: run authorize service: %w", rerr)
	} else if !ran {
		return nil, nil, fmt.Errorf("dispatch: no authorize service was in flight to run")
	}
	return model, h, nil
}

// CrashRecoveryResult is the observable outcome of [RunCrashRecovery]: the
// configuration captured immediately after recovery (proving the live state survived
// the crash), the payment hold and folded log recovered with it, and the final
// configuration the recovered order reached after being driven to completion.
type CrashRecoveryResult struct {
	// RecoveredConfig is the active configuration the order reported immediately after
	// [durable.Recover] reconstructed it from the store — the parallel Active
	// configuration (Cooking + OnTime) the live run had reached before the crash.
	RecoveredConfig []fooddelivery.Stage
	// RecoveredAuthHold is the payment-authorization token the live authorize service
	// produced, recovered from the store without re-invoking the service.
	RecoveredAuthHold string
	// RecoveredLog is the milestone log folded before the crash, recovered intact.
	RecoveredLog []string
	// FinalConfig is the configuration the recovered order reached after being driven
	// on to completion — the Delivered terminal.
	FinalConfig []fooddelivery.Stage
	// FinalLog is the full milestone log after the recovered order ran to Delivered.
	FinalLog []string
}

// RunCrashRecovery drives the order saga to its live Active configuration under a
// durable runner backed by the on-disk FileStore rooted at dir, simulates a process
// crash by dropping the runner and handle, reconstructs the order from the store
// alone, and drives the recovered order on to Delivered — returning the observable
// evidence that the live state, payment hold, and log survived the crash.
//
// The crash boundary is real: nothing from the first runner is reused after the
// drop; the recovered handle is built purely from what the FileStore persisted.
func RunCrashRecovery(ctx context.Context, dir string) (CrashRecoveryResult, error) {
	store, err := durable.NewFileStore(dir)
	if err != nil {
		return CrashRecoveryResult{}, fmt.Errorf("dispatch: open file store: %w", err)
	}
	return crashRecovery(ctx, store, durable.InstanceID("order-1"))
}

// crashRecovery is the store-agnostic core of [RunCrashRecovery]: it drives the order
// to Active under store, simulates the crash, recovers, and drives on to Delivered.
// Taking the Store as a parameter keeps the public entry point's storage choice
// (on-disk FileStore) separate from the durable logic, and lets tests inject a store
// to exercise the recovery and drive seams.
func crashRecovery(ctx context.Context, store durable.Store, id durable.InstanceID) (CrashRecoveryResult, error) {
	opts := durableOptions(state.NewFakeClock(fixedClockStart))

	// Live run: start the order, authorize the payment, and reach the Active
	// fulfillment superstate. Every step is recorded write-ahead to the store. The
	// returned live Handle is intentionally discarded here — CRASH — so everything below
	// is reconstructed from the store alone, with nothing from the live run reused.
	model, _, err := startActiveOrder(ctx, store, id, opts)
	if err != nil {
		return CrashRecoveryResult{}, err
	}

	// Recover the order purely from the store: Load the checkpoint, Restore it,
	// replay the recorded tail (re-settling the recorded authorize result without
	// re-invoking the service).
	recovered, err := durable.Recover(ctx, model, store, id, opts...)
	if err != nil {
		return CrashRecoveryResult{}, fmt.Errorf("dispatch: recover order: %w", err)
	}

	recSnap := recovered.Instance().Snapshot()
	result := CrashRecoveryResult{
		RecoveredConfig:   recovered.Instance().Configuration(),
		RecoveredAuthHold: recSnap.Context.AuthHold,
		RecoveredLog:      append([]string(nil), recSnap.Context.Log...),
	}

	// Drive the recovered order on to Delivered, step by step, through the durable
	// Handle's actor and event seams.
	if err = driveToDelivered(ctx, recovered); err != nil {
		return CrashRecoveryResult{}, err
	}

	finalSnap := recovered.Instance().Snapshot()
	result.FinalConfig = recovered.Instance().Configuration()
	result.FinalLog = append([]string(nil), finalSnap.Context.Log...)
	return result, nil
}

// driveToDelivered advances a recovered order from its Active fulfillment
// configuration to the Delivered terminal through the durable Handle seams: complete
// the kitchen actor (re-fires PlatedUp → AwaitingCourier), dispatch the courier
// (PickedUp → EnRoute), then complete the courier actor (re-fires DroppedOff, which
// the Active compound handles cross-cuttingly, exiting to Settling and then — via the
// always-transition — to Delivered).
func driveToDelivered(ctx context.Context, h *durable.Handle[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) error {
	// Complete the kitchen actor spawned on entering Cooking: its plated output
	// re-fires PlatedUp, advancing Fulfillment to AwaitingCourier.
	if err := completeActor(ctx, h, "kitchen", fooddelivery.Cooking, fooddelivery.KitchenCook); err != nil {
		return err
	}

	// Dispatch the courier: PickedUp advances Fulfillment to EnRoute, spawning the
	// courier actor.
	if _, err := h.Fire(ctx, fooddelivery.PickedUp); err != nil {
		return fmt.Errorf("dispatch: fire PickedUp: %w", err)
	}

	// Complete the courier actor spawned on entering EnRoute: its drop confirmation
	// re-fires DroppedOff, which the Active compound handles cross-cuttingly, exiting
	// the parallel state to Settling and then — via the always-transition — Delivered.
	return completeActor(ctx, h, "courier", fooddelivery.EnRoute, fooddelivery.CourierDrive)
}

// completeActor drives the fulfillment actor named src — spawned on entering from —
// to completion by delivering message to it, so its terminal output re-fires the
// parent transition that advances the saga. It addresses the actor by the id the
// kernel stamps on its spawn ([state.ActorID] over the order machine, the spawning
// stage, and index 0). It reports a clear error when no such actor is running or the
// delivery cannot be recorded, so a mis-wired drive fails loudly rather than silently
// stalling.
func completeActor(
	ctx context.Context,
	h *durable.Handle[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order],
	src string,
	from fooddelivery.Stage,
	message any,
) error {
	ref, ok := h.ActorRef(state.ActorID("order", from, 0))
	if !ok {
		return fmt.Errorf("dispatch: no %s actor running to drive", src)
	}
	delivered, err := h.DeliverToActor(ctx, ref, message)
	if err != nil {
		return fmt.Errorf("dispatch: deliver to %s actor: %w", src, err)
	}
	if !delivered {
		return fmt.Errorf("dispatch: %s actor was not running", src)
	}
	return nil
}

// LifecycleStep is one entry in a time-travel timeline: a recorded step ordinal and
// the order's reconstructed state and folded-log length as of that step.
type LifecycleStep struct {
	// Step is the recorded step ordinal the state was reconstructed at.
	Step int
	// Config is the active configuration the order held as of the step.
	Config []fooddelivery.Stage
	// LogLen is the number of milestones folded into the order's log by the step, a
	// compact stand-in for the log's growth across the lifecycle.
	LogLen int
}

// TimeTravelResult is the observable outcome of [RunTimeTravel]: the order's full
// recorded timeline (one entry per step), and the configuration the order held at an
// earlier step — proving a historical reconstruction differs from the final state.
type TimeTravelResult struct {
	// Timeline lists the order's reconstructed state at each recorded step, in order.
	Timeline []LifecycleStep
	// EarlierStep is the step the earlier reconstruction was taken at.
	EarlierStep int
	// EarlierConfig is the configuration the order held at EarlierStep.
	EarlierConfig []fooddelivery.Stage
	// FinalConfig is the configuration the order held at its last recorded step.
	FinalConfig []fooddelivery.Stage
}

// RunTimeTravel records the order saga's happy path through a history-retaining
// MemStore, then reconstructs the order's state read-only at each recorded step and
// at one earlier point in its lifecycle — proving the earlier reconstruction differs
// from the final, delivered state without re-running any service or actor.
func RunTimeTravel(ctx context.Context) (TimeTravelResult, error) {
	store := durable.NewMemStore(durable.WithHistory())
	return timeTravel(ctx, store, durable.InstanceID("order-2"))
}

// timeTravel is the store-agnostic core of [RunTimeTravel]: it records the happy path
// under store, then reconstructs the order's state across its recorded steps and at
// an earlier point. Taking the Store as a parameter keeps the public entry point's
// storage choice (a history-retaining MemStore) separate from the durable logic, and
// lets tests inject a store to exercise the enumeration and reconstruction seams.
func timeTravel(ctx context.Context, store durable.Store, id durable.InstanceID) (TimeTravelResult, error) {
	opts := durableOptions(state.NewFakeClock(fixedClockStart))

	// Record the full happy path: start, authorize into Active, then drive on to
	// Delivered. The history-retaining store keeps every Record so any step is
	// reachable for reconstruction.
	model, h, err := startActiveOrder(ctx, store, id, opts)
	if err != nil {
		return TimeTravelResult{}, err
	}
	if err = driveToDelivered(ctx, h); err != nil {
		return TimeTravelResult{}, err
	}

	steps, err := durable.Steps(ctx, store, id)
	if err != nil {
		return TimeTravelResult{}, fmt.Errorf("dispatch: enumerate steps: %w", err)
	}
	if len(steps) == 0 {
		return TimeTravelResult{}, fmt.Errorf("dispatch: order recorded no steps")
	}

	result := TimeTravelResult{Timeline: make([]LifecycleStep, 0, len(steps))}
	for _, step := range steps {
		view, verr := durable.StateAt(ctx, model, store, id, step, opts...)
		if verr != nil {
			return TimeTravelResult{}, fmt.Errorf("dispatch: reconstruct step %d: %w", step, verr)
		}
		result.Timeline = append(result.Timeline, LifecycleStep{
			Step:   step,
			Config: view.Instance().Configuration(),
			LogLen: len(view.Snapshot().Context.Log),
		})
	}

	// The earlier reconstruction targets the first recorded step (the Submit that left
	// the order Authorizing) — well before the Delivered terminal the final step holds.
	result.EarlierStep = steps[0]
	earlier, err := durable.StateAt(ctx, model, store, id, result.EarlierStep, opts...)
	if err != nil {
		return TimeTravelResult{}, fmt.Errorf("dispatch: reconstruct earlier step: %w", err)
	}
	result.EarlierConfig = earlier.Instance().Configuration()

	final, err := durable.StateAt(ctx, model, store, id, steps[len(steps)-1], opts...)
	if err != nil {
		return TimeTravelResult{}, fmt.Errorf("dispatch: reconstruct final step: %w", err)
	}
	result.FinalConfig = final.Instance().Configuration()
	return result, nil
}

// durableOptions builds the durable runner options that wire the reused saga into
// the runtime: the payment service registry, the kitchen/courier actor palette, and
// the supplied clock. They are shared by Start, Recover, and StateAt so the live run,
// its recovery, and any time-travel read resolve the same services and actors.
func durableOptions(clk state.Clock) []durable.Option[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order] {
	return []durable.Option[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]{
		durable.WithServiceRegistry[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order](fooddelivery.ServiceRegistry()),
		durable.WithActorPalette[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order](map[string]state.ActorBehavior{
			"kitchen": fooddelivery.KitchenBehavior(),
			"courier": fooddelivery.CourierBehavior(),
		}),
		durable.WithRunnerClock[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order](clk),
	}
}
