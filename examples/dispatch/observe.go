package dispatch

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/telemetry"
)

// This file is the showcase's observability capability: it runs the proven,
// durable order saga while emitting a trace span and a metric per transition
// through Crucible's vendor-neutral telemetry seam. There is no kernel hook into
// the state machine; instead the host wraps its own drive calls — the same Fire
// and actor-completion calls the durable capability makes — in spans and counter
// increments. Telemetry arrives as an injected [telemetry.Provider], so a caller
// supplies a real tracer/meter (an slog, otel, or datadog adapter) and a test
// supplies an slog adapter writing to a buffer, while the default — [telemetry.Nop]
// — leaves the run silent and allocation-free.
//
// Each drive step is observed identically: the host reads the order's headline
// stage before the step, performs the step against the durable [durable.Handle],
// reads the headline stage after, then opens a short "order.transition" span tagged
// with from/to stages and increments the "order.transitions" counter with the same
// tags. The span and counter therefore narrate the order's progress from Placed
// through the Active fulfillment configuration to Delivered.

// transitionSpanName is the operation name the observed saga opens a span under for
// each order transition, tagged with the from/to stages.
const transitionSpanName = "order.transition"

// transitionMetricName is the monotonic counter the observed saga increments once
// per order transition, tagged with the from/to stages.
const transitionMetricName = "order.transitions"

// ObservedReport is the observable outcome of [RunObservedSaga]: the facts a caller
// (or test) can assert without scraping telemetry output. It mirrors what the spans
// and metrics narrate — how many transitions were observed and the stage the order
// finished in — so the harness is verifiable from its return value alone, with the
// emitted telemetry as the human-facing trace of the same run.
type ObservedReport struct {
	// Transitions is the number of order transitions observed — one span and one
	// counter increment was emitted for each.
	Transitions int
	// FinalStage is the order's headline stage after the run, the Delivered terminal
	// on a clean run.
	FinalStage fooddelivery.Stage
	// Stages lists the headline stage the order entered at each observed transition,
	// in order, so a caller can assert the saga's path as well as its endpoint.
	Stages []fooddelivery.Stage
}

// driveStep is one named drive against the durable handle: the action the host
// performs to advance the order one transition. Naming each step keeps the
// observation loop uniform — every step is wrapped in a span and counter the same
// way — while letting each step carry its own durable-handle call.
type driveStep struct {
	// name labels the step for error context; it is not emitted as a span name (the
	// span name is the uniform transitionSpanName so traces aggregate cleanly).
	name string
	// fire performs the step against the handle, advancing the order one transition.
	fire func(context.Context, *durable.Handle[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) error
}

// RunObservedSaga drives the proven order saga to Delivered under the durable
// runtime — reusing the same model, payment services, and kitchen/courier actors the
// durable capability runs — while emitting one trace span and one counter increment
// per transition through tel. The span ("order.transition") and counter
// ("order.transitions") are each tagged with the from/to stage the order moved
// between, so tel narrates the order's progress from Placed to Delivered.
//
// tel is injected: pass an slog-, otel-, or datadog-backed [telemetry.Provider] to
// observe the run, or [telemetry.Nop] to run silently. RunObservedSaga returns an
// [ObservedReport] of the observed facts (transition count, final stage, path) so a
// caller can assert the run from its return value, with the emitted telemetry as the
// human-facing trace. Nothing is swallowed: any drive failure is returned wrapped.
func RunObservedSaga(ctx context.Context, tel telemetry.Provider) (ObservedReport, error) {
	store := durable.NewMemStore()
	return runObserved(ctx, tel, store, durable.InstanceID("order-observed"))
}

// runObserved is the store-agnostic core of [RunObservedSaga]: it starts the order
// to its Active fulfillment configuration, then drives it on to Delivered, observing
// every transition through tel. Taking the Store as a parameter keeps the public
// entry point's storage choice (an in-memory store) separate from the observation
// logic and lets a test inject its own store.
func runObserved(
	ctx context.Context,
	tel telemetry.Provider,
	store durable.Store,
	id durable.InstanceID,
) (ObservedReport, error) {
	opts := durableOptions(state.NewFakeClock(fixedClockStart))

	_, h, err := startActiveOrder(ctx, store, id, opts)
	if err != nil {
		return ObservedReport{}, err
	}

	report := ObservedReport{Stages: make([]fooddelivery.Stage, 0, len(deliveredDriveSteps()))}
	counter := tel.Meter.Counter(
		transitionMetricName,
		telemetry.WithUnit("{transition}"),
		telemetry.WithDescription("order saga transitions observed"),
	)

	for _, step := range deliveredDriveSteps() {
		from := headlineStage(h)
		if err = step.fire(ctx, h); err != nil {
			return ObservedReport{}, fmt.Errorf("dispatch: observe %s: %w", step.name, err)
		}
		to := headlineStage(h)

		observeTransition(ctx, tel, counter, from, to)
		report.Transitions++
		report.Stages = append(report.Stages, to)
		report.FinalStage = to
	}

	return report, nil
}

// observeTransition emits the telemetry for one transition: a short span named
// [transitionSpanName] tagged with the from/to stages, and one increment of counter
// carrying the same tags. The span is opened and immediately ended around the
// already-applied transition because the transition itself is synchronous and has no
// nested work to parent under it; the span exists to mark the transition in a trace,
// and the counter to make the order's progress measurable.
func observeTransition(
	ctx context.Context,
	tel telemetry.Provider,
	counter telemetry.Counter,
	from, to fooddelivery.Stage,
) {
	attrs := []telemetry.Attr{
		telemetry.String("from", from.String()),
		telemetry.String("to", to.String()),
	}
	_, span := tel.Tracer.Start(ctx, transitionSpanName, attrs...)
	span.SetStatus(telemetry.StatusOK, "")
	span.End()
	counter.Add(ctx, 1, attrs...)
}

// headlineStage reads the order's headline (leading) stage from the durable handle's
// live configuration: the configuration's first entry, the stage a reader thinks of
// the order as being "in". It is the from/to value the transition spans and counter
// are tagged with.
func headlineStage(h *durable.Handle[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) fooddelivery.Stage {
	config := h.Instance().Configuration()
	if len(config) == 0 {
		return fooddelivery.Placed
	}
	return config[0]
}

// deliveredDriveSteps is the ordered sequence of drive steps that advances an order
// from its Active fulfillment configuration to the Delivered terminal — the same
// kitchen-complete, dispatch-courier, courier-complete path [driveToDelivered] takes,
// decomposed into individually observable steps so each transition is spanned and
// counted on its own.
func deliveredDriveSteps() []driveStep {
	return []driveStep{
		{
			name: "kitchen-cook",
			fire: func(ctx context.Context, h *durable.Handle[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) error {
				return completeActor(ctx, h, "kitchen", fooddelivery.Cooking, fooddelivery.KitchenCook)
			},
		},
		{
			name: "courier-pickup",
			fire: func(ctx context.Context, h *durable.Handle[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) error {
				if _, err := h.Fire(ctx, fooddelivery.PickedUp); err != nil {
					return fmt.Errorf("dispatch: fire PickedUp: %w", err)
				}
				return nil
			},
		},
		{
			name: "courier-drive",
			fire: func(ctx context.Context, h *durable.Handle[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]) error {
				return completeActor(ctx, h, "courier", fooddelivery.EnRoute, fooddelivery.CourierDrive)
			},
		},
	}
}
