package telemetry_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/telemetry"
)

// Example shows how an IO module instruments an operation against the
// vendor-neutral interface. With no telemetry provided, the no-op default makes
// every call silent and allocation-free.
func Example() {
	// A consuming module holds a Provider, defaulted to the silent no-op pair.
	// A real consumer would pass telemetry.WithTracer/WithMeter to swap in their
	// backend (for example the slog adapter); here we keep the default.
	tel := telemetry.Nop().Apply()

	ctx := context.Background()

	// Trace the operation. The returned context is propagated to nested work so
	// downstream spans parent under this one.
	ctx, span := tel.Tracer.Start(ctx, "sink.Sink",
		telemetry.String("payload.type", "Order"),
	)
	defer span.End()

	// Metric instruments mirror what the sink module emits. Attributes are built
	// with the typed constructors — scalars are zero-allocation.
	sunk := tel.Meter.Counter("sink.sunk", telemetry.WithDescription("records sunk"))
	latency := tel.Meter.Histogram("sink.flush_latency_ms", telemetry.WithUnit("ms"))

	sunk.Add(ctx, 1, telemetry.String("outlet", "dynamo"))
	latency.Record(ctx, 3.2)

	span.SetStatus(telemetry.StatusOK, "")

	fmt.Println("emitted")
	// Output: emitted
}
