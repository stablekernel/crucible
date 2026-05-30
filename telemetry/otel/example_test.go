package otel_test

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/stablekernel/crucible/telemetry"
	oteladapter "github.com/stablekernel/crucible/telemetry/otel"
)

// Example wires the OpenTelemetry adapter into a consuming module's telemetry
// Provider, so spans flow to a configured OpenTelemetry TracerProvider. Here a
// test SpanRecorder stands in for a real exporter so the output is deterministic.
func Example() {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	tel := telemetry.Nop().Apply(
		telemetry.WithTracer(oteladapter.NewTracer(tp.Tracer("crucible"))),
	)

	_, span := tel.Tracer.Start(context.Background(), "sink.Sink",
		telemetry.String("outlet", "dynamo"))
	span.SetStatus(telemetry.StatusOK, "")
	span.End()

	s := rec.Ended()[0]
	fmt.Println("span:", s.Name())
	fmt.Println("status:", s.Status().Code)

	// Output:
	// span: sink.Sink
	// status: Ok
}
