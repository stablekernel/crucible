package datadog_test

import (
	"context"
	"fmt"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"

	"github.com/stablekernel/crucible/telemetry"
	ddadapter "github.com/stablekernel/crucible/telemetry/datadog"
)

// Example wires the Datadog adapter into a consuming module's telemetry Provider,
// so spans flow to dd-trace-go. Here mocktracer stands in for the real tracer (in
// production, call tracer.Start in your bootstrap) so the output is deterministic.
func Example() {
	mt := mocktracer.Start()
	defer mt.Stop()

	tel := telemetry.Nop().Apply(
		telemetry.WithTracer(ddadapter.NewTracer()),
	)

	_, span := tel.Tracer.Start(context.Background(), "sink.Sink",
		telemetry.String("outlet", "dynamo"))
	span.End()

	s := mt.FinishedSpans()[0]
	fmt.Println("span:", s.OperationName())
	fmt.Println("outlet:", s.Tag("outlet"))

	// Output:
	// span: sink.Sink
	// outlet: dynamo
}
