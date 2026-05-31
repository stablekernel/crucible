package dispatch_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/stablekernel/crucible/examples/dispatch"
	"github.com/stablekernel/crucible/telemetry"
	crucibleslog "github.com/stablekernel/crucible/telemetry/slog"
)

// ExampleRunObservedSaga drives the proven, durable order saga to Delivered while
// emitting a trace span and a metric per transition through an slog-backed telemetry
// Provider. It prints the observed facts from the returned report rather than the raw
// log lines, so the output is deterministic regardless of log formatting; the slog
// adapter is wired only to show how a host injects a real telemetry backend.
func ExampleRunObservedSaga() {
	ctx := context.Background()

	// A host wires its telemetry backend into a Provider. Here the slog adapter emits
	// spans and metrics as structured logs to stderr (kept off the example's stdout so
	// the deterministic report below is the only checked output); a production host
	// would wire an otel or datadog adapter against the same interfaces instead.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	tel := telemetry.Nop().Apply(
		telemetry.WithTracer(crucibleslog.NewTracer(crucibleslog.WithLogger(logger))),
		telemetry.WithMeter(crucibleslog.NewMeter(crucibleslog.WithLogger(logger))),
	)

	report, err := dispatch.RunObservedSaga(ctx, tel)
	if err != nil {
		panic(err)
	}

	fmt.Println("observed saga")
	fmt.Printf("  transitions observed: %d\n", report.Transitions)
	fmt.Printf("  path:                 %v\n", report.Stages)
	fmt.Printf("  final stage:          %v\n", report.FinalStage)

	// Output:
	// observed saga
	//   transitions observed: 3
	//   path:                 [AwaitingCourier EnRoute Delivered]
	//   final stage:          Delivered
}
