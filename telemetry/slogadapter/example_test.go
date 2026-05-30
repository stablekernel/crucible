package slogadapter_test

import (
	"context"
	"log/slog"
	"os"

	"github.com/stablekernel/crucible/telemetry"
	"github.com/stablekernel/crucible/telemetry/slogadapter"
)

// Example wires the slog adapter into a consuming module's telemetry Provider,
// so spans and metrics are emitted as structured logs with no external
// dependency.
func Example() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		// Drop time and the elapsed duration so the example output is stable.
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.Attr{}
			}
			if a.Key == "elapsed" {
				return slog.Attr{}
			}
			return a
		},
	}))

	tel := telemetry.Nop().Apply(
		telemetry.WithTracer(slogadapter.NewTracer(slogadapter.WithLogger(logger))),
		telemetry.WithMeter(slogadapter.NewMeter(slogadapter.WithLogger(logger))),
	)

	ctx, span := tel.Tracer.Start(context.Background(), "sink.Sink")
	tel.Meter.Counter("sink.sunk").Add(ctx, 1)
	span.End()

	// Output:
	// level=DEBUG msg=span.start span.name=sink.Sink span.id=1
	// level=DEBUG msg=metric metric.name=sink.sunk metric.kind=counter metric.value=1
	// level=DEBUG msg=span.end span.name=sink.Sink span.id=1 span.status=unset
}
