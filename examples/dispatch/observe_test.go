package dispatch

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/telemetry"
	crucibleslog "github.com/stablekernel/crucible/telemetry/slog"
)

// captureProvider builds a telemetry.Provider whose tracer and meter emit
// structured text records into the returned buffer, with deterministic span ids
// and a fixed clock so the captured output is stable across runs. It is the
// observation sink the telemetry tests assert against.
func captureProvider() (*bytes.Buffer, telemetry.Provider) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		// Drop the wall-clock time and the span elapsed so the captured records are
		// deterministic; the from/to tags and operation names are what the tests assert.
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

	var ids atomic.Uint64
	base := time.Unix(0, 0).UTC()
	opts := []crucibleslog.Option{
		crucibleslog.WithLogger(logger),
		crucibleslog.WithClock(func() time.Time { return base }),
		crucibleslog.WithIDFn(func() uint64 { return ids.Add(1) }),
	}

	tel := telemetry.Nop().Apply(
		telemetry.WithTracer(crucibleslog.NewTracer(opts...)),
		telemetry.WithMeter(crucibleslog.NewMeter(opts...)),
	)
	return &buf, tel
}

// TestRunObservedSaga_EmitsTransitionTelemetry runs the observed saga against an
// slog-backed provider writing to a buffer and asserts the run both returns the
// expected facts and emits a span and a metric per transition — tagged with the
// from/to stages — proving the host instrumentation drives the telemetry seam end
// to end.
func TestRunObservedSaga_EmitsTransitionTelemetry(t *testing.T) {
	buf, tel := captureProvider()

	report, err := RunObservedSaga(context.Background(), tel)
	if err != nil {
		t.Fatalf("RunObservedSaga: %v", err)
	}

	// The drive path advances the order through three observable transitions to the
	// Delivered terminal.
	if report.Transitions != 3 {
		t.Fatalf("observed transitions = %d, want 3", report.Transitions)
	}
	if report.FinalStage != fooddelivery.Delivered {
		t.Fatalf("final stage = %v, want Delivered", report.FinalStage)
	}
	if got := len(report.Stages); got != report.Transitions {
		t.Fatalf("recorded %d stages for %d transitions, want equal", got, report.Transitions)
	}
	if last := report.Stages[len(report.Stages)-1]; last != fooddelivery.Delivered {
		t.Fatalf("last recorded stage = %v, want Delivered", last)
	}

	out := buf.String()

	// One span.start and one span.end were emitted per transition, named
	// "order.transition".
	if got := strings.Count(out, "msg=span.start"); got != report.Transitions {
		t.Fatalf("span.start count = %d, want %d", got, report.Transitions)
	}
	if got := strings.Count(out, "msg=span.end"); got != report.Transitions {
		t.Fatalf("span.end count = %d, want %d", got, report.Transitions)
	}
	if !strings.Contains(out, "span.name="+transitionSpanName) {
		t.Fatalf("buffer missing %q span name; got:\n%s", transitionSpanName, out)
	}

	// One metric record was emitted per transition, named "order.transitions".
	if got := strings.Count(out, "msg=metric"); got != report.Transitions {
		t.Fatalf("metric count = %d, want %d", got, report.Transitions)
	}
	if !strings.Contains(out, "metric.name="+transitionMetricName) {
		t.Fatalf("buffer missing %q metric name; got:\n%s", transitionMetricName, out)
	}

	// The telemetry is tagged with the from/to stages, so the trace narrates the
	// order's path. The final transition lands on Delivered.
	if !strings.Contains(out, "span.attrs.to="+fooddelivery.Delivered.String()) {
		t.Fatalf("buffer missing the Delivered transition tag; got:\n%s", out)
	}
}

// TestRunObservedSaga_NopProviderIsSilent confirms the default no-op provider runs
// the saga to completion without panicking and without emitting telemetry — the
// silent, allocation-free default a caller gets when it passes telemetry.Nop().
func TestRunObservedSaga_NopProviderIsSilent(t *testing.T) {
	report, err := RunObservedSaga(context.Background(), telemetry.Nop())
	if err != nil {
		t.Fatalf("RunObservedSaga with Nop provider: %v", err)
	}
	if report.FinalStage != fooddelivery.Delivered {
		t.Fatalf("final stage = %v, want Delivered", report.FinalStage)
	}
	if report.Transitions != 3 {
		t.Fatalf("observed transitions = %d, want 3", report.Transitions)
	}
}
