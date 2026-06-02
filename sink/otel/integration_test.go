// SPDX-License-Identifier: Apache-2.0

//go:build integration

package otel_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	csink "github.com/stablekernel/crucible/sink"
	otelsink "github.com/stablekernel/crucible/sink/otel"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string
}

// TestIntegrationSinkRecordsAgainstRealSDKMeter drives the real Outlet path
// against a real OpenTelemetry SDK meter read through an in-memory ManualReader,
// then collects the metrics to prove the counter was recorded.
func TestIntegrationSinkRecordsAgainstRealSDKMeter(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	reg := otelsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[otelsink.Meter] {
		return otelsink.Counter("orders.placed", 2, attribute.String("id", o.ID))
	})

	outlet := otelsink.New(mp.Meter("integration"), reg)
	if err := outlet.Sink(context.Background(), orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if sum := counterSum(t, &rm, "orders.placed"); sum != 2 {
		t.Fatalf("orders.placed sum = %d, want 2", sum)
	}
}

// TestIntegrationSinkRecordsSpanEventOnRealSDKSpan drives the real Outlet path
// so a span event lands on a real recording SDK span, asserted through the
// in-memory span recorder.
func TestIntegrationSinkRecordsSpanEventOnRealSDKSpan(t *testing.T) {
	t.Parallel()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := tp.Tracer("integration").Start(context.Background(), "work")

	mp := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	reg := otelsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[otelsink.Meter] {
		return otelsink.SpanEvent("order.placed", attribute.String("id", o.ID))
	})

	outlet := otelsink.New(mp.Meter("integration"), reg)
	if err := outlet.Sink(ctx, orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	span.End()

	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	events := ended[0].Events()
	if len(events) != 1 || events[0].Name != "order.placed" {
		t.Fatalf("events = %+v, want one order.placed", events)
	}
}

// counterSum totals the data points of the named int64 sum metric.
func counterSum(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name != name {
				continue
			}
			sum, ok := mt.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q data = %T, want Sum[int64]", name, mt.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	t.Fatalf("metric %q not collected", name)
	return 0
}
