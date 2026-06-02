// SPDX-License-Identifier: Apache-2.0

package otel_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	csink "github.com/stablekernel/crucible/sink"
	otelsink "github.com/stablekernel/crucible/sink/otel"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeMeter is a hand-rolled otelsink.Meter with no SDK meter and no mockery. It
// records which instrument kinds were created and the measurements made against
// them, and can inject a creation error per instrument kind.
type fakeMeter struct {
	counters   []recordedNum[int64]
	histograms []recordedNum[float64]
	gauges     []recordedNum[float64]

	counterErr   error
	histogramErr error
	gaugeErr     error
}

type recordedNum[N int64 | float64] struct {
	name  string
	value N
	attrs []attribute.KeyValue
}

func (m *fakeMeter) Int64Counter(name string, _ ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if m.counterErr != nil {
		return nil, m.counterErr
	}
	return fakeInt64Counter{meter: m, name: name}, nil
}

func (m *fakeMeter) Float64Histogram(name string, _ ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if m.histogramErr != nil {
		return nil, m.histogramErr
	}
	return fakeFloat64Histogram{meter: m, name: name}, nil
}

func (m *fakeMeter) Float64Gauge(name string, _ ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	if m.gaugeErr != nil {
		return nil, m.gaugeErr
	}
	return fakeFloat64Gauge{meter: m, name: name}, nil
}

type fakeInt64Counter struct {
	metric.Int64Counter // embedded for forward-compatible interface; methods overridden below
	meter               *fakeMeter
	name                string
}

func (c fakeInt64Counter) Add(_ context.Context, v int64, opts ...metric.AddOption) {
	cfg := metric.NewAddConfig(opts)
	set := cfg.Attributes()
	c.meter.counters = append(c.meter.counters, recordedNum[int64]{c.name, v, set.ToSlice()})
}

type fakeFloat64Histogram struct {
	metric.Float64Histogram
	meter *fakeMeter
	name  string
}

func (h fakeFloat64Histogram) Record(_ context.Context, v float64, opts ...metric.RecordOption) {
	cfg := metric.NewRecordConfig(opts)
	set := cfg.Attributes()
	h.meter.histograms = append(h.meter.histograms, recordedNum[float64]{h.name, v, set.ToSlice()})
}

type fakeFloat64Gauge struct {
	metric.Float64Gauge
	meter *fakeMeter
	name  string
}

func (g fakeFloat64Gauge) Record(_ context.Context, v float64, opts ...metric.RecordOption) {
	cfg := metric.NewRecordConfig(opts)
	set := cfg.Attributes()
	g.meter.gauges = append(g.meter.gauges, recordedNum[float64]{g.name, v, set.ToSlice()})
}

type orderPlaced struct{ ID string }

func TestCounterAdds(t *testing.T) {
	t.Parallel()

	m := &fakeMeter{}
	reg := otelsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[otelsink.Meter] {
		return otelsink.Counter("orders.placed", 1, attribute.String("id", o.ID))
	})
	if err := otelsink.New(m, reg).Sink(context.Background(), orderPlaced{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(m.counters) != 1 || m.counters[0].name != "orders.placed" || m.counters[0].value != 1 {
		t.Fatalf("counters = %+v, want one orders.placed +1", m.counters)
	}
	if len(m.counters[0].attrs) != 1 || m.counters[0].attrs[0].Value.AsString() != "A-1" {
		t.Fatalf("attrs = %v, want [id=A-1]", m.counters[0].attrs)
	}
}

func TestGaugeRecords(t *testing.T) {
	t.Parallel()

	m := &fakeMeter{}
	reg := otelsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, _ orderPlaced) csink.Op[otelsink.Meter] {
		return otelsink.Gauge("queue.depth", 12.5)
	})
	if err := otelsink.New(m, reg).Sink(context.Background(), orderPlaced{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(m.gauges) != 1 || m.gauges[0].name != "queue.depth" || m.gauges[0].value != 12.5 {
		t.Fatalf("gauges = %+v, want one queue.depth=12.5", m.gauges)
	}
}

func TestHistogramRecords(t *testing.T) {
	t.Parallel()

	m := &fakeMeter{}
	reg := otelsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, _ orderPlaced) csink.Op[otelsink.Meter] {
		return otelsink.Histogram("request.latency.ms", 42.0)
	})
	if err := otelsink.New(m, reg).Sink(context.Background(), orderPlaced{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(m.histograms) != 1 || m.histograms[0].name != "request.latency.ms" || m.histograms[0].value != 42.0 {
		t.Fatalf("histograms = %+v, want one request.latency.ms=42", m.histograms)
	}
}

func TestSpanEventAddsToActiveSpan(t *testing.T) {
	t.Parallel()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	ctx, span := tp.Tracer("test").Start(context.Background(), "work")

	reg := otelsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[otelsink.Meter] {
		return otelsink.SpanEvent("order.placed", attribute.String("id", o.ID))
	})
	if err := otelsink.New(&fakeMeter{}, reg).Sink(ctx, orderPlaced{ID: "A-1"}); err != nil {
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

func TestSpanEventNoActiveSpanIsNoop(t *testing.T) {
	t.Parallel()

	reg := otelsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, _ orderPlaced) csink.Op[otelsink.Meter] {
		return otelsink.SpanEvent("order.placed")
	})
	// Background context carries a non-recording no-op span; AddEvent is skipped.
	if err := otelsink.New(&fakeMeter{}, reg).Sink(context.Background(), orderPlaced{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() with no span = %v, want nil", err)
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	err := otelsink.New(&fakeMeter{}, otelsink.NewRegistry()).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestInstrumentErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("instrument creation failed")
	tests := []struct {
		name  string
		meter *fakeMeter
		op    csink.Op[otelsink.Meter]
	}{
		{"counter", &fakeMeter{counterErr: boom}, otelsink.Counter("c", 1)},
		{"gauge", &fakeMeter{gaugeErr: boom}, otelsink.Gauge("g", 1)},
		{"histogram", &fakeMeter{histogramErr: boom}, otelsink.Histogram("h", 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reg := otelsink.NewRegistry()
			op := tt.op
			csink.Register(reg, func(_ context.Context, _ orderPlaced) csink.Op[otelsink.Meter] { return op })

			err := otelsink.New(tt.meter, reg).Sink(context.Background(), orderPlaced{ID: "A-1"})
			if !errors.Is(err, boom) {
				t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
			}
			var se *csink.Error
			if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "otel" {
				t.Fatalf("recovered = %+v, want *sink.Error{Outlet:otel, Phase:apply}", se)
			}
		})
	}
}

// TestRealSDKMeterRecords proves the Op constructors record against a real
// OpenTelemetry SDK meter, asserted through an in-memory manual reader. No live
// collector and no network: the reader collects synchronously on demand.
func TestRealSDKMeterRecords(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test")

	reg := otelsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[otelsink.Meter] {
		return otelsink.Counter("orders.placed", 3, attribute.String("id", o.ID))
	})
	if err := otelsink.New(meter, reg).Sink(context.Background(), orderPlaced{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	sum := findCounterSum(t, &rm, "orders.placed")
	if sum != 3 {
		t.Fatalf("orders.placed sum = %d, want 3", sum)
	}
}

func findCounterSum(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
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

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet {
		return otelsink.New(&fakeMeter{}, otelsink.NewRegistry())
	})
}

// TestSDKMeterSatisfiesInterface confirms a real OpenTelemetry SDK meter
// satisfies the narrow Meter interface structurally, so production wiring needs
// no adapter.
func TestSDKMeterSatisfiesInterface(t *testing.T) {
	t.Parallel()
	mp := sdkmetric.NewMeterProvider()
	var _ otelsink.Meter = mp.Meter("test")
}
