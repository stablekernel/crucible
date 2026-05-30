package otel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/stablekernel/crucible/telemetry"
	oteladapter "github.com/stablekernel/crucible/telemetry/otel"
)

// newTracer returns an adapter tracer plus the SpanRecorder capturing its spans.
func newTracer() (*oteladapter.Tracer, *tracetest.SpanRecorder) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	return oteladapter.NewTracer(tp.Tracer("test")), rec
}

// newMeter returns an adapter meter plus a manual reader to collect metrics.
func newMeter() (*oteladapter.Meter, *metric.ManualReader) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	return oteladapter.NewMeter(mp.Meter("test")), reader
}

func collect(t *testing.T, reader *metric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

// findAttr returns the value of key in a set, plus whether it was present.
func findAttr(set attribute.Set, key string) (attribute.Value, bool) {
	return set.Value(attribute.Key(key))
}

// TestTracer_SpanLifecycle asserts a span is recorded with its name, attributes,
// recorded error, and OK status.
func TestTracer_SpanLifecycle(t *testing.T) {
	tr, rec := newTracer()

	ctx, span := tr.Start(context.Background(), "sink.Sink",
		telemetry.String("payload.type", "Order"),
		telemetry.Int64("count", 3),
		telemetry.Bool("flush", true),
		telemetry.Float64("ratio", 0.5),
	)
	span.SetAttributes(telemetry.String("outlet", "dynamo"))
	span.RecordError(errors.New("boom"))
	span.SetStatus(telemetry.StatusOK, "done")
	span.End()
	_ = ctx

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.Name() != "sink.Sink" {
		t.Errorf("name = %q, want sink.Sink", s.Name())
	}
	attrs := attribute.NewSet(s.Attributes()...)
	if v, ok := findAttr(attrs, "payload.type"); !ok || v.AsString() != "Order" {
		t.Errorf("payload.type = %v (ok=%v)", v.AsString(), ok)
	}
	if v, ok := findAttr(attrs, "count"); !ok || v.AsInt64() != 3 {
		t.Errorf("count = %v (ok=%v)", v.AsInt64(), ok)
	}
	if v, ok := findAttr(attrs, "flush"); !ok || !v.AsBool() {
		t.Errorf("flush = %v (ok=%v)", v.AsBool(), ok)
	}
	if v, ok := findAttr(attrs, "ratio"); !ok || v.AsFloat64() != 0.5 {
		t.Errorf("ratio = %v (ok=%v)", v.AsFloat64(), ok)
	}
	if v, ok := findAttr(attrs, "outlet"); !ok || v.AsString() != "dynamo" {
		t.Errorf("outlet = %v (ok=%v)", v.AsString(), ok)
	}
	if s.Status().Code != codes.Ok {
		t.Errorf("status = %v, want Ok", s.Status().Code)
	}
	if len(s.Events()) == 0 {
		t.Error("expected a recorded error event")
	}
}

// TestTracer_ErrorStatus asserts StatusError maps onto codes.Error with its msg.
func TestTracer_ErrorStatus(t *testing.T) {
	tr, rec := newTracer()
	_, span := tr.Start(context.Background(), "op")
	span.SetStatus(telemetry.StatusError, "outlet failed")
	span.End()

	s := rec.Ended()[0]
	if s.Status().Code != codes.Error {
		t.Errorf("status = %v, want Error", s.Status().Code)
	}
	if s.Status().Description != "outlet failed" {
		t.Errorf("description = %q, want 'outlet failed'", s.Status().Description)
	}
}

// TestTracer_Parentage asserts a child span started from the returned context
// parents under the first span.
func TestTracer_Parentage(t *testing.T) {
	tr, rec := newTracer()
	ctx, parent := tr.Start(context.Background(), "state.transition")
	_, child := tr.Start(ctx, "sink.Sink")
	child.End()
	parent.End()

	var p, c sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch s.Name() {
		case "state.transition":
			p = s
		case "sink.Sink":
			c = s
		}
	}
	if p == nil || c == nil {
		t.Fatal("missing spans")
	}
	if c.Parent().SpanID() != p.SpanContext().SpanID() {
		t.Errorf("child parent = %v, want %v", c.Parent().SpanID(), p.SpanContext().SpanID())
	}
}

// TestTracer_AttrKinds exercises the duration/time/uint64/any conversions.
func TestTracer_AttrKinds(t *testing.T) {
	tr, rec := newTracer()
	_, span := tr.Start(context.Background(), "op",
		telemetry.Duration("elapsed", 1500*time.Millisecond),
		telemetry.Uint64("u", 42),
		telemetry.Time("at", time.Unix(0, 0).UTC()),
		telemetry.Any("obj", struct{ X int }{1}),
	)
	span.End()

	attrs := attribute.NewSet(rec.Ended()[0].Attributes()...)
	if v, ok := findAttr(attrs, "elapsed"); !ok || v.AsInt64() != int64(1500*time.Millisecond) {
		t.Errorf("elapsed = %v (ok=%v)", v.AsInt64(), ok)
	}
	if v, ok := findAttr(attrs, "u"); !ok || v.AsInt64() != 42 {
		t.Errorf("u = %v (ok=%v)", v.AsInt64(), ok)
	}
	if v, ok := findAttr(attrs, "at"); !ok || v.AsString() == "" {
		t.Errorf("at = %v (ok=%v)", v.AsString(), ok)
	}
	if v, ok := findAttr(attrs, "obj"); !ok || v.AsString() == "" {
		t.Errorf("obj = %v (ok=%v)", v.AsString(), ok)
	}
}

// TestMeter_Instruments asserts counter/histogram/gauge are recorded with their
// values and attributes.
func TestMeter_Instruments(t *testing.T) {
	mt, reader := newMeter()
	ctx := context.Background()

	mt.Counter("sink.sunk", telemetry.WithDescription("records sunk"), telemetry.WithUnit("{record}")).
		Add(ctx, 3, telemetry.String("outlet", "dynamo"))
	mt.Histogram("sink.flush_latency_ms", telemetry.WithUnit("ms")).Record(ctx, 12.5)
	mt.Gauge("state.in_state").Record(ctx, 7)

	rm := collect(t, reader)
	found := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			found[m.Name] = true
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				if m.Name == "sink.sunk" {
					if data.DataPoints[0].Value != 3 {
						t.Errorf("counter value = %d, want 3", data.DataPoints[0].Value)
					}
					if v, ok := findAttr(data.DataPoints[0].Attributes, "outlet"); !ok || v.AsString() != "dynamo" {
						t.Errorf("counter attr outlet = %v (ok=%v)", v.AsString(), ok)
					}
					if m.Unit != "{record}" {
						t.Errorf("counter unit = %q, want {record}", m.Unit)
					}
				}
			case metricdata.Histogram[float64]:
				if data.DataPoints[0].Sum != 12.5 {
					t.Errorf("histogram sum = %v, want 12.5", data.DataPoints[0].Sum)
				}
				if m.Unit != "ms" {
					t.Errorf("histogram unit = %q, want ms", m.Unit)
				}
			case metricdata.Gauge[float64]:
				if data.DataPoints[0].Value != 7 {
					t.Errorf("gauge value = %v, want 7", data.DataPoints[0].Value)
				}
			}
		}
	}
	for _, name := range []string{"sink.sunk", "sink.flush_latency_ms", "state.in_state"} {
		if !found[name] {
			t.Errorf("instrument %q not recorded", name)
		}
	}
}

// TestSpan_NoErrorOnNil confirms RecordError(nil) records nothing.
func TestSpan_NoErrorOnNil(t *testing.T) {
	tr, rec := newTracer()
	_, span := tr.Start(context.Background(), "op")
	span.RecordError(nil)
	span.SetStatus(telemetry.StatusUnset, "")
	span.End()

	if len(rec.Ended()[0].Events()) != 0 {
		t.Error("RecordError(nil) should record no event")
	}
}
