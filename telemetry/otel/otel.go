// Package otel implements the Crucible telemetry interfaces on top of
// OpenTelemetry. It bridges crucible/telemetry's vendor-neutral Tracer and Meter
// onto an OpenTelemetry trace.Tracer and metric.Meter, so a consumer wired with
//
//	telemetry.Nop().Apply(
//	    telemetry.WithTracer(otel.NewTracer(otelTracer)),
//	    telemetry.WithMeter(otel.NewMeter(otelMeter)),
//	)
//
// emits real OpenTelemetry spans and metrics without taking a direct dependency
// on the OpenTelemetry SDK anywhere in its own code.
//
// Import path: github.com/stablekernel/crucible/telemetry/otel
//
// # Mapping
//
//   - Tracer.Start delegates to trace.Tracer.Start, propagating the returned
//     context so nested spans parent correctly. Attributes are converted from
//     telemetry.Attr (an slog.Attr, value an slog.Value) to OpenTelemetry
//     attribute.KeyValue by reading slog.Value.Kind.
//   - Span.SetAttributes → span.SetAttributes; Span.RecordError →
//     span.RecordError; Span.SetStatus → span.SetStatus with codes.Ok/Error/Unset;
//     Span.End → span.End.
//   - Meter.Counter → an Int64Counter (Add); Meter.Histogram → a Float64Histogram
//     (Record); Meter.Gauge → a synchronous Float64Gauge (Record). WithUnit and
//     WithDescription are honored via the OpenTelemetry instrument options.
//
// Instrument construction never panics: if the OpenTelemetry SDK returns an error
// building an instrument, the adapter falls back to a no-op instrument so a
// metric never brings the caller down.
package otel

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/stablekernel/crucible/telemetry"
)

// Tracer is a telemetry.Tracer backed by an OpenTelemetry trace.Tracer.
type Tracer struct {
	tracer oteltrace.Tracer
}

// NewTracer returns a telemetry.Tracer that emits spans through the given
// OpenTelemetry tracer. A typical caller obtains the tracer from a configured
// TracerProvider, for example otelsdk.Tracer("crucible").
func NewTracer(t oteltrace.Tracer) *Tracer { return &Tracer{tracer: t} }

// Start begins an OpenTelemetry span and returns the span-carrying context and a
// telemetry.Span wrapping it. The returned context MUST be propagated to nested
// work so downstream spans parent under this one.
func (t *Tracer) Start(ctx context.Context, name string, attrs ...telemetry.Attr) (context.Context, telemetry.Span) {
	ctx, s := t.tracer.Start(ctx, name, oteltrace.WithAttributes(convertAttrs(attrs)...))
	return ctx, span{s}
}

// span is a telemetry.Span backed by an OpenTelemetry span.
type span struct {
	s oteltrace.Span
}

func (s span) SetAttributes(attrs ...telemetry.Attr) {
	s.s.SetAttributes(convertAttrs(attrs)...)
}

func (s span) RecordError(err error) {
	if err != nil {
		s.s.RecordError(err)
	}
}

func (s span) SetStatus(code telemetry.StatusCode, msg string) {
	switch code {
	case telemetry.StatusOK:
		s.s.SetStatus(codes.Ok, msg)
	case telemetry.StatusError:
		s.s.SetStatus(codes.Error, msg)
	default:
		s.s.SetStatus(codes.Unset, msg)
	}
}

func (s span) End() { s.s.End() }

// Meter is a telemetry.Meter backed by an OpenTelemetry metric.Meter.
type Meter struct {
	meter metric.Meter
}

// NewMeter returns a telemetry.Meter that emits instruments through the given
// OpenTelemetry meter. A typical caller obtains the meter from a configured
// MeterProvider, for example otelsdk.Meter("crucible").
func NewMeter(m metric.Meter) *Meter { return &Meter{meter: m} }

// Counter returns a monotonic int64 counter backed by an OpenTelemetry
// Int64Counter. If the instrument cannot be created it falls back to a no-op.
func (m *Meter) Counter(name string, opts ...telemetry.InstrumentOption) telemetry.Counter {
	cfg := telemetry.ResolveInstrument(opts...)
	c, err := m.meter.Int64Counter(name, metric.WithUnit(cfg.Unit), metric.WithDescription(cfg.Description))
	if err != nil {
		return telemetry.NopMeter().Counter(name)
	}
	return counter{c}
}

// Histogram returns a float64 distribution backed by an OpenTelemetry
// Float64Histogram. If the instrument cannot be created it falls back to a no-op.
func (m *Meter) Histogram(name string, opts ...telemetry.InstrumentOption) telemetry.Histogram {
	cfg := telemetry.ResolveInstrument(opts...)
	h, err := m.meter.Float64Histogram(name, metric.WithUnit(cfg.Unit), metric.WithDescription(cfg.Description))
	if err != nil {
		return telemetry.NopMeter().Histogram(name)
	}
	return histogram{h}
}

// Gauge returns a synchronous float64 gauge backed by an OpenTelemetry
// Float64Gauge. If the instrument cannot be created it falls back to a no-op.
func (m *Meter) Gauge(name string, opts ...telemetry.InstrumentOption) telemetry.Gauge {
	cfg := telemetry.ResolveInstrument(opts...)
	g, err := m.meter.Float64Gauge(name, metric.WithUnit(cfg.Unit), metric.WithDescription(cfg.Description))
	if err != nil {
		return telemetry.NopMeter().Gauge(name)
	}
	return gauge{g}
}

type counter struct{ c metric.Int64Counter }

func (c counter) Add(ctx context.Context, n int64, attrs ...telemetry.Attr) {
	c.c.Add(ctx, n, metric.WithAttributes(convertAttrs(attrs)...))
}

type histogram struct{ h metric.Float64Histogram }

func (h histogram) Record(ctx context.Context, v float64, attrs ...telemetry.Attr) {
	h.h.Record(ctx, v, metric.WithAttributes(convertAttrs(attrs)...))
}

type gauge struct{ g metric.Float64Gauge }

func (g gauge) Record(ctx context.Context, v float64, attrs ...telemetry.Attr) {
	g.g.Record(ctx, v, metric.WithAttributes(convertAttrs(attrs)...))
}

// convertAttrs converts a slice of telemetry.Attr into OpenTelemetry
// attribute.KeyValue. Each attribute's slog.Value is down-converted by Kind so
// scalar values keep their type; Any and any unrecognized kind stringify via the
// slog.Value's own String method.
func convertAttrs(attrs []telemetry.Attr) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, convertAttr(a))
	}
	return out
}

func convertAttr(a telemetry.Attr) attribute.KeyValue {
	key := attribute.Key(a.Key)
	v := a.Value
	switch v.Kind() {
	case slog.KindString:
		return key.String(v.String())
	case slog.KindInt64:
		return key.Int64(v.Int64())
	case slog.KindUint64:
		// OpenTelemetry has no uint64 attribute type; represent as int64, which
		// is exact for values that fit and otherwise wraps deterministically.
		return key.Int64(int64(v.Uint64()))
	case slog.KindFloat64:
		return key.Float64(v.Float64())
	case slog.KindBool:
		return key.Bool(v.Bool())
	case slog.KindDuration:
		// Durations are encoded as integer nanoseconds, the slog default, so the
		// unit is unambiguous on the backend.
		return key.Int64(int64(v.Duration()))
	case slog.KindTime:
		return key.String(v.Time().Format("2006-01-02T15:04:05.999999999Z07:00"))
	default:
		// Any, Group, LogValuer, and anything unrecognized: stringify. slog.Value's
		// String never panics and resolves LogValuers.
		return key.String(stringify(v))
	}
}

func stringify(v slog.Value) string {
	if s := v.String(); s != "" {
		return s
	}
	return fmt.Sprintf("%v", v.Any())
}

// Compile-time assertions that the adapter types satisfy the telemetry
// interfaces.
var (
	_ telemetry.Tracer    = (*Tracer)(nil)
	_ telemetry.Span      = span{}
	_ telemetry.Meter     = (*Meter)(nil)
	_ telemetry.Counter   = counter{}
	_ telemetry.Histogram = histogram{}
	_ telemetry.Gauge     = gauge{}
)
