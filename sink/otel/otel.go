// SPDX-License-Identifier: Apache-2.0

// Package otel is a sink destination that turns payloads into OpenTelemetry
// metric recordings and span events. Register a transformer that maps each
// payload type to an [Op] built with [Counter], [Gauge], [Histogram], or
// [SpanEvent], then attach the result of [New] to a sink.Manifold.
//
// The injected client is a [Meter]: a narrow subset of the OpenTelemetry
// metric.Meter (Int64Counter, Float64Histogram, Float64Gauge) covering only the
// instruments this destination creates. Tests satisfy it with a hand-rolled
// fake; a real metric.Meter satisfies it structurally. Span-event operations
// read the active span from the context, so they need no client method and work
// against whatever span the caller has already started.
//
// Instruments are created at apply time from the meter and recorded against
// immediately, so a registry entry stays a pure description of intent. A
// creation failure is returned to the caller (the Emitter wraps it as a
// *sink.Error with sink.PhaseApply); it never panics.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	oteltrace "go.opentelemetry.io/otel/trace"

	csink "github.com/stablekernel/crucible/sink"
)

// Meter is the narrow OpenTelemetry metric.Meter surface this destination needs
// to build instruments. It is satisfied structurally by any *metric.Meter
// obtained from a configured MeterProvider, so this package never depends on a
// concrete SDK meter implementation and tests can supply a hand-rolled fake.
type Meter interface {
	Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error)
	Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error)
	Float64Gauge(name string, options ...metric.Float64GaugeOption) (metric.Float64Gauge, error)
}

// Counter returns an Op that creates a monotonic Int64 counter named name and
// adds delta to it with the given attributes. Instrument creation happens at
// apply time against the injected Meter; a creation error is returned unwrapped
// for the Emitter to classify.
func Counter(name string, delta int64, attrs ...attribute.KeyValue) csink.Op[Meter] {
	return csink.OpFunc[Meter](func(ctx context.Context, m Meter) error {
		inst, err := m.Int64Counter(name)
		if err != nil {
			return err
		}
		inst.Add(ctx, delta, metric.WithAttributes(attrs...))
		return nil
	})
}

// Gauge returns an Op that creates a synchronous Float64 gauge named name and
// records value to it with the given attributes. A gauge captures the latest
// instantaneous reading rather than accumulating.
func Gauge(name string, value float64, attrs ...attribute.KeyValue) csink.Op[Meter] {
	return csink.OpFunc[Meter](func(ctx context.Context, m Meter) error {
		inst, err := m.Float64Gauge(name)
		if err != nil {
			return err
		}
		inst.Record(ctx, value, metric.WithAttributes(attrs...))
		return nil
	})
}

// Histogram returns an Op that creates a Float64 histogram named name and
// records value into its distribution with the given attributes.
func Histogram(name string, value float64, attrs ...attribute.KeyValue) csink.Op[Meter] {
	return csink.OpFunc[Meter](func(ctx context.Context, m Meter) error {
		inst, err := m.Float64Histogram(name)
		if err != nil {
			return err
		}
		inst.Record(ctx, value, metric.WithAttributes(attrs...))
		return nil
	})
}

// SpanEvent returns an Op that adds an event named name, carrying attrs, to the
// span already active on the apply context. It reads the span with
// oteltrace.SpanFromContext, so it records nothing when no span is recording;
// that is a no-op and not an error. The Meter client is unused: span events
// flow through the trace API, not an instrument.
func SpanEvent(name string, attrs ...attribute.KeyValue) csink.Op[Meter] {
	return csink.OpFunc[Meter](func(ctx context.Context, _ Meter) error {
		span := oteltrace.SpanFromContext(ctx)
		if !span.IsRecording() {
			return nil
		}
		span.AddEvent(name, oteltrace.WithAttributes(attrs...))
		return nil
	})
}

// NewRegistry returns an empty registry of Op[Meter] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Meter]] {
	return csink.NewRegistry[csink.Op[Meter]]()
}

// New builds an Outlet that applies each payload's registered Op[Meter] against
// meter. The outlet is named "otel" unless overridden with sink.WithName.
func New(meter Meter, reg *csink.Registry[csink.Op[Meter]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Meter](meter, reg, append([]csink.EmitterOption{csink.WithName("otel")}, opts...)...)
}
