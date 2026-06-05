// Package datadog implements the Crucible telemetry interfaces on top of
// Datadog. Tracing is bridged onto dd-trace-go (github.com/DataDog/dd-trace-go/v2)
// and metrics onto DogStatsD (github.com/DataDog/datadog-go/v5/statsd), so a
// consumer wired with
//
//	telemetry.Nop().Apply(
//	    telemetry.WithTracer(datadog.NewTracer()),
//	    telemetry.WithMeter(datadog.NewMeter(statsdClient)),
//	)
//
// emits real Datadog spans and metrics without taking a direct dependency on the
// Datadog SDKs anywhere in its own code.
//
// Import path: github.com/stablekernel/crucible/telemetry/datadog
//
// # Tracing
//
// Tracer.Start starts a dd-trace-go span from the context (so it parents under
// any span already in the context) and returns the span-carrying context. The
// caller MUST propagate that context to nested work. Telemetry attributes become
// span tags via Span.SetTag, converted from telemetry.Attr (an slog.Attr) by
// reading slog.Value.Kind. Span.SetStatus(StatusError, …) and Span.RecordError
// mark the span errored; the recorded error is attached when the span finishes
// via Span.End (which calls Finish with tracer.WithError).
//
// The span starter is injectable (WithSpanStarter) so tests can drive the adapter
// with dd-trace-go's mocktracer. The default uses tracer.StartSpanFromContext,
// which routes to whichever global tracer is active (the real tracer in
// production, the mock tracer under test).
//
// # Metrics
//
// Meter.Counter/Histogram/Gauge emit DogStatsD Count/Histogram/Gauge calls
// through a statsd.ClientInterface (so tests can inject a fake). Attributes are
// converted to DogStatsD tags of the form "key:value". DogStatsD has no notion of
// per-instrument unit or description, so WithUnit/WithDescription are accepted and
// ignored (additive, never an error).
package datadog

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	"github.com/stablekernel/crucible/telemetry"
)

// spanStarter starts a span from a context, mirroring
// tracer.StartSpanFromContext. It is the seam tests inject through.
type spanStarter func(ctx context.Context, name string, opts ...tracer.StartSpanOption) (*tracer.Span, context.Context)

// TracerOption configures a Tracer.
type TracerOption func(*Tracer)

// WithSpanStarter overrides how spans are started. The default is
// tracer.StartSpanFromContext. Inject this to drive the adapter from a test
// (for example dd-trace-go's mocktracer).
func WithSpanStarter(start func(ctx context.Context, name string, opts ...tracer.StartSpanOption) (*tracer.Span, context.Context)) TracerOption {
	return func(t *Tracer) {
		if start != nil {
			t.start = start
		}
	}
}

// Tracer is a telemetry.Tracer backed by dd-trace-go.
type Tracer struct {
	start spanStarter
}

// NewTracer returns a telemetry.Tracer that emits dd-trace-go spans. By default
// it uses the active global dd-trace-go tracer; start one with tracer.Start in
// your process bootstrap (and tracer.Stop on shutdown).
func NewTracer(opts ...TracerOption) *Tracer {
	t := &Tracer{start: tracer.StartSpanFromContext}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Start begins a dd-trace-go span and returns the span-carrying context and a
// telemetry.Span wrapping it. The returned context MUST be propagated to nested
// work so downstream spans parent under this one.
func (t *Tracer) Start(ctx context.Context, name string, attrs ...telemetry.Attr) (context.Context, telemetry.Span) {
	s, ctx := t.start(ctx, name)
	for _, a := range attrs {
		s.SetTag(a.Key, attrValue(a))
	}
	return ctx, &span{s: s}
}

// span is a telemetry.Span backed by a dd-trace-go span.
type span struct {
	s     *tracer.Span
	err   error
	ended bool
}

func (s *span) SetAttributes(attrs ...telemetry.Attr) {
	if s.ended {
		return
	}
	for _, a := range attrs {
		s.s.SetTag(a.Key, attrValue(a))
	}
}

func (s *span) RecordError(err error) {
	if s.ended || err == nil {
		return
	}
	// Retain the error so End can attach it via tracer.WithError, which records
	// the message, type, and stack the way Datadog expects.
	s.err = err
}

func (s *span) SetStatus(code telemetry.StatusCode, msg string) {
	if s.ended {
		return
	}
	switch code {
	case telemetry.StatusError:
		if s.err == nil {
			// SetStatus(Error) without a recorded error still marks the span errored;
			// synthesize an error carrying the status message.
			if msg == "" {
				msg = "error"
			}
			s.err = statusError(msg)
		}
	case telemetry.StatusOK:
		// An explicit OK overrides any previously recorded error so the span
		// finishes clean. The error event was already appended by RecordError;
		// clearing s.err only prevents End from finishing the span with
		// tracer.WithError, which is what determines the error flag in Datadog.
		s.err = nil
	}
}

func (s *span) End() {
	if s.ended {
		return
	}
	s.ended = true
	if s.err != nil {
		s.s.Finish(tracer.WithError(s.err))
		return
	}
	s.s.Finish()
}

// statusError is a minimal error carrying a status message, used when SetStatus
// reports a failure without an associated error value.
type statusError string

func (e statusError) Error() string { return string(e) }

// Meter is a telemetry.Meter backed by DogStatsD.
type Meter struct {
	client statsd.ClientInterface
	rate   float64
}

// MeterOption configures a Meter.
type MeterOption func(*Meter)

// WithSampleRate sets the DogStatsD sample rate applied to every metric emission
// (1.0 = always sample). The default is 1.0.
func WithSampleRate(rate float64) MeterOption {
	return func(m *Meter) {
		if rate > 0 {
			m.rate = rate
		}
	}
}

// NewMeter returns a telemetry.Meter that emits DogStatsD metrics through the
// given client. A typical caller passes a *statsd.Client built with statsd.New.
func NewMeter(client statsd.ClientInterface, opts ...MeterOption) *Meter {
	m := &Meter{client: client, rate: 1.0}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Counter returns a monotonic int64 counter emitting DogStatsD Count. WithUnit
// and WithDescription are accepted and ignored (DogStatsD carries no such
// metadata).
func (m *Meter) Counter(name string, _ ...telemetry.InstrumentOption) telemetry.Counter {
	return &counter{client: m.client, name: name, rate: m.rate}
}

// Histogram returns a float64 distribution emitting DogStatsD Histogram.
func (m *Meter) Histogram(name string, _ ...telemetry.InstrumentOption) telemetry.Histogram {
	return &histogram{client: m.client, name: name, rate: m.rate}
}

// Gauge returns a synchronous float64 gauge emitting DogStatsD Gauge.
func (m *Meter) Gauge(name string, _ ...telemetry.InstrumentOption) telemetry.Gauge {
	return &gauge{client: m.client, name: name, rate: m.rate}
}

type counter struct {
	client statsd.ClientInterface
	name   string
	rate   float64
}

func (c *counter) Add(_ context.Context, n int64, attrs ...telemetry.Attr) {
	_ = c.client.Count(c.name, n, tags(attrs), c.rate)
}

type histogram struct {
	client statsd.ClientInterface
	name   string
	rate   float64
}

func (h *histogram) Record(_ context.Context, v float64, attrs ...telemetry.Attr) {
	_ = h.client.Histogram(h.name, v, tags(attrs), h.rate)
}

type gauge struct {
	client statsd.ClientInterface
	name   string
	rate   float64
}

func (g *gauge) Record(_ context.Context, v float64, attrs ...telemetry.Attr) {
	_ = g.client.Gauge(g.name, v, tags(attrs), g.rate)
}

// tags converts telemetry attributes into DogStatsD "key:value" tag strings.
func tags(attrs []telemetry.Attr) []string {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]string, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, a.Key+":"+tagValue(a.Value))
	}
	return out
}

// attrValue down-converts an attribute's slog.Value to a typed Go value suitable
// for Span.SetTag, which accepts any. Scalars keep their type; everything else
// stringifies.
func attrValue(a telemetry.Attr) any {
	v := a.Value
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return v.Float64()
	case slog.KindBool:
		return v.Bool()
	case slog.KindDuration:
		return v.Duration()
	case slog.KindTime:
		return v.Time()
	default:
		return tagValue(v)
	}
}

// tagValue renders an slog.Value as a DogStatsD tag value string by Kind.
func tagValue(v slog.Value) string {
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return strconv.FormatInt(v.Int64(), 10)
	case slog.KindUint64:
		return strconv.FormatUint(v.Uint64(), 10)
	case slog.KindFloat64:
		return strconv.FormatFloat(v.Float64(), 'g', -1, 64)
	case slog.KindBool:
		return strconv.FormatBool(v.Bool())
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindTime:
		return v.Time().Format("2006-01-02T15:04:05.999999999Z07:00")
	default:
		if s := v.String(); s != "" {
			return s
		}
		return fmt.Sprintf("%v", v.Any())
	}
}

// Compile-time assertions that the adapter types satisfy the telemetry
// interfaces.
var (
	_ telemetry.Tracer    = (*Tracer)(nil)
	_ telemetry.Span      = (*span)(nil)
	_ telemetry.Meter     = (*Meter)(nil)
	_ telemetry.Counter   = (*counter)(nil)
	_ telemetry.Histogram = (*histogram)(nil)
	_ telemetry.Gauge     = (*gauge)(nil)
)
