package telemetry

import "context"

// The no-op implementations are the zero-configuration default for every seam.
// They record nothing, allocate nothing per call, never panic, and are safe for
// concurrent use and for use after End. A consumer that provides no telemetry
// gets silent behavior with no overhead and no extra imports.
//
// The no-op types are unexported; consumers obtain them through NopTracer and
// NopMeter so the concrete shape stays an implementation detail.

// NopTracer returns a Tracer whose spans do nothing. It is the default Tracer
// for any consumer that provides none. The returned Tracer is safe for
// concurrent use.
func NopTracer() Tracer { return nopTracer{} }

// NopMeter returns a Meter whose instruments do nothing. It is the default
// Meter for any consumer that provides none. The returned Meter is safe for
// concurrent use.
func NopMeter() Meter { return nopMeter{} }

type nopTracer struct{}

func (nopTracer) Start(ctx context.Context, _ string, _ ...Attr) (context.Context, Span) {
	return ctx, nopSpan{}
}

type nopSpan struct{}

func (nopSpan) SetAttributes(...Attr)        {}
func (nopSpan) RecordError(error)            {}
func (nopSpan) SetStatus(StatusCode, string) {}
func (nopSpan) End()                         {}

type nopMeter struct{}

func (nopMeter) Counter(string, ...InstrumentOption) Counter     { return nopCounter{} }
func (nopMeter) Histogram(string, ...InstrumentOption) Histogram { return nopHistogram{} }
func (nopMeter) Gauge(string, ...InstrumentOption) Gauge         { return nopGauge{} }

type nopCounter struct{}

func (nopCounter) Add(context.Context, int64, ...Attr) {}

type nopHistogram struct{}

func (nopHistogram) Record(context.Context, float64, ...Attr) {}

type nopGauge struct{}

func (nopGauge) Record(context.Context, float64, ...Attr) {}

// Compile-time assertions that the no-op types satisfy the interfaces.
var (
	_ Tracer    = nopTracer{}
	_ Span      = nopSpan{}
	_ Meter     = nopMeter{}
	_ Counter   = nopCounter{}
	_ Histogram = nopHistogram{}
	_ Gauge     = nopGauge{}
)
