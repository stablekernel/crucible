// Package telemetry defines Crucible's vendor-neutral tracing and metrics
// interfaces. See doc.go for the package overview.
package telemetry

import (
	"context"
	"log/slog"
)

// Attr is a single key/value telemetry attribute (an OpenTelemetry "attribute",
// a Datadog "tag", a structured-log field). It is an alias for the standard
// library's slog.Attr, so its Value is a slog.Value — the stdlib's
// allocation-optimized tagged union. Scalar attributes (string, int64, float64,
// bool, duration, time) are stored inline without boxing, and remain type-safe
// via Value.Kind and the typed accessors; only Any boxes an arbitrary value
// into an interface. Adapters read Value.Kind to down-convert to their backend.
//
// Construct attributes with the typed constructors (String, Int64, Float64,
// Bool, …) rather than building the struct directly. Keep keys to dotted
// lower-snake with a module prefix (for example "payload.type", "sink.outlet");
// see the package README for the naming convention shared across the suite.
type Attr = slog.Attr

// The typed attribute constructors are re-exported directly from log/slog. Each
// returns an Attr (= slog.Attr); the scalar constructors are zero-allocation
// because slog.Value stores their value inline without an interface box.
//
// Any is the explicit escape hatch for an arbitrary value: it boxes into an
// interface and so allocates. Reach for it only when no typed constructor fits.
var (
	// String builds a string attribute. Zero-allocation.
	String = slog.String
	// Int64 builds an int64 attribute. Zero-allocation.
	Int64 = slog.Int64
	// Int builds an int attribute (stored as int64). Zero-allocation.
	Int = slog.Int
	// Uint64 builds a uint64 attribute. Zero-allocation.
	Uint64 = slog.Uint64
	// Float64 builds a float64 attribute. Zero-allocation.
	Float64 = slog.Float64
	// Bool builds a bool attribute. Zero-allocation.
	Bool = slog.Bool
	// Duration builds a time.Duration attribute. Zero-allocation.
	Duration = slog.Duration
	// Time builds a time.Time attribute. Zero-allocation.
	Time = slog.Time
	// Any builds an attribute from an arbitrary value. This is the documented
	// escape hatch: it boxes the value into an interface and therefore allocates.
	// Prefer a typed constructor whenever one fits.
	Any = slog.Any
)

// StatusCode classifies the outcome of a span, distinct from any error event
// recorded on it via Span.RecordError. It maps onto OpenTelemetry's status code
// and Datadog's error flag.
type StatusCode int

const (
	// StatusUnset is the default: the span carries no explicit outcome.
	StatusUnset StatusCode = iota
	// StatusOK marks the operation as having completed successfully.
	StatusOK
	// StatusError marks the operation as having failed.
	StatusError
)

// Tracer starts spans. It is the only entry point for tracing; a Span is always
// obtained from Start, never constructed directly.
type Tracer interface {
	// Start begins a span named name, returning a context carrying the span and
	// the span itself. The returned context MUST be propagated to nested work so
	// that downstream spans (in this module or another) parent under this one.
	// The caller MUST call End on the returned span, conventionally via defer.
	Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
}

// Span is a single traced operation. It is obtained from Tracer.Start and must
// be ended exactly once. All methods MUST be safe to call on a span returned by
// a no-op tracer and after End has been called.
type Span interface {
	// SetAttributes adds or overwrites attributes on the span.
	SetAttributes(attrs ...Attr)
	// RecordError records err as an error event on the span. It does not itself
	// set the span's status; call SetStatus(StatusError, …) for that when the
	// error is terminal for the operation.
	RecordError(err error)
	// SetStatus sets the span's outcome. The final call wins.
	SetStatus(code StatusCode, msg string)
	// End finishes the span. Calls after the first are no-ops.
	End()
}

// Meter creates metric instruments. Instruments are identified by name; calling
// the same constructor with the same name and options is expected to return an
// equivalent instrument (adapters may cache and return the same handle). Pass
// instrument metadata (unit, description) via InstrumentOption.
type Meter interface {
	// Counter returns a monotonic int64 counter. Counters only ever increase;
	// for values that rise and fall, use Gauge.
	Counter(name string, opts ...InstrumentOption) Counter
	// Histogram returns a float64 distribution instrument for recording sampled
	// values such as latencies or sizes.
	Histogram(name string, opts ...InstrumentOption) Histogram
	// Gauge returns a synchronous float64 gauge for a value that rises and falls
	// (for example the number of instances currently in a given state). The
	// caller sets the current value via Gauge.Record at the moment it changes.
	Gauge(name string, opts ...InstrumentOption) Gauge
}

// Counter is a monotonic, integer-valued metric. Counts are non-negative; the
// suite uses int64 deltas because every counted thing (records sunk, failures,
// drops) is whole-numbered, which keeps adapter mappings exact.
type Counter interface {
	// Add increments the counter by n (n >= 0) with the given attributes.
	Add(ctx context.Context, n int64, attrs ...Attr)
}

// Histogram records a distribution of float64 samples (latencies, sizes).
// Values are float64 so sub-unit measurements (for example fractional
// milliseconds) are not truncated.
type Histogram interface {
	// Record adds the sample v with the given attributes.
	Record(ctx context.Context, v float64, attrs ...Attr)
}

// Gauge records the current value of a quantity that rises and falls. It is
// synchronous: the consumer calls Record whenever the value changes. An
// observable/callback variant is intentionally omitted from the core to keep
// the interface small; it can be added additively by an adapter if needed.
type Gauge interface {
	// Record sets the gauge's current value to v with the given attributes.
	Record(ctx context.Context, v float64, attrs ...Attr)
}

// instrumentConfig holds resolved InstrumentOption state. It is exposed to
// adapters via InstrumentConfig so they can read unit/description without each
// adapter re-implementing option handling.
type instrumentConfig struct {
	unit        string
	description string
}

// InstrumentOption configures a metric instrument at creation time. Options are
// additive: unknown-to-an-adapter metadata is simply ignored, never an error.
type InstrumentOption func(*instrumentConfig)

// WithUnit sets the instrument's unit (for example "ms", "By", "{record}"),
// following the UCUM conventions OpenTelemetry uses.
func WithUnit(unit string) InstrumentOption {
	return func(c *instrumentConfig) { c.unit = unit }
}

// WithDescription sets a human-readable description of the instrument.
func WithDescription(desc string) InstrumentOption {
	return func(c *instrumentConfig) { c.description = desc }
}

// InstrumentConfig is the resolved metadata for an instrument, for adapters to
// read when constructing their backend instrument.
type InstrumentConfig struct {
	// Unit is the instrument unit, or "" if unset.
	Unit string
	// Description is the human-readable description, or "" if unset.
	Description string
}

// ResolveInstrument applies opts and returns the resolved InstrumentConfig.
// Adapters call this to honor WithUnit/WithDescription uniformly.
func ResolveInstrument(opts ...InstrumentOption) InstrumentConfig {
	var c instrumentConfig
	for _, opt := range opts {
		opt(&c)
	}
	return InstrumentConfig{Unit: c.unit, Description: c.description}
}
