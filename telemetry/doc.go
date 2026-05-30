// Package telemetry is the Crucible suite's vendor-neutral tracing and metrics
// interface.
//
// Import path: github.com/stablekernel/crucible/telemetry
//
// # What this module is
//
// telemetry defines a small, stable set of interfaces — Tracer/Span,
// Meter/Counter/Histogram/Gauge, and the Attr attribute type — that the suite's
// IO modules (sink, broker, store) depend on for observability. It imports only
// the Go standard library and forces no vendor SDK on any consumer. That is the
// whole point: a consumer brings their own tracing/metrics backend through a
// thin adapter, and a consumer that brings nothing gets silent, zero-overhead
// behavior from the built-in no-op defaults.
//
// This is the "thin seams, no-op defaults, no forced dependencies" rule from the
// suite's engineering standards, applied to telemetry: the core interface forces
// no dependency, the default does nothing, and vendor wiring lives in optional,
// separately-versioned adapter sub-modules.
//
// # The interface
//
// Tracing:
//
//	Tracer.Start(ctx, name, attrs...) -> (ctx, Span)
//	Span.SetAttributes(attrs...)
//	Span.RecordError(err)
//	Span.SetStatus(code, msg)
//	Span.End()
//
// Metrics:
//
//	Meter.Counter(name, opts...)   -> Counter.Add(ctx, n int64, attrs...)
//	Meter.Histogram(name, opts...) -> Histogram.Record(ctx, v float64, attrs...)
//	Meter.Gauge(name, opts...)     -> Gauge.Record(ctx, v float64, attrs...)
//
// Counters are monotonic int64 deltas; histograms and gauges carry float64
// samples. Instrument metadata (unit, description) is supplied with the additive
// InstrumentOption helpers WithUnit and WithDescription.
//
// # Attributes
//
// Attr is an alias for the standard library's slog.Attr, so attribute values are
// slog.Value — the stdlib's allocation-optimized tagged union. Build attributes
// with the typed constructors re-exported here:
//
//	telemetry.String("payload.type", "Order")
//	telemetry.Int64("size", 100)
//	telemetry.Float64("latency_ms", 3.2)
//	telemetry.Bool("retried", true)
//
// The scalar constructors (String, Int64, Int, Uint64, Float64, Bool, Duration,
// Time) are zero-allocation: slog.Value stores their value inline, with no
// interface box. Type safety is preserved — an adapter reads Value.Kind and the
// typed accessors (Value.String, Value.Int64, …) rather than type-switching on
// any. Any is the documented escape hatch for an arbitrary value; it boxes into
// an interface and so allocates, so reach for it only when no typed constructor
// fits.
//
// # Context propagation is the seam
//
// Tracer.Start returns a context carrying the new span. Propagating that context
// into nested work is how spans parent: a downstream module's span, started from
// the returned context, nests under the caller's span automatically. This is the
// only coupling between modules — there is no shared global tracer, no
// package-level state.
//
// # No-op default
//
// NopTracer and NopMeter return implementations that record nothing, allocate
// nothing per call, never panic, and are safe to call concurrently and after a
// span has ended. Provider (see options.go) bundles a Tracer and Meter for a
// consuming module's config; Nop returns one wired to the no-op pair, so an
// unconfigured module is silent by default.
//
// # Injection
//
// A consuming module embeds a Provider in its config, seeds it with Nop, and
// exposes WithTracer/WithMeter options:
//
//	cfg.tel = telemetry.Nop().Apply(
//	    telemetry.WithTracer(myTracer),
//	    telemetry.WithMeter(myMeter),
//	)
//
// # Adapters
//
// Adapters translate this interface to a concrete backend and ship as separate,
// optional sub-modules so the core never imports a vendor SDK:
//
//   - telemetry/slog — a standard-library log/slog adapter (zero external
//     deps) that emits spans and metrics as structured logs. Shipped here; it
//     proves the seam end to end. Because Attr is slog.Attr, this adapter is
//     conversion-free: attributes pass straight to the slog handler.
//   - telemetry/otel, telemetry/datadog — deferred. Each would live in its own
//     sub-module with its own go.mod that requires the vendor SDK, implement the
//     same interfaces (Span.SetStatus -> otel status / dd error flag,
//     ResolveInstrument -> instrument unit/description), and be wired by a
//     consumer via WithTracer/WithMeter exactly like the slog adapter. They convert
//     each attribute with a switch over Attr.Value.Kind (the slog.Value kind),
//     reading the typed accessor for each scalar kind and Value.Any only for the
//     KindAny escape hatch.
//
// # Naming convention
//
// Instrument and span names use dotted lower-snake with a module prefix
// (sink.sunk, sink.flush_latency_ms, state.transitions). Every module follows
// this so metrics and traces line up across the suite.
//
// # Guardrails
//
// No os.Exit, no log.Fatal, no panic on operational paths. The interface carries
// no error returns on the hot path; recording telemetry never fails a caller's
// operation.
//
// Stability label: experimental (pre-v1).
package telemetry
