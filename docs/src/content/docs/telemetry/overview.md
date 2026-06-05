---
title: What is crucible/telemetry
description: A vendor-neutral tracing and metrics interface for the IO modules; stdlib-only, with a no-op default and opt-in adapters.
sidebar:
  order: 1
---

<!-- IMAGE-SLOT: telemetry-overview-seam (a sky-squid smith reading gauges fed by one molten stream that splits toward labelled instruments: slog, otel, datadog; ember/copper on steel) 16:9 -->

`crucible/telemetry` is the suite's **observability seam**: a small, stable set
of interfaces that the IO modules ([`sink`](/crucible/sink/overview/),
[`source`](/crucible/source/overview/), and friends) depend on for tracing and
metrics.

It imports **only the Go standard library** and forces **no vendor SDK** on any
consumer. That is the whole point. A consumer brings their own tracing/metrics
backend through a thin adapter, and a consumer that brings nothing gets silent,
zero-overhead behavior from the built-in no-op defaults. This is the suite's
"thin seams, no-op defaults, no forced dependencies" rule applied to telemetry:
the core interface forces no dependency, the default does nothing, and vendor
wiring lives in optional, separately-versioned adapter sub-modules.

## The interface

Two surfaces, tracing and metrics, plus an attribute type:

```go
// Tracing
Tracer.Start(ctx, name, attrs...) -> (ctx, Span)
Span.SetAttributes(attrs...)
Span.RecordError(err)
Span.SetStatus(code, msg)   // StatusUnset | StatusOK | StatusError
Span.End()

// Metrics
Meter.Counter(name, opts...)   -> Counter.Add(ctx, n int64, attrs...)
Meter.Histogram(name, opts...) -> Histogram.Record(ctx, v float64, attrs...)
Meter.Gauge(name, opts...)     -> Gauge.Record(ctx, v float64, attrs...)
```

Counters are monotonic `int64` deltas, so every counted thing (records sunk,
failures, drops) stays whole-numbered and adapter mappings stay exact.
Histograms and gauges carry `float64` samples so sub-unit measurements
(fractional milliseconds) are not truncated. Instrument metadata is supplied
with the additive `WithUnit` and `WithDescription` options.

## Attributes are slog.Attr

`Attr` is an alias for the standard library's `slog.Attr`, so an attribute value
is a `slog.Value`, the stdlib's allocation-optimized tagged union. Build
attributes with the typed constructors re-exported here:

```go
telemetry.String("payload.type", "Order")
telemetry.Int64("size", 100)
telemetry.Float64("latency_ms", 3.2)
telemetry.Bool("retried", true)
```

The scalar constructors (`String`, `Int64`, `Int`, `Uint64`, `Float64`, `Bool`,
`Duration`, `Time`) are zero-allocation: the value is stored inline, never boxed.
`telemetry.Any` is the documented escape hatch for an arbitrary value; it boxes
into an interface and so allocates, so reach for it only when no typed
constructor fits.

## Context is the only coupling

`Tracer.Start` returns a context carrying the new span. Propagate that context
into nested work and a downstream span, in this module or another, parents under
the caller's span automatically. There is no shared global tracer and no
package-level state; context is the only coupling between modules.

## No-op by default

`NopTracer` and `NopMeter` record nothing, allocate nothing per call, never
panic, and are safe to call concurrently and after a span has ended. A consuming
module embeds a `Provider`, seeds it with `Nop`, and exposes `WithTracer` and
`WithMeter` options:

```go
tel := telemetry.Nop().Apply(
    telemetry.WithTracer(myTracer),
    telemetry.WithMeter(myMeter),
)

ctx, span := tel.Tracer.Start(ctx, "sink.Sink",
    telemetry.String("payload.type", "Order"))
defer span.End()
tel.Meter.Counter("sink.sunk").Add(ctx, 1, telemetry.String("outlet", "dynamo"))
```

A `nil` tracer or meter passed to an option is ignored, preserving the no-op
default, so call sites never need nil checks. Recording telemetry carries no
error return on the hot path: it never fails a caller's operation.

## Naming convention

Instrument and span names use **dotted lower-snake with a module prefix**, so
metrics and traces line up across the suite:

| Module  | Examples |
| ------- | -------- |
| `sink`  | `sink.sunk`, `sink.failed`, `sink.flush_latency_ms`, span `sink.Sink` |
| `state` | `state.transitions`, gauge `state.in_state`, span `state.transition` |

## Next

- [Adapters](/crucible/telemetry/adapters/): the slog, OpenTelemetry, and Datadog
  backends, each in its own optional sub-module.
