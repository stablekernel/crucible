# crucible/telemetry

The [Crucible](../README.md) suite's vendor-neutral tracing and metrics
interface.

Import path: `github.com/stablekernel/crucible/telemetry`

## What it is

`telemetry` is a small, stable set of interfaces (`Tracer`/`Span`,
`Meter`/`Counter`/`Histogram`/`Gauge`, and the `Attr` attribute type) that the
suite's IO modules (`sink`, `broker`, `store`) depend on for observability.

It imports **only the Go standard library** and forces **no vendor SDK** on any
consumer. That is the whole point: a consumer brings their own tracing/metrics
backend through a thin adapter, and a consumer that brings nothing gets silent,
zero-overhead behavior from the built-in no-op defaults.

This is the suite's "thin seams, no-op defaults, no forced dependencies" rule
applied to telemetry: the core interface forces no dependency, the default does
nothing, and vendor wiring lives in optional, separately-versioned adapter
sub-modules.

## The interface

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

// Attributes: Attr is an alias for the stdlib's slog.Attr
type Attr = slog.Attr
telemetry.String(k, v) / Int64 / Int / Uint64 / Float64 / Bool / Duration / Time
telemetry.Any(k, v)                     // explicit boxing escape hatch

// Instrument metadata
WithUnit(unit) / WithDescription(desc)  // InstrumentOption
```

- **Counters** are monotonic `int64` deltas. Every counted thing (records sunk,
  failures, drops) is whole-numbered, which keeps adapter mappings exact.
- **Histograms** and **gauges** carry `float64` samples so sub-unit measurements
  (fractional milliseconds) are not truncated.
- **`Attr` is `slog.Attr`**: its value is a `slog.Value`, the stdlib's
  allocation-optimized tagged union. Build attributes with the typed constructors
  (`telemetry.String`, `Int64`, `Float64`, `Bool`, ...). The scalar constructors
  are **zero-allocation** (the value is stored inline, never boxed) and
  type-safe: adapters read `Value.Kind` plus the typed accessors. `telemetry.Any`
  is the documented escape hatch for an arbitrary value; it boxes into an
  interface and so **allocates**, so reach for it only when no typed constructor
  fits. All of this stays within the standard library.

## Context propagation is the seam

`Tracer.Start` returns a context carrying the new span. Propagate that context
into nested work and a downstream span (in this module or another) parents
under the caller's span automatically. There is no shared global tracer and no
package-level state; context is the only coupling.

## No-op default

`NopTracer()` and `NopMeter()` record nothing, allocate nothing per call, never
panic, and are safe to call concurrently and after a span has ended.

## Injecting your backend

A consuming module embeds a `Provider`, seeds it with `Nop()`, and exposes
`WithTracer`/`WithMeter` options:

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

A `nil` tracer/meter passed to an option is ignored, preserving the no-op
default, so call sites never need nil checks.

## Naming convention

Instrument and span names use **dotted lower-snake with a module prefix**:

| Module  | Examples |
| ------- | -------- |
| `sink`  | `sink.sunk`, `sink.failed`, `sink.skipped`, `sink.dropped`, `sink.batch_size`, `sink.flush_latency_ms`, span `sink.Sink` |
| `state` | `state.transitions`, gauge `state.in_state`, span `state.transition` |

Every module follows this so metrics and traces line up across the suite.

## Adapters

Adapters translate this interface to a concrete backend and ship as separate,
optional sub-modules so the core never imports a vendor SDK.

| Adapter                | Status   | Deps |
| ---------------------- | -------- | ---- |
| [`telemetry/slog`](slog/README.md) | shipped  | stdlib `log/slog` only; emits spans/metrics as structured logs |
| [`telemetry/otel`](otel/README.md) | shipped  | OpenTelemetry SDK (in its own `go.mod`) |
| [`telemetry/datadog`](datadog/README.md) | shipped  | `dd-trace-go` / `datadog-go` (in their own `go.mod`) |

Each adapter implements the same interfaces and is wired by a consumer via
`WithTracer`/`WithMeter` exactly like the `slog` adapter. `Span.SetStatus` maps to the
otel status code / Datadog error flag; `ResolveInstrument` exposes the
unit/description an adapter needs to construct its backend instrument. Attributes
are converted with a `switch` over `attr.Value.Kind()` (the `slog.Value` kind),
reading the typed accessor for each scalar kind and `Value.Any()` only for the
`KindAny` escape hatch. The `slog` adapter is conversion-free because `Attr` is
already `slog.Attr`.

## Stability

Stability label: **experimental** (pre-v1).

## License

Apache 2.0. See [LICENSE](../LICENSE) and [NOTICE](../NOTICE).
