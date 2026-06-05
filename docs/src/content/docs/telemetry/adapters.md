---
title: Adapters
description: Optional sub-modules that translate the telemetry interface onto slog, OpenTelemetry, or Datadog, each pulling its own vendor SDK so the core stays stdlib-only.
sidebar:
  order: 2
---

An adapter translates the [`telemetry`](/crucible/telemetry/overview/) interface
to a concrete backend. Each one ships as a separate, optional sub-module with its
own `go.mod`, so the core never imports a vendor SDK and a deployment compiles in
only the backend it uses. Every adapter implements the same `Tracer` and `Meter`
interfaces and is wired identically through `WithTracer` and `WithMeter`.

| Adapter             | Deps                                                  |
| ------------------- | ----------------------------------------------------- |
| `telemetry/slog`    | stdlib `log/slog` only; emits spans/metrics as logs   |
| `telemetry/otel`    | the OpenTelemetry Go SDK                               |
| `telemetry/datadog` | dd-trace-go and DogStatsD (datadog-go)                |

## slog

`telemetry/slog` is the reference adapter, built on Go's `log/slog` with **zero
external dependencies**. It emits spans and metric instruments as structured log
records and proves the seam end to end, which makes it the natural choice for
development, tests, and environments where structured logs are the only
observability sink. Because `telemetry.Attr` is already `slog.Attr`, the adapter
is conversion-free: attributes pass straight to the slog handler, so
zero-allocation scalar attributes stay zero-allocation through emission.

The package name is `slog`, which collides with the standard library, so import
it under an alias:

```go
import (
    "log/slog"

    "github.com/stablekernel/crucible/telemetry"
    crucibleslog "github.com/stablekernel/crucible/telemetry/slog"
)

logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug, // span/metric records emit at DEBUG
}))

tel := telemetry.Nop().Apply(
    telemetry.WithTracer(crucibleslog.NewTracer(crucibleslog.WithLogger(logger))),
    telemetry.WithMeter(crucibleslog.NewMeter(crucibleslog.WithLogger(logger))),
)
```

Span starts, attribute updates, errors, and ends become `span.start`,
`span.attributes`, `span.error`, and `span.end` records; counters, histograms,
and gauges become `metric` records. `Tracer.Start` carries the current span id in
the returned context, so a nested span logs its parent's id, reproducing span
parentage in the logs.

## OpenTelemetry

`telemetry/otel` bridges the interface onto an OpenTelemetry `trace.Tracer` and
`metric.Meter`. The consuming module keeps its dependency on the vendor-neutral
interface; the OpenTelemetry SDK is pulled in only by this adapter's own
`go.mod`.

```go
import (
    "github.com/stablekernel/crucible/telemetry"
    oteladapter "github.com/stablekernel/crucible/telemetry/otel"
)

// tp/mp are your configured OpenTelemetry TracerProvider / MeterProvider.
tel := telemetry.Nop().Apply(
    telemetry.WithTracer(oteladapter.NewTracer(tp.Tracer("crucible"))),
    telemetry.WithMeter(oteladapter.NewMeter(mp.Meter("crucible"))),
)
```

`Tracer.Start` maps to `trace.Tracer.Start` with the context propagated for
parentage; `Span.SetStatus(OK/Error/Unset)` maps to the matching `codes` value;
counters, histograms, and gauges map to an `Int64Counter`, a `Float64Histogram`,
and a synchronous `Float64Gauge`. Each attribute is converted by reading
`slog.Value.Kind`: scalar kinds map to the matching typed attribute, `Duration`
encodes as integer nanoseconds, `Time` as RFC 3339, and anything else is
stringified. If the SDK returns an error constructing an instrument, the adapter
falls back to a no-op instrument rather than panicking, so a metric never brings
the caller down.

## Datadog

`telemetry/datadog` bridges the tracer onto dd-trace-go and the meter onto
DogStatsD (datadog-go). As with the others, the Datadog SDKs are pulled in only by
this adapter's `go.mod`.

```go
import (
    "github.com/DataDog/datadog-go/v5/statsd"
    ddtracer "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

    "github.com/stablekernel/crucible/telemetry"
    ddadapter "github.com/stablekernel/crucible/telemetry/datadog"
)

ddtracer.Start()
defer ddtracer.Stop()

client, _ := statsd.New("127.0.0.1:8125")

tel := telemetry.Nop().Apply(
    telemetry.WithTracer(ddadapter.NewTracer()),
    telemetry.WithMeter(ddadapter.NewMeter(client)),
)
```

`Tracer.Start` maps to `tracer.StartSpanFromContext` with the context propagated
for parentage; span attributes become tags via `SetTag`; a recorded error is
attached on `Finish`; `SetStatus(Error)` marks the span errored. Counters,
histograms, and gauges map to `statsd.Count`, `statsd.Histogram`, and
`statsd.Gauge`, with metric attributes rendered as DogStatsD `"key:value"` tags.
`NewTracer` uses the active global dd-trace-go tracer by default; inject a span
starter with `WithSpanStarter` to drive it from a test (for example dd-trace-go's
`mocktracer`).

## Writing your own

Any backend works the same way: implement `Tracer`/`Span` and
`Meter`/`Counter`/`Histogram`/`Gauge`, convert each attribute with a `switch` over
`attr.Value.Kind()` (reading the typed accessor for each scalar kind and
`Value.Any()` only for the `KindAny` escape hatch), and wire it through
`WithTracer`/`WithMeter`. The `slog` adapter is the smallest worked example.
