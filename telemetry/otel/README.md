# crucible/telemetry/otel

An [OpenTelemetry](https://opentelemetry.io/) adapter for
[`crucible/telemetry`](../README.md). Bridges the vendor-neutral
`telemetry.Tracer` and `telemetry.Meter` onto an OpenTelemetry `trace.Tracer` and
`metric.Meter`.

Import path: `github.com/stablekernel/crucible/telemetry/otel`

## What it is

`otel` implements the `telemetry.Tracer` and `telemetry.Meter` interfaces on top
of the OpenTelemetry Go SDK. A consuming module keeps its dependency on the
vendor-neutral `crucible/telemetry` interface; the OpenTelemetry SDK is pulled in
only by this adapter's own `go.mod`, so the core stays free of vendor deps.

## Usage

```go
import (
    "go.opentelemetry.io/otel"

    "github.com/stablekernel/crucible/telemetry"
    oteladapter "github.com/stablekernel/crucible/telemetry/otel"
)

// tp/mp are your configured OpenTelemetry TracerProvider / MeterProvider.
tel := telemetry.Nop().Apply(
    telemetry.WithTracer(oteladapter.NewTracer(tp.Tracer("crucible"))),
    telemetry.WithMeter(oteladapter.NewMeter(mp.Meter("crucible"))),
)
```

## Mapping

| Crucible surface                | OpenTelemetry |
| ------------------------------- | ------------- |
| `Tracer.Start`                  | `trace.Tracer.Start` (context propagated for parentage) |
| `Span.SetAttributes`            | `span.SetAttributes` (attrs converted by `slog.Value.Kind`) |
| `Span.RecordError`              | `span.RecordError` |
| `Span.SetStatus(OK/Error/Unset, msg)` | `span.SetStatus(codes.Ok/Error/Unset, msg)` |
| `Span.End`                      | `span.End` |
| `Meter.Counter` → `Add`         | `Int64Counter.Add` |
| `Meter.Histogram` → `Record`    | `Float64Histogram.Record` |
| `Meter.Gauge` → `Record`        | synchronous `Float64Gauge.Record` |
| `WithUnit` / `WithDescription`  | `metric.WithUnit` / `metric.WithDescription` |

### Attribute conversion

Each `telemetry.Attr` (an `slog.Attr`, value an `slog.Value`) is converted to an
OpenTelemetry `attribute.KeyValue` by reading `slog.Value.Kind`: `String`,
`Int64`/`Uint64`, `Float64`, and `Bool` map to the matching typed attribute;
`Duration` is encoded as integer nanoseconds; `Time` as RFC 3339; and `Any` (or
any unrecognized kind) is stringified.

If the OpenTelemetry SDK returns an error constructing an instrument, the adapter
falls back to a no-op instrument rather than panicking, so a metric never brings
the caller down.

## Pinned versions

OpenTelemetry SDK pinned at **v1.44.0** (`otel`, `otel/trace`, `otel/metric`,
`otel/sdk`, `otel/sdk/metric`). The synchronous `Float64Gauge` used for
`Meter.Gauge` is part of the stable metric API at this version.

## Stability

Stability label: **experimental** (pre-v1).

## License

Apache 2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
