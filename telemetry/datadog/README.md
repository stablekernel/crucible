# crucible/telemetry/datadog

A [Datadog](https://www.datadoghq.com/) adapter for
[`crucible/telemetry`](../README.md). Bridges the vendor-neutral
`telemetry.Tracer` onto [dd-trace-go](https://github.com/DataDog/dd-trace-go) and
`telemetry.Meter` onto [DogStatsD](https://github.com/DataDog/datadog-go).

Import path: `github.com/stablekernel/crucible/telemetry/datadog`

## What it is

`datadog` implements the `telemetry.Tracer` and `telemetry.Meter` interfaces on
top of the Datadog Go SDKs. A consuming module keeps its dependency on the
vendor-neutral `crucible/telemetry` interface; the Datadog SDKs are pulled in only
by this adapter's own `go.mod`, so the core stays free of vendor deps.

## Usage

```go
import (
    "github.com/DataDog/datadog-go/v5/statsd"
    ddtracer "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

    "github.com/stablekernel/crucible/telemetry"
    ddadapter "github.com/stablekernel/crucible/telemetry/datadog"
)

// Start the dd-trace-go tracer in your process bootstrap.
ddtracer.Start()
defer ddtracer.Stop()

client, _ := statsd.New("127.0.0.1:8125")

tel := telemetry.Nop().Apply(
    telemetry.WithTracer(ddadapter.NewTracer()),
    telemetry.WithMeter(ddadapter.NewMeter(client)),
)
```

`NewTracer` uses the active global dd-trace-go tracer by default. Inject a span
starter with `WithSpanStarter` to drive the adapter from a test (for example
dd-trace-go's `mocktracer`).

## Mapping

| Crucible surface                | Datadog |
| ------------------------------- | ------- |
| `Tracer.Start`                  | `tracer.StartSpanFromContext` (context propagated for parentage) |
| `Span.SetAttributes`            | `Span.SetTag` per attribute |
| `Span.RecordError`              | error retained, attached on `Finish(tracer.WithError(err))` |
| `Span.SetStatus(Error, msg)`    | marks the span errored (`error.message`, `error.type` tags) |
| `Span.End`                      | `Span.Finish` (with the recorded error, if any) |
| `Meter.Counter` → `Add`         | `statsd.Count` |
| `Meter.Histogram` → `Record`    | `statsd.Histogram` |
| `Meter.Gauge` → `Record`        | `statsd.Gauge` |

### Attribute and tag conversion

Span attributes become tags via `Span.SetTag`, keeping their Go type for scalar
kinds. Metric attributes become DogStatsD `"key:value"` tag strings, rendered
from the `slog.Value` by kind. DogStatsD carries no per-instrument unit or
description, so `WithUnit`/`WithDescription` are accepted and ignored.

The DogStatsD sample rate defaults to `1.0`; override it with `WithSampleRate`.

## Pinned versions

- Tracing: `github.com/DataDog/dd-trace-go/v2` **v2.8.2**. v2 is the current
  major of dd-trace-go (the older `gopkg.in/DataDog/dd-trace-go.v1` line is
  superseded).
- Metrics: `github.com/DataDog/datadog-go/v5` **v5.8.3** (DogStatsD).

## Stability

Stability label: **experimental** (pre-v1).

## License

Apache 2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
