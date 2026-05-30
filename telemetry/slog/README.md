# crucible/telemetry/slog

A standard-library `log/slog` adapter for
[`crucible/telemetry`](../README.md). Emits spans and metric instruments as
structured log records, with **zero external dependencies**.

Import path: `github.com/stablekernel/crucible/telemetry/slog`

## What it is

This adapter implements the `telemetry.Tracer` and `telemetry.Meter` interfaces
on top of Go's `log/slog`. It is the reference adapter — it proves the telemetry
seam end to end without pulling in any vendor SDK — and is useful for
development, tests, and environments where structured logs are the only
observability sink.

Because `telemetry.Attr` is an alias for `slog.Attr`, the adapter is
**conversion-free**: attributes pass straight to the slog handler with no
re-boxing, so the zero-allocation scalar attributes stay zero-allocation through
emission.

For production tracing/metrics backends, use (or write) an otel or datadog
adapter against the same interfaces.

## Usage

The package name is `slog`, which collides with the standard library's
`log/slog`. Import it under an alias (e.g. `crucibleslog`) in any file that also
imports `log/slog`:

```go
import (
    "log/slog"

    "github.com/stablekernel/crucible/telemetry"
    crucibleslog "github.com/stablekernel/crucible/telemetry/slog"
)

logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug, // span/metric records are emitted at DEBUG
}))

tel := telemetry.Nop().Apply(
    telemetry.WithTracer(crucibleslog.NewTracer(crucibleslog.WithLogger(logger))),
    telemetry.WithMeter(crucibleslog.NewMeter(crucibleslog.WithLogger(logger))),
)
```

## Emission shape

| Surface                | Record (`msg`)    | Level | Key fields |
| ---------------------- | ----------------- | ----- | ---------- |
| `Tracer.Start`         | `span.start`      | DEBUG | `span.name`, `span.id`, `span.parent`, `span.attrs.*` |
| `Span.SetAttributes`   | `span.attributes` | DEBUG | `span.name`, `span.id`, `span.attrs.*` |
| `Span.RecordError`     | `span.error`      | ERROR | `span.name`, `span.id`, `span.error` |
| `Span.End`             | `span.end`        | DEBUG | `span.name`, `span.id`, `span.status`, `span.elapsed`, `span.status_msg` |
| Counter/Histogram/Gauge | `metric`         | DEBUG | `metric.name`, `metric.kind`, `metric.value`, `metric.unit`, `metric.attrs.*` |

`Tracer.Start` carries the current span id in the returned context, so a nested
span logs its parent's id — reproducing span parentage in the logs.

## Options

| Option           | Default                        | Purpose |
| ---------------- | ------------------------------ | ------- |
| `WithLogger(l)`  | `slog.New(slog.DiscardHandler)` | the slog logger to emit to (silent until set) |
| `WithClock(now)` | `time.Now`                     | time source for span durations (inject for deterministic tests) |
| `WithIDFn(next)` | internal atomic counter        | monotonic span-id generator (inject for deterministic tests) |

## Stability

Stability label: **experimental** (pre-v1).

## License

Apache 2.0 — see [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
