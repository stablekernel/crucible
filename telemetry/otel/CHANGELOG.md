# Changelog

All notable changes to `crucible/telemetry/otel` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- OpenTelemetry adapter implementing `telemetry.Tracer` and `telemetry.Meter` on
  top of an OpenTelemetry `trace.Tracer` and `metric.Meter`, wired into a
  consumer via `telemetry.WithTracer`/`telemetry.WithMeter`.
- `Tracer.Start` propagates the span context for parentage; `Span.SetStatus`
  maps onto `codes.Ok`/`codes.Error`/`codes.Unset`; attributes convert from
  `slog.Value` by kind.
- Metric instruments backed by `Int64Counter`, `Float64Histogram`, and a
  synchronous `Float64Gauge`, honoring `WithUnit`/`WithDescription`. Instrument
  construction falls back to a no-op on SDK error rather than panicking.
- Pins the OpenTelemetry SDK at v1.44.0.
