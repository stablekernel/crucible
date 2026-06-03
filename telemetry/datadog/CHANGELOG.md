# Changelog

All notable changes to `crucible/telemetry/datadog` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-30

### Added

- Datadog adapter implementing `telemetry.Tracer` on dd-trace-go and
  `telemetry.Meter` on DogStatsD, wired into a consumer via
  `telemetry.WithTracer`/`telemetry.WithMeter`.
- `Tracer.Start` starts a span from the context for parentage; attributes become
  span tags; `Span.RecordError` and `Span.SetStatus(Error, ...)` mark the span
  errored and attach the error on `Finish`. The span starter is injectable
  (`WithSpanStarter`) for testing with `mocktracer`.
- Metric instruments emit `statsd.Count`/`Histogram`/`Gauge`, converting
  attributes to `"key:value"` DogStatsD tags, with a configurable sample rate
  (`WithSampleRate`).
- Pins `dd-trace-go/v2` v2.8.2 and `datadog-go/v5` v5.8.3.

[Unreleased]: https://github.com/stablekernel/crucible/compare/telemetry/datadog/v0.1.0...HEAD
[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/telemetry/datadog/v0.1.0
