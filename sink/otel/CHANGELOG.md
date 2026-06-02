# Changelog

All notable changes to `crucible/sink/otel` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- OpenTelemetry sink destination: a narrow `Meter` interface (`Int64Counter`,
  `Float64Histogram`, `Float64Gauge`, satisfied structurally by any
  `metric.Meter`), the `Counter`, `Gauge`, `Histogram`, and `SpanEvent`
  operation constructors, `NewRegistry`, and `New` building a `sink.Outlet`
  named "otel".

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/otel
