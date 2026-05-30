# Changelog

All notable changes to `crucible/telemetry` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-30

### Added

- Vendor-neutral telemetry interface (`Tracer`, `Span`, `Meter`, `Counter`,
  `Histogram`, `Gauge`, `Attr`) with zero third-party imports and a no-op
  default, so a core module depends only on this interface and never on a vendor
  SDK.
- Attributes backed by the standard library's `slog.Value`, giving type-safe,
  zero-allocation scalar attributes (`Any` is the opt-in boxing escape hatch).
- A `slog` adapter (`telemetry/slog`) that emits spans and metrics as
  structured logs with no conversion.

[Unreleased]: https://github.com/stablekernel/crucible/compare/telemetry/v0.1.0...HEAD
[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/telemetry/v0.1.0
