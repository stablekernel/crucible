# Changelog

All notable changes to `crucible/telemetry` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Vendor-neutral telemetry interface (`Tracer`, `Span`, `Meter`, `Counter`,
  `Histogram`, `Attr`) with zero third-party imports and a no-op default, so a
  core module depends only on this interface and never on a vendor SDK.

[Unreleased]: https://github.com/stablekernel/crucible/commits/main/telemetry
