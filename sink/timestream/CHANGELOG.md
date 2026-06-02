# Changelog

All notable changes to `crucible/sink/timestream` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Amazon Timestream sink destination: a narrow `Client` interface (`WriteRecords`,
  satisfied by `*timestreamwrite.Client`), a `WriteRecords` operation constructor,
  `NewRegistry`, and `New` building a `sink.Outlet` named "timestream".

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/timestream
