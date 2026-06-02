# Changelog

All notable changes to `crucible/sink/kinesis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Kinesis sink destination: a narrow `Client` interface (`PutRecord`,
  `PutRecords`, satisfied structurally by `*kinesis.Client` from AWS SDK v2),
  four Op constructors (`PutRecord`, `PutRecordOf`, `PutRecords`,
  `PutRecordsOf`), `NewRegistry`, and `New` building a `sink.Outlet`.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/kinesis
