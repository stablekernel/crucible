# Changelog

All notable changes to `crucible/sink/firehose` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Firehose sink destination: a narrow `Client` interface (`PutRecord`,
  `PutRecordBatch`, satisfied structurally by `*firehose.Client` from AWS SDK
  v2), three Op constructors (`PutRecord`, `PutRecordOf`, `PutRecordBatch`),
  `ErrPartialFailure` for surfacing partial batch failures, `NewRegistry`, and
  `New` building a `sink.Outlet`.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/firehose
