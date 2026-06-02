# Changelog

All notable changes to `crucible/sink/sns` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Amazon SNS sink destination: a narrow `Client` interface (`Publish`,
  `PublishBatch`, satisfied by `*sns.Client` from the AWS SDK v2), three Op
  constructors (`Publish`, `PublishInput`, `PublishBatch`), `NewRegistry`, and
  `New` building a `sink.Outlet` named "sns".

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/sns
