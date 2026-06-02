# Changelog

All notable changes to `crucible/sink/eventbridge` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Amazon EventBridge sink destination: a narrow `Client` interface (`PutEvents`,
  satisfied by `*eventbridge.Client` from the AWS SDK v2), two Op constructors
  (`PutEvent` for single-entry convenience, `PutEvents` for full input control),
  partial-failure detection on `PutEventsOutput.FailedEntryCount`, `NewRegistry`,
  and `New` building a `sink.Outlet` named "eventbridge".

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/eventbridge
