# Changelog

All notable changes to `crucible/sink/nats` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- NATS sink destination: a narrow `Client` interface (`Publish` and `PublishMsg`,
  satisfied by `*nats.Conn`), `Publish` and `PublishMsg` operation constructors,
  `NewRegistry`, and `New` building a `sink.Outlet` backed by
  `github.com/nats-io/nats.go`.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/nats
