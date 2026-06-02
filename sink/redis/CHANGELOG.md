# Changelog

All notable changes to `crucible/sink/redis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Redis sink destination: a narrow `Client` interface (`XAdd`, `Publish`; satisfied
  by `*redis.Client` from `github.com/redis/go-redis/v9`), an `XAdd` operation
  constructor for Redis Streams, a `Publish` constructor for Redis Pub/Sub,
  `NewRegistry`, and `New` building a `sink.Outlet` named `"redis"`.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/redis
