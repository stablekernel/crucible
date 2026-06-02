# Changelog

All notable changes to `crucible/sink/cloudwatch` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Amazon CloudWatch Logs sink destination: a narrow `Client` interface
  (`PutLogEvents`, satisfied by `*cloudwatchlogs.Client` from the AWS SDK v2),
  two Op constructors (`PutLogEvent`, `PutLogEvents`), `NewRegistry`, and `New`
  building a `sink.Outlet` named "cloudwatch".

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/cloudwatch
