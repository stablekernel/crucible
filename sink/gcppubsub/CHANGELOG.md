# Changelog

All notable changes to `crucible/sink/gcppubsub` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Google Cloud Pub/Sub sink destination: a narrow `Publisher` interface
  (publish one message, block for the server-assigned ID), an `Adapt` bridge
  over `*pubsub.Publisher` from `cloud.google.com/go/pubsub/v2`, a `Publish`
  operation constructor, `NewRegistry`, and `New` building a `sink.Outlet`.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/gcppubsub
