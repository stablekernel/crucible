# Changelog

All notable changes to `crucible/sink/kafka` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Kafka sink destination: a narrow `Producer` interface (one `Produce` method),
  `NewProducer` adapting a franz-go `*kgo.Client` onto it via `ProduceSync`, a
  `Produce` operation constructor, `NewRegistry`, and `New` building a
  `sink.Outlet`. The broker SDK stays out of every exported signature.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/kafka
