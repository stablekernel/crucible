# Changelog

All notable changes to `crucible/sink/prometheus` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Prometheus Pushgateway sink destination: a narrow `Doer` interface
  (`Do(*http.Request) (*http.Response, error)`, satisfied by `*http.Client`),
  a `Push` operation constructor for pre-formatted text exposition bodies, a
  `PushMetrics` constructor that serializes a `[]Metric` slice, `NewRegistry`,
  and `New` building a `sink.Outlet` with no Prometheus client dependency.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/prometheus
