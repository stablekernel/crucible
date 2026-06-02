# Changelog

All notable changes to `crucible/sink/statsd` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- StatsD sink destination built on `github.com/DataDog/datadog-go/v5`. A narrow
  `Client` interface (`Count`, `Gauge`, `Histogram`, `Distribution`, `Timing`,
  `Set`), satisfied structurally by the SDK client.
- `Aggregator` (the primary surface) implementing `sink.Outlet`,
  `sink.Flusher`, and `sink.Shutdowner`: folds counters (summed) and gauges
  (last write wins) by name and sorted tags, buffers histograms, distributions,
  timings, and sets as raw samples, and emits on an injected-clock interval and
  on `Flush`/`Shutdown` with an atomic buffer swap. Built with `NewAggregator`
  and the `WithName`, `WithRegistry`, `WithInterval`, and `WithClock` options.
- `Metric` payload type with a `Type` field (`TypeCount`, `TypeGauge`,
  `TypeHistogram`, `TypeDistribution`, `TypeTiming`, `TypeSet`), `NewMetricRegistry`,
  and a `Dial` convenience constructor for the SDK client.
- Emitter path: `Count`/`Gauge`/`Histogram`/`Distribution`/`Timing`/`Set` `Op`
  constructors, `NewRegistry`, and `New` building a `sink.Outlet` that emits one
  StatsD call per `Sink` with no aggregation.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/statsd
