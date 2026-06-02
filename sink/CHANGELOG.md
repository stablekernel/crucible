# Changelog

All notable changes to `crucible/sink` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Fire-and-forget fan-out `Manifold`: one `Sink(ctx, payload)` call fans a
  payload out to every attached `Outlet`. Failures route to the configured
  `slog` logger and metrics; there is no synchronous join.
- `Outlet` contract with optional `Flusher`, `BatchOutlet`, and `Shutdowner`
  capabilities detected by assertion, and an `OutletFunc` adapter.
- Type-keyed `Registry` (no package globals) with the free `Register` function,
  generic `Op`/`OpFunc` operations, and a generic `Emitter` for
  bring-your-own-client destinations.
- `Reservoir` batching wrapper and `Poller` periodic sampler, both with an
  injected clock for deterministic, sleep-free tests.
- `Bucket` test outlet and `RecordsOf` typed accessor.
- Typed error model (`Error`, `Phase`, `ErrUnregistered`) that is
  `errors.Is`/`errors.As` friendly.
- Observability seams (`WithLogger`, `WithTracer`, `WithMeter`) over
  `crucible/telemetry`, emitting a `sink.Sink` span and the `sink.sunk`,
  `sink.failed`, `sink.skipped`, `sink.dropped` counters plus the
  `sink.batch_size` and `sink.flush_latency_ms` histograms.
- `sinktest.OutletConformance` harness for validating any `Outlet`.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink
