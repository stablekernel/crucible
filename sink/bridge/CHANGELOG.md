# Changelog

All notable changes to `crucible/sink/bridge` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Middleware` adapter: a `state.Middleware` that fans every successful
  transition out through a `sink.Manifold`, starting a `state.transition` span
  and propagating its context so the emit span nests beneath it.
- `Inspector` adapter: a `state.InspectorFunc` that fans transition events out
  through a `sink.Manifold` (no context propagation; see the README note on the
  observer seam).
- `Transition` payload type and `WithTracer`/`WithSpanName` options.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/sink/bridge
