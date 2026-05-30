# Changelog

All notable changes to `crucible/state` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
A machine definition is treated as a schema: see the
[Evolution Guide](https://github.com/stablekernel/crucible/discussions/6) for what
counts as an additive (minor) versus breaking (major) change. Use the
`state/evolution` package to classify a machine change and decide the bump.

## [Unreleased]

### Added

- `state/evolution` package: classifies the difference between two machine
  definitions as additive or breaking per the Evolution Guide, and maps the
  result onto a semantic-version bump (`Diff`, `DiffJSON`, `DiffMachines`,
  `Report.Breaking`, `Report.SemverBump`).

## [0.1.0]

Initial release of the pure state-machine kernel.

### Added

- Kernel core: `Forge`/`Temper`/`Quench`/`Cast`/`Fire`/`Assay` foundry API with
  pure-function step semantics — firing an event returns `(newState, effects,
  trace)` with no IO.
- Serializable definition IR with lossless JSON round-trip; guards, actions, and
  effects are named references bound to a host-provided registry.
- Hierarchical (compound) and parallel (orthogonal-region) states.
- Path planning (`PlanPath`) over the machine graph, honoring guards.
- Mermaid and DOT export for state machines.
- Trace-first observability, functional options throughout, and injected
  clock/ID seams for determinism.
- Reusable conformance harness with golden scenarios.

[Unreleased]: https://github.com/stablekernel/crucible/compare/state/v0.1.0...HEAD
[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/state/v0.1.0
