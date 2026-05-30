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

- Arbitrarily nested superstates in the builder DSL. A `SuperState` block may now
  contain another `SuperState` block (and so on, to any depth), so a deep
  hierarchy can be authored entirely through the chained DSL rather than only via
  the IR/`Provide` path. The entry cascade descends through every level to the
  deepest initial leaf, the exit cascade unwinds innermost-first across all
  levels, child-first event resolution bubbles up through every ancestor, and a
  nested compound's `done` event propagates upward as each level completes.
  Deep history authored via the DSL now restores the full nested leaf
  configuration, and the IR round-trips losslessly at arbitrary depth. The
  remaining superstate lints (a compound with substates needs an `Initial`,
  unclosed blocks, etc.) are unchanged.
- History pseudo-states (shallow and deep). A history pseudo-state belongs to a
  compound state and remembers that compound's last active configuration;
  transitioning to it re-enters the remembered configuration instead of the
  compound's initial child. Shallow restores the last active direct child; deep
  restores the full nested leaf configuration. With no recorded history the
  resolver falls back to the history state's declared default target, else the
  compound's initial. Declared via `Builder.History(name, HistoryShallow|
  HistoryDeep)` with optional `Builder.DefaultTo(target)`. The recorded
  per-compound configuration is per-instance runtime state threaded through
  `Fire` (which stays pure); the pseudo-states themselves serialize, so machines
  with history round-trip losslessly through the IR. A Quench lint flags a
  history state declared outside a compound state.
- `state/analysis` package: static model-checking over a machine's IR. `Analyze`
  returns a classified `Report` of `Finding`s covering reachability
  (unreachable/dead states), dead transitions, guardless nondeterminism,
  non-final dead ends, and liveness (states that can never reach a final state).
  Reachability reuses the kernel's breadth-first graph walk; checks are exact
  where the IR proves them and heuristic where opaque guards limit static
  certainty. Restrict the pass with the `Only`/`Without` options.
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
