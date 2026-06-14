# Changelog

All notable changes to this module are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this module adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0]

### Added

- `Eject` codegen that turns a state machine's IR into typed Go stub source. From
  an `state.IR` it renders a single gofmt'd Go file containing:
  - a generated `Context` type from the IR's context schema — a struct (one field
    per schema field, with kind-to-Go mapping and `json` tags) when fields are
    declared, or a `map[string]any` alias when the schema is absent or empty;
  - a panic-bodied stub for every referenced guard, action, assign, and service,
    each typed to the exact engine signature with the generated context type
    substituted for the machine's context type parameter; and
  - a `Provide` function registering every stub against a `state.Registry` by its
    original IR name (assigns register through `Reducer`).
- Functional options `WithPackageName` and `WithContextTypeName` configuring the
  emitted package clause and context type name.
- Deterministic output: behavior names are walked across the full state hierarchy
  (states, children, regions, transitions, invocations), deduplicated, and sorted,
  so ejecting the same IR twice yields byte-identical source. A name shared across
  behavior kinds gets a unique, kind-suffixed Go identifier while its registration
  string stays the original name.

[Unreleased]: https://github.com/stablekernel/crucible/compare/gen/v0.1.0...HEAD
[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/gen/v0.1.0
