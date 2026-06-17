# Changelog

All notable changes to the crucible CLI are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this module
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). It is
versioned independently of the `state` module.

## [Unreleased]

### Added

- `lint -format` selects the output format: `text` (default), `json`, or
  `sarif` (SARIF 2.1.0) for machine-readable CI ingestion.
- `diff -format` selects `text` (default) or `json` output.
- `diff -exit-code` exits non-zero when the recommended bump is `major`
  (at least one breaking change), so a diff can gate CI.

## [0.1.0] - 2026-06-13

Initial release.

### Added

- `lint` runs static analysis over an IR and exits non-zero on findings.
- `render` emits a Mermaid or DOT diagram of a machine.
- `diff` classifies the changes between two IRs and recommends a semver bump.
- `validate` confirms an IR loads and assembles.
- `eject` generates typed Go behavior stubs from an IR.
- `version` (and `-version`) prints the CLI version.
- Commands read an IR file path or `-` for stdin.

[Unreleased]: https://github.com/stablekernel/crucible/compare/cmd/crucible/v0.1.0...HEAD
[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/cmd/crucible/v0.1.0
