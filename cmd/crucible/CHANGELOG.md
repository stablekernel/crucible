# Changelog

All notable changes to the crucible CLI are documented here. This module is
versioned independently of the `state` module.

## 0.1.0

Initial release.

- `lint` runs static analysis over an IR and exits non-zero on findings.
- `render` emits a Mermaid or DOT diagram of a machine.
- `diff` classifies the changes between two IRs and recommends a semver bump.
- `validate` confirms an IR loads and assembles.
- `eject` generates typed Go behavior stubs from an IR.
- `version` (and `-version`) prints the CLI version.
- Commands read an IR file path or `-` for stdin.
