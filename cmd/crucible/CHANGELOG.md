# Changelog

All notable changes to the crucible CLI are documented here. The format is based
on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this module
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). It is
versioned independently of the `state` module.

## [Unreleased]

### Added

- Machine visualizer: `render -format svg` renders the machine directly to a
  themed, scalable SVG via the embedded D2 engine (pure Go, no Chromium, no
  external Graphviz install). The pipeline supports scope and detail projection:
  `-from`/`-to` with `-mode shortest|all|trace` select a whole / reachable-from
  / path scope, and `-detail outline|guards|actions|lifecycle|full` (default
  `actions`) sets a cumulative detail ladder, refined by repeatable
  `-show`/`-hide <dimension>`. `-o file` writes the SVG to a file instead of
  stdout.
- `render -theme file.json` overlays a JSON theme onto the embedded default
  forge palette; omitted fields keep their defaults.
- `lint -format` selects the output format: `text` (default), `json`, or
  `sarif` (SARIF 2.1.0) for machine-readable CI ingestion.
- `diff -format` selects `text` (default) or `json` output.
- `diff -exit-code` exits non-zero when the recommended bump is `major`
  (at least one breaking change), so a diff can gate CI.
- `simulate` fires a sequence of events against a machine from a given state
  and prints the step trace. `-events` takes a comma-separated list; `-events-file`
  accepts a JSON events file. `-guard name=bool` seeds a guard verdict (unseeded
  guards default to false). `-initial` overrides the IR's declared start state.
  `-format` selects `text` (default) or `json` output.

### Changed

- The `render` SVG backend now uses the embedded D2 engine
  (`oss.terrastruct.com/d2`) instead of the previous WebAssembly Graphviz
  backend. SVG output is themed with the Crucible forge palette and rendered
  in-process.

### Removed

- The previous WebAssembly Graphviz rendering dependency (and its bundled
  Graphviz engine) is removed in favor of D2.
- `render -format png` no longer renders directly; it now exits with a usage
  error hinting to render `-format svg` and convert with `resvg` or
  `rsvg-convert`.

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
