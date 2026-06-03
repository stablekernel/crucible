# Contributing to Crucible

Thanks for your interest in contributing. This document covers everything you
need to get set up and land a change.

By participating you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).

## Engineering standards

All contributions are held to the suite-wide standards below, the authoritative
baseline every module is built to. Module-specific design lives in each module's
own docs and the [documentation site](https://stablekernel.github.io/crucible/).

The through-line is **thin seams, no-op defaults, no forced dependencies**. Every
cross-cutting concern (logging, tracing, metrics, IDs, time) is a small,
consumer-providable interface with a do-nothing default. Zero configuration means
silent behavior, zero overhead, and zero extra imports; a consumer brings their
logger, their tracer, their clock, and we never make them adopt ours. The pure
kernel (`crucible/state`) is the extreme end of this: stdlib-only, no injected IO
at all. The IO modules carry the heavier seams via injection but follow the same
rule. The test for any new API: does it force the consumer to adopt our choice, or
let them bring their own? If it forces, redesign it as a seam with a no-op default.

### API design & compatibility

- **Functional options everywhere.** Every public constructor and operation takes
  a variadic `...XxxOption` tail. Required inputs stay positional; everything
  optional or extensible is an option; a zero-option call reads clean. New
  capability arrives as a new option: additive-only, never a breaking signature
  change. Never hide a required input behind an option.
- **Per-module SemVer** with a written compatibility policy, plus a **stability
  label** (experimental / beta / stable) declared in each module's README and
  godoc. The label sets the compatibility promise.
- **Context-first.** Every IO or cancelable operation takes `context.Context` as
  its first parameter.
- **Zero global state.** No package-level mutable state, no `init()` side effects.
  Everything is constructed explicitly and injected.
- **Never leak third-party types** in public signatures. Public APIs speak stdlib
  types or Crucible's own types only.
- **Typed errors.** Return typed, wrapped errors (`%w`, `errors.Is` / `errors.As`
  friendly). No string-matching contracts.

### Observability

- **Logging via `log/slog`.** Consumers pass their own `*slog.Logger`; the default
  is a no-op. Severity maps onto slog's integer levels, extending the standard four
  to a six-level scale: TRACE (-8), DEBUG (-4), INFO (0), WARN (+4), ERR (+8),
  and FATAL (+12).
- **A library never exits the process.** Crucible code never calls `os.Exit`,
  `log.Fatal`, or panics on an operational error. FATAL is a severity, not an
  action: log at FATAL severity (if a logger is provided) and return the error;
  the consumer decides whether to exit. Panic is reserved strictly for programmer
  error at construction time, never for runtime conditions.
- **Vendor-neutral telemetry.** Crucible defines its own minimal tracing/metrics
  interface with zero third-party imports and a no-op default. Adapters ship as
  separate, optional sub-modules (`telemetry/otel`, `telemetry/slog`,
  `telemetry/datadog`) so the core never imports a vendor SDK.

### Determinism

- Modules never call `time.Now()` or `rand` directly. Both are injected seams with
  real defaults: `WithClock(Clock)` for the time source and `WithIDFn(func() string)`
  for the identifier generator. This is what makes `after`/delayed transitions,
  reproducible `Trace` output, and flake-free unit tests possible.

### Lifecycle

- Stateful modules (anything holding connections, buffers, or goroutines) expose a
  graceful shutdown surface: `Shutdown(ctx context.Context) error` and/or
  `io.Closer`. Shutdown drains in-flight work within the context deadline rather
  than dropping it. The pure kernel is stateless and needs no lifecycle surface.

### Testing

- **Layered suites** (unit + integration + e2e, each in its own scope) run under
  `-race` in CI.
- **Native fuzzing** (`testing.F`) for parsers and the IR round-trip, plus
  property-based tests for invariants.
- **Golden files** for serialized output (machine IR, Mermaid, DOT), diffed in CI.
- **Example tests** double as godoc and run in CI, so docs can't drift from behavior.
- **Benchmarks + a `benchstat` regression gate** fail the build on a regression,
  and a **coverage threshold** is enforced per module.
- The **conformance harness** ships as a reusable exported package so downstream
  consumers can prove their own machines correct with the same tooling.

### Build & tooling

- Standardized **magefiles** across modules (build, test, lint, cover, bench, fuzz,
  tidy, release).
- One shared **golangci-lint** config plus **gofumpt**, **staticcheck**, and
  **govulncheck**, all in CI, with **pinned tool versions** so every contributor
  and CI run uses the same toolchain.
- **Trunk-based development** with a CI matrix over Go versions × OS, and
  **conventional + signed commits**.

### Supply chain

- **Dependency minimalism.** The kernel being stdlib-only is a security feature,
  not just an aesthetic one. IO modules keep their dependency sets small and
  justified.
- **govulncheck** in CI and **Dependabot** for dependency updates.

## Development setup

Requirements:

- **Go**: one of the last two minor releases (see [Supported versions](./SECURITY.md)).
- **[Mage](https://magefile.org)**: the build tool. Install with
  `go install github.com/magefile/mage@latest`, or run targets via
  `go run github.com/magefile/mage <target>`.

This repo is a Go workspace (`go.work`) spanning multiple modules. The build
automation lives in its own module under `magefiles/` so its dependencies never
leak into the library modules.

## Mage targets

Run `mage -l` to list everything. Each target iterates the suite's modules.

| Target     | What it does                                          |
| ---------- | ----------------------------------------------------- |
| `build`    | `go build ./...` per module                           |
| `test`     | `go test ./...` per module                            |
| `testRace` | `go test -race ./...` per module                      |
| `lint`     | `golangci-lint run` with the shared config per module |
| `cover`    | tests with a coverage profile per module              |
| `bench`    | benchmarks per module                                 |
| `benchCompare` | benches the working tree vs a base ref (the CI gate, locally) |
| `fuzz`     | a short fuzzing pass per module                       |
| `tidy`     | `go mod tidy` per module                              |
| `vuln`     | `govulncheck ./...` per module                        |
| `check`    | thorough pre-flight: `lint` + `testRace` + `vuln`     |

## What must pass

Before you push, **tests, lint, vet, and `govulncheck` must all pass.** The
quickest way to verify locally is `mage check`. CI runs the same gates across a
Go-version × OS matrix on every PR.

Docs-only pull requests (changing only `*.md`, `docs/`, `LICENSE`, or `NOTICE`)
skip the Go matrix automatically and are gated solely by the aggregate `gate`
check, so a README tweak doesn't spend the full test run.

## Performance

Performance regressions fail the build. On every pull request, CI runs the
`state` benchmarks on **both your branch head and the PR base, on the same
runner**, then `benchstat`-diffs the two. Running both refs on one machine
cancels machine-to-machine variance, so a genuine regression stands apart from
runner jitter. The comparison table is written to the job's step summary.

The gate fails if any benchmark's **time/op (`sec/op`) or `allocs/op`** regresses
past a head/base ratio of **1.20 (a 20% slowdown)**. That threshold is
deliberately generous to absorb shared-runner noise on micro-benchmarks; it is a
single, clearly-commented constant in `.github/workflows/ci.yml`
(`BENCH_THRESHOLD`) and `.github/scripts/bench-gate.awk`, easy to tighten as the
benchmark history on CI stabilizes. `B/op` is reported for context but not gated.
New benchmarks (with no counterpart on the base) and removed ones never fail the
gate.

Reproduce the exact comparison locally before you push:

```sh
mage benchCompare            # working tree vs origin/main
mage benchCompare v0.1.0     # working tree vs an explicit ref (tag/branch/SHA)
```

It benches the base ref in a throwaway git worktree (your working tree is left
untouched), prints the `benchstat` table, and exits non-zero on a regression,
the same verdict CI produces.

## Commits

- **Conventional commits**: `type: subject` (e.g. `feat: add IR round-trip`).
  Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`.
- **DCO sign-off** is required on every commit; there is **no CLA**. Add the
  `Signed-off-by` trailer with `git commit -s`. By signing off you certify the
  [Developer Certificate of Origin](https://developercertificate.org/).
- Commits should be **signed** (`git commit -S`) where possible.

## Branch & PR flow

Crucible uses trunk-based development.

1. Branch off the default branch: `<type>/<short-description>`.
2. Make focused commits; keep the history readable.
3. Run `mage check` and ensure it is green.
4. Open a PR, fill out the template, and link any related issue.
5. A maintainer (`@stablekernel/crucible-maintainers`) reviews and merges.

## Cutting a module release

Each module versions independently and ships its own `CHANGELOG.md`. A release
is a tag push; the `Release` workflow does the rest.

1. **Decide the version bump.** Diff the change against the last released
   definition. For a state machine, the `state/evolution` package classifies the
   diff per the [Evolution Guide](https://stablekernel.github.io/crucible/analysis/evolution/)
   and recommends the bump:

   ```go
   report, _ := evolution.DiffJSON[State, Event, *Entity](goldenBytes, currentBytes)
   switch report.SemverBump() {
   case evolution.Major: // a breaking change; follow the deprecation lifecycle first
   case evolution.Minor: // additive only
   case evolution.Patch: // no schema change
   }
   ```

   Breaking changes (`report.Breaking()`) require the full Deprecated → Removed
   lifecycle from the Evolution Guide before the old definition is removed.

2. **Update the module's `CHANGELOG.md`.** Move the `Unreleased` entries under a
   new `vX.Y.Z` heading with the date, and refresh the compare links.

3. **Tag and push.** Module tags are `module/vX.Y.Z` (e.g. `state/v0.2.0`); a
   bare `vX.Y.Z` tag releases the primary module (`state`).

   ```sh
   git tag -s state/v0.2.0 -m "state v0.2.0"
   git push origin state/v0.2.0
   ```

4. **The workflow takes over.** On the tag push, `Release` re-runs the full
   validation gate (lint, `-race` tests, `govulncheck`, coverage threshold) for
   the tagged module across the Go × OS matrix, then publishes a GitHub release
   with generated notes. A tag never publishes unless the gate is green.

## Questions & design

Design rationale and guides live on the
[documentation site](https://stablekernel.github.io/crucible/). Start with the
[suite overview](https://stablekernel.github.io/crucible/about/overview/). For
questions or to float an architectural change before writing a large PR, open a
GitHub issue.
