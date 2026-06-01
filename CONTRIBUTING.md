# Contributing to Crucible

Thanks for your interest in contributing. This document covers everything you
need to get set up and land a change.

By participating you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).

## Engineering standards

All contributions are held to the suite-wide
[Crucible Engineering Standards](https://github.com/stablekernel/crucible/discussions/9).
Read it before opening a PR — it is the authoritative baseline for API design,
observability, determinism, testing, and supply-chain practices. Module-specific
design lives in each module's own docs and discussion threads.

## Development setup

Requirements:

- **Go** — one of the last two minor releases (see [Supported versions](./SECURITY.md)).
- **[Mage](https://magefile.org)** — the build tool. Install with
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
untouched), prints the `benchstat` table, and exits non-zero on a regression —
the same verdict CI produces.

## Commits

- **Conventional commits**: `type: subject` (e.g. `feat: add IR round-trip`).
  Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `ci`.
- **DCO sign-off** is required on every commit — there is **no CLA**. Add the
  `Signed-off-by` trailer with `git commit -s`. By signing off you certify the
  [Developer Certificate of Origin](https://developercertificate.org/).
- Commits should be **signed** (`git commit -S`) where possible.

## Branch & PR flow

Crucible uses trunk-based development.

1. Branch off the default branch: `<type>/<short-description>`.
2. Make focused commits; keep the history readable.
3. Run `mage check` and ensure it is green.
4. Open a PR, fill out the template, and link any relevant discussion or issue.
5. A maintainer (`@stablekernel/crucible-maintainers`) reviews and merges.

## Cutting a module release

Each module versions independently and ships its own `CHANGELOG.md`. A release
is a tag push; the `Release` workflow does the rest.

1. **Decide the version bump.** Diff the change against the last released
   definition. For a state machine, the `state/evolution` package classifies the
   diff per the [Evolution Guide](https://github.com/stablekernel/crucible/discussions/6)
   and recommends the bump:

   ```go
   report, _ := evolution.DiffJSON[State, Event, *Entity](goldenBytes, currentBytes)
   switch report.SemverBump() {
   case evolution.Major: // a breaking change — follow the deprecation lifecycle first
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

## Questions & design discussion

Design rationale lives on the
[Discussions board](https://github.com/stablekernel/crucible/discussions), not
in commit messages. Open a discussion for anything architectural before writing
a large change.
