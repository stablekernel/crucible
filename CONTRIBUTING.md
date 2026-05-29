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
| `fuzz`     | a short fuzzing pass per module                       |
| `tidy`     | `go mod tidy` per module                              |
| `vuln`     | `govulncheck ./...` per module                        |
| `check`    | thorough pre-flight: `lint` + `testRace` + `vuln`     |

## What must pass

Before you push, **tests, lint, vet, and `govulncheck` must all pass.** The
quickest way to verify locally is `mage check`. CI runs the same gates across a
Go-version × OS matrix on every PR.

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

## Questions & design discussion

Design rationale lives on the
[Discussions board](https://github.com/stablekernel/crucible/discussions), not
in commit messages. Open a discussion for anything architectural before writing
a large change.
