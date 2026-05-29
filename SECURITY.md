# Security Policy

## Supported versions

Crucible follows Go's own support window. Security fixes are provided for:

- The **latest released version** of each module, and
- Versions that build against the **last two minor releases of Go**.

Older versions and versions built against unsupported Go releases do not receive
security updates.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

Instead, report privately using GitHub's
[private vulnerability reporting](https://github.com/stablekernel/crucible/security/advisories/new)
for this repository. This routes the report directly to the maintainers and
keeps details confidential until a fix is available.

When reporting, please include:

- The affected module(s) and version(s).
- A description of the vulnerability and its impact.
- Steps to reproduce, or a proof-of-concept, if available.

## Disclosure process

1. We acknowledge your report and begin investigation.
2. We confirm the issue, determine affected versions, and prepare a fix.
3. We release a patched version and publish a security advisory crediting the
   reporter (unless anonymity is requested).
4. We coordinate public disclosure timing with the reporter where appropriate.

## Supply chain

The pure kernel (`state`) is **stdlib-only** by design — a tiny dependency graph
is a tiny attack surface. IO modules keep their dependency sets small and
justified. We run `govulncheck` in CI, use Dependabot for dependency updates,
and treat release provenance as a first-class concern.
