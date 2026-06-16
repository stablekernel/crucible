# Stability policy

Crucible is a multi-module repository. **Every module is versioned independently
on its own SemVer line and keeps its own `CHANGELOG.md`** — there is no single
global version. This document defines the stability labels used in the
[Modules table](README.md#modules); that table is the live source for which label
each module currently carries.

## Labels

| Label | What it promises |
| --- | --- |
| **stable (v1.x)** | Frozen public contract. Breaking changes only in a new major version. `state` is here. |
| **stable contract (pre-1.0)** | The module version is still `v0.x`, but a named part of its behavior is a committed contract that will not break silently. `state/expr` commits its expression *semantics* this way. |
| **released (v0.x)** | Released and usable, versioned independently, free to move at its own pace. Being pre-1.0, a minor release may still break. `gen` and `cmd/crucible` are here. |
| **advisory** | Ships inside a stable module but sits *outside* its frozen contract and may change in a minor release. The `state` subpackages (`analysis`, `evolution`, `conformance`, `verify`) are advisory. |
| **experimental** | Usable, tested, and benchmarked, but the API may change before it reaches v1. Pin a version and expect churn. The IO edges (`sink`, `source`) and host-side runtimes (`durable`, `cluster`, `transport`, `wasm`, `telemetry`) are experimental. |
| **planned** | Not yet implemented. `broker` is planned. |

## Using pre-1.0 modules

The experimental and released-pre-1.0 modules are real implementations — tested and
benchmarked, not stubs. The label is about *contract stability*, not quality: pin an
exact version, read the module's `CHANGELOG.md` before upgrading, and expect the API
to move until the module graduates to v1.

## Graduation to v1

Promoting an experimental module to v1 means committing to a frozen public contract
under the same terms as `state`. The graduation criteria and a per-module
compatibility matrix are tracked in
[issue #179](https://github.com/stablekernel/crucible/issues/179).
