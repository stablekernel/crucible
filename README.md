# Crucible

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)

**Forge event-driven services in Go.**

Crucible is a multi-module Go toolkit for building event-driven services. Its
design philosophy is **thin seams, no-op defaults, no forced dependencies**:
every cross-cutting concern (logging, tracing, metrics, IDs, time) is a small,
consumer-providable interface with a do-nothing default. You bring *your*
logger, *your* tracer, *your* clock — Crucible never makes you adopt its
choices, and never leaks a third-party type into a public signature.

The pure kernel (`state`) is the extreme end of this: **stdlib-only**, with no
injected IO at all. The IO modules carry the heavier seams via injection, but
follow the same rule — defaults are no-ops, nothing third-party is forced on the
consumer.

## Modules

Each module is independently versioned (per-module SemVer) and carries its own
stability label.

| Module    | What it is                                                                 | Status      |
| --------- | -------------------------------------------------------------------------- | ----------- |
| `state`     | Pure, abstract, domain-agnostic state machine kernel. Stdlib-only, no IO.  | experimental |
| `telemetry` | Vendor-neutral tracing/metrics interface for the IO modules. Stdlib-only.  | experimental |
| `broker`  | Message broker seam — publish/subscribe transport with injected adapters.  | planned     |
| `store`   | Durable state/event store seam with graceful lifecycle.                    | planned     |
| `sink`    | Effect dispatch / egress seam for emitted effects.                         | planned     |

The kernel emits effects as pure data; the IO modules are the thin seams that
carry those effects to real transports, stores, and sinks — each
"bring your own adapter," none forced on the consumer.

## Status

Early and evolving. The `state` kernel is implemented — the builder and
transition engine, guards and actions, validation, path planning, batch
helpers, and JSON (de)serialization — with test coverage; treat its API as
experimental until a tagged release. The `broker`, `store`, and `sink` modules
are planned.

## Design & discussions

Design rationale and roadmaps live on the GitHub
[Discussions board](https://github.com/stablekernel/crucible/discussions),
organized into the **State Machine** and **Conventions** categories. Start with:

- [Crucible Engineering Standards](https://github.com/stablekernel/crucible/discussions/9)
  — the suite-wide baseline every module is held to.
- [State Machine — Overview & Roadmap](https://github.com/stablekernel/crucible/discussions/1)
- [State Machine — Kernel Core](https://github.com/stablekernel/crucible/discussions/2)

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md) for dev
setup, the [Mage](https://magefile.org) targets, conventional commits, and the
DCO sign-off requirement. By participating you agree to the
[Code of Conduct](./CODE_OF_CONDUCT.md).

## License

Licensed under the [Apache License, Version 2.0](./LICENSE). See
[NOTICE](./NOTICE) for attribution.
