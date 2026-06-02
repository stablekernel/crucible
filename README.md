# Crucible

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)

**Forge event-driven services in Go.**

Crucible is a multi-module Go toolkit for building event-driven services. Its
design philosophy is **thin seams, no-op defaults, no forced dependencies**:
every cross-cutting concern (logging, tracing, metrics, IDs, time) is a small,
consumer-providable interface with a do-nothing default. You bring *your*
logger, *your* tracer, *your* clock — Crucible never makes you adopt its
choices, and never leaks a third-party type into a public signature.

The `state` engine is the extreme end of this: **stdlib-only**, with no injected
IO at all. The IO modules carry the heavier seams via injection, but follow the
same rule — defaults are no-ops, nothing third-party is forced on the consumer.

## Documentation

Guides, concepts, the food-delivery example, and the generated API reference live
in the documentation site:

### 👉 **[stablekernel.github.io/crucible](https://stablekernel.github.io/crucible/)**

## Modules

Each module is independently versioned (per-module SemVer) and carries its own
stability label.

| Module                | What it is                                                                       | Status       |
| --------------------- | -------------------------------------------------------------------------------- | ------------ |
| `state`               | Full-featured, domain-agnostic statechart engine. Stdlib-only, no IO.            | experimental |
| `state/analysis`      | Static model-checking and path enumeration over a machine's IR.                  | experimental |
| `state/evolution`     | Diffs two machine definitions and classifies the SemVer bump.                    | experimental |
| `state/conformance`   | Reusable harness for driving golden scenarios against a machine.                 | experimental |
| `state/expr`          | Rich expression tier: CEL-backed guards type-checked against the context schema. | experimental |
| `telemetry`           | Vendor-neutral tracing/metrics interface for the IO modules. Stdlib-only.        | experimental |
| `telemetry/slog`      | `log/slog` adapter for the telemetry interface.                                  | experimental |
| `telemetry/otel`      | OpenTelemetry adapter for the telemetry interface.                               | experimental |
| `telemetry/datadog`   | Datadog adapter for the telemetry interface.                                     | experimental |
| `broker`              | Message broker seam — publish/subscribe transport with injected adapters.        | planned      |
| `store`               | Durable state/event store seam with graceful lifecycle.                          | planned      |
| `sink`                | Egress seam: fan emitted effects out to many destinations, fire-and-forget.      | experimental |

The engine emits effects as pure data; the IO modules are the thin seams that
carry those effects to real transports, stores, and sinks — each
"bring your own adapter," none forced on the consumer.

## Status

Early and evolving. The `state` module is now a complete, embeddable statechart
engine — hierarchical, parallel, and final states; history; guard combinators;
delayed transitions; invoked services; an actor model with message passing;
snapshots; inspection; and JSON (de)serialization — backed by its `analysis`,
`evolution`, and `conformance` companion packages. Treat its API as experimental
until it reaches v1. The `telemetry` interface and its `slog`, `otel`, and
`datadog` adapters are released. The `sink` egress seam and its destination
adapters are now available and documented; the `broker` and `store` modules are
planned.

## Roadmap — event-driven seams

The kernel emits effects as pure data; a small family of bring-your-own-adapter
IO seams moves events to and from the outside world, each defaulting to a no-op
and forcing nothing third-party on the consumer:

- **`broker`** _(planned)_ — pub/sub transport: publish emitted events and
  subscribe machines to external streams.
- **`sink`** — egress fan-out: dispatch emitted effects to many outlets (SQL,
  Dynamo, StatsD, …), fire-and-forget.
  [Docs](https://stablekernel.github.io/crucible/sink/overview/).
- **`source`** _(exploring)_ — ingress: subscribe external streams and drive
  machines; the symmetric counterpart to `sink`.
- **`bellows`** _(exploring)_ — resilience seam: circuit-breaking and
  backpressure around the IO edges.

Durable state and event persistence is tracked separately with the `durable`
runtime, not here.

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
