<h1 align="center">Crucible</h1>

<!-- Row 1: identity & quality -->
<p align="center">
  <a href="https://pkg.go.dev/github.com/stablekernel/crucible/state"><img src="https://pkg.go.dev/badge/github.com/stablekernel/crucible/state.svg" alt="Go Reference"></a>
  <a href="https://goreportcard.com/report/github.com/stablekernel/crucible/state"><img src="https://goreportcard.com/badge/github.com/stablekernel/crucible/state" alt="Go Report Card"></a>
  <a href="./state/go.mod"><img src="https://img.shields.io/github/go-mod/go-version/stablekernel/crucible?filename=state%2Fgo.mod" alt="Go Version"></a>
  <a href="https://stablekernel.github.io/crucible/"><img src="https://img.shields.io/badge/docs-crucible-E8702A" alt="Docs"></a>
</p>

<!-- Row 2: project health & governance -->
<p align="center">
  <a href="./CONTRIBUTING.md"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs welcome"></a>
  <a href="./CODE_OF_CONDUCT.md"><img src="https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg" alt="Contributor Covenant 2.1"></a>
  <a href="https://securityscorecards.dev/viewer/?uri=github.com/stablekernel/crucible"><img src="https://api.securityscorecards.dev/projects/github.com/stablekernel/crucible/badge" alt="OpenSSF Scorecard"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License: Apache 2.0"></a>
</p>

<p align="center">
  <img src="docs/src/assets/mascot.png" alt="The Crucible sky-squid mascot" width="220">
</p>

<p align="center"><strong>Forge event-driven services in Go.</strong></p>

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

## Design & docs

Design rationale, concepts, and guides live on the
[documentation site](https://stablekernel.github.io/crucible/). Start with:

- [Suite overview & philosophy](https://stablekernel.github.io/crucible/about/overview/)
  — the suite-wide baseline every module is built to.
- [State machine introduction](https://stablekernel.github.io/crucible/start/introduction/)
- [Concepts: machine & instance](https://stablekernel.github.io/crucible/concepts/machine-and-instance/)

The suite-wide engineering standards are inlined in
[CONTRIBUTING.md](./CONTRIBUTING.md#engineering-standards). For questions or
proposals, open a GitHub issue.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](./CONTRIBUTING.md) for dev
setup, the [Mage](https://magefile.org) targets, conventional commits, and the
DCO sign-off requirement. By participating you agree to the
[Code of Conduct](./CODE_OF_CONDUCT.md).

## License

Licensed under the [Apache License, Version 2.0](./LICENSE). See
[NOTICE](./NOTICE) for attribution.
