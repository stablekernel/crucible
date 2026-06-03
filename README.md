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
logger, *your* tracer, *your* clock. Crucible never makes you adopt its
choices, and never leaks a third-party type into a public signature.

The `state` engine is the extreme end of this: **stdlib-only**, with no injected
IO at all. The IO modules carry the heavier seams via injection, but follow the
same rule. Defaults are no-ops, nothing third-party is forced on the consumer.

## Documentation

Guides, concepts, the food-delivery example, and the generated API reference live
in the documentation site:

### **[stablekernel.github.io/crucible](https://stablekernel.github.io/crucible/)**

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
| `broker`              | Message broker seam: publish/subscribe transport with injected adapters.         | planned      |
| `sink`                | Egress seam: fan emitted effects out to many destinations, fire-and-forget.      | experimental |
| `source`              | Ingress seam: consume streams and drive statecharts; ack on durable transition.  | experimental |
| `source/kafka`        | Kafka/RedPanda Inlet over franz-go: group consumer, mark-commit-after-process.   | experimental |
| `source/jetstream`    | NATS JetStream Inlet over nats.go: pull consumer, ack/nak/term, MaxAckPending.    | experimental |
| `source/redis`        | Redis Streams Inlet over go-redis: consumer group, XACK/pending-claim, DLQ.       | experimental |
| `source/cloudevents`  | CloudEvents codec with structured and binary content modes.                      | experimental |
| `source/cdc`          | Change-data-capture codec: decode Debezium/OpenCDC change events, drive by key.   | experimental |
| `source/statemachine` | Bridge: an inbound message drives a transition, ack tied to the durable commit.  | experimental |

source also ships composable reliability middleware as its own opt-in modules
(`source/retry`, `source/dlq`, `source/idempotency`, `source/schema`) and an
in-memory `source/memsource` test source, each experimental.

The engine emits effects as pure data; the IO modules are the thin seams that
carry those effects to real transports and sinks. Each is
"bring your own adapter," none forced on the consumer.

## Status

Early and evolving. The `state` module is now a complete, embeddable statechart
engine, covering hierarchical, parallel, and final states; history; guard combinators;
delayed transitions; invoked services; an actor model with message passing;
snapshots; inspection; and JSON (de)serialization. It is backed by its `analysis`,
`evolution`, and `conformance` companion packages. Treat its API as experimental
until it reaches v1. The `telemetry` interface and its `slog`, `otel`, and
`datadog` adapters are released. The `sink` egress seam and its destination
adapters, and the `source` ingress seam with its Kafka, JetStream, and Redis
Streams adapters, CloudEvents and CDC codecs, reliability middleware, and
state-machine bridge, are now available and documented; the `broker` module is
planned.

## Roadmap: event-driven seams

The kernel emits effects as pure data; a small family of bring-your-own-adapter
IO seams moves events to and from the outside world, each defaulting to a no-op
and forcing nothing third-party on the consumer:

- [ ] **`broker`** _(planned)_: pub/sub transport. Publish emitted events and
  subscribe machines to external streams.
- [x] **`sink`**: egress fan-out. Dispatch emitted effects to many outlets (SQL,
  Dynamo, StatsD, and more), fire-and-forget.
  [Docs](https://stablekernel.github.io/crucible/sink/overview/).
- [x] **`source`**: ingress. Subscribe external streams and drive machines, with
  the ack tied to a durable transition; the symmetric counterpart to `sink`.
  [Docs](https://stablekernel.github.io/crucible/source/overview/). The
  `source/cdc` codec decodes Debezium/OpenCDC change-event topics into typed
  change events; a native database write-ahead-log connector (logical replication
  slot, binlog) remains future work.
- [ ] **`bellows`** _(exploring)_: resilience seam. Circuit-breaking and
  backpressure around the IO edges.

Durable state and event persistence is tracked separately with the `durable`
runtime, not here.

## Roadmap: authoring & visualization

The serializable IR is a first-class artifact, not just an internal format: anything
that reads or writes it can build on the engine without reaching into the kernel. A
small set of tools works the IR directly.

- [ ] **Visual editor** _(planned)_: a browser workbench over the IR. Author, simulate,
  and inspect machines, with reachability and version-diff overlays from the existing
  `analysis` and `evolution` packages.
- [ ] **IR CLI** _(exploring)_: headless IR tooling for CI. Lint reachability and
  nondeterminism, render diagrams, and classify version diffs straight from a machine's
  IR.

## Design & docs

Design rationale, concepts, and guides live on the
[documentation site](https://stablekernel.github.io/crucible/).

Start with:

- [Suite overview & philosophy](https://stablekernel.github.io/crucible/about/overview/):
  the suite-wide baseline every module is built to.
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
