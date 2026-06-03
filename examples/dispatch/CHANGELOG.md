# Changelog

All notable changes to `crucible/examples/dispatch` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0]

The first release of the flagship Crucible showcase. It takes the food-delivery
order saga (a single rich statechart with hierarchy, parallel regions, actors,
invoked services, a timed SLA watchdog, and a compensation saga) and runs that one
machine under the whole Crucible suite, proving it is at once proven, durable,
distributed, polyglot, and observable.

### Added

- **Proof.** `Prove` establishes the order saga is well-formed before any order is
  dispatched: every key lifecycle stage is reachable (verified exactly and
  guard-agnostically), the Watchdog region's `OnTime` and `Overdue` leaves are mutually
  exclusive, and no transition guard is a contradictory dead branch. It returns a
  `ProofReport` a host can assert on at startup, in a test, or in a release gate.
- **Durable execution.** `RunCrashRecovery` drives the proven saga to its live `Active`
  fulfillment configuration under the durable runtime backed by an on-disk store,
  simulates a process crash, reconstructs the order from the store alone (state,
  payment hold, and folded log intact), and drives it on to `Delivered`. `RunTimeTravel`
  reconstructs the order's state read-only at earlier points in its lifecycle.
- **Distributed fulfillment.** `RunDistributedFulfillment` hosts the same kitchen and
  courier behaviors as remote cluster actors on separate worker nodes over real gRPC
  (carried in-memory by `bufconn`), restarts a crashed worker actor through a worker-side
  supervisor, and drives both remote actors to completion across the wire, proving the
  fulfillment actors are location-transparent.
- **Polyglot guard.** `RunPolyglotEquivalence` proves the saga's "generous order"
  admission guard decides identically whether evaluated by the in-tree CEL engine or by a
  WebAssembly guest, swapped in through the engine-agnostic guard seam without touching
  the machine.
- **Observability.** `RunObservedSaga` drives the durable saga to `Delivered` while
  emitting one trace span and one counter increment per transition, each tagged with the
  from/to stage, through Crucible's vendor-neutral telemetry seam. Telemetry arrives as an
  injected `telemetry.Provider`, so a host wires an slog, otel, or datadog backend while
  the default runs silently.

[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/examples/dispatch/v0.1.0
