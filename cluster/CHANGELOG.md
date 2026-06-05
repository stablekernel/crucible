# Changelog

All notable changes to `crucible/cluster` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`Supervisor.Forget`.** Discards a supervisor's per-actor restart bookkeeping
  (spent-restart counter and any scheduled backoff restart) for an actor id. A host
  calls it when an actor is permanently stopped, so the restart map does not
  accumulate one entry per distinct actor id for the process lifetime under churn.

### Fixed

- **Backoff overflow.** `backoffDelay` clamps to `math.MaxInt64` before the
  `time.Duration` conversion, so an uncapped schedule (no max set) with a high
  restart count no longer overflows into a negative or wrapped delay.
- **Nil-respawner Tick.** `Supervisor.Tick` guards against a Respawner cleared
  between scheduling a backoff and the due tick: the due restart is a no-op and the
  pending restart is preserved rather than panicking.

### Documentation

- **Capture consistency boundary.** `Capture`'s godoc now states that the instance
  snapshot and actor-tree snapshot are read as two separate operations, so the
  caller must quiesce the instance (no concurrent Fire or delivery) for the pair to
  be point-in-time consistent.

## [0.1.0]

The first release of the host-side distribution runtime for the `crucible/state`
kernel. It is additive over the kernel: it consumes the opaque `ActorRef` (with its
`Node` locator), the injectable `ActorSystem`, the `Snapshot`/`Restore` pair, and the
typed `ActorEscalation`/`EscalationHandler`, and the kernel stays pure and
stdlib-only. The cluster core is itself stdlib-only; transport dependencies live
behind the `Transport` interface.

### Added

- **Remote actors.** `System` wraps a node's local `state.ActorSystem` with a node
  identity and an optional `Transport`. `Spawn` starts an actor on this node or, when
  the target is another node, over the transport; `Deliver` routes to the actor's
  owning node; both treat the `ActorRef` as opaque. `SpawnLocal` and `Respawn` are the
  local primitives a transport and a supervisor drive. A `System` with no transport
  serves local actors and reports `ErrNoTransport` for a remote ref.
- **In-memory transport.** `InMemoryTransport` connects node-scoped systems in one
  process (the reference `Transport` for tests and single-process development) with
  `ErrNodeUnreachable` for an unregistered node. A real network transport implements
  the same interface out of tree.
- **Supervision.** `Supervisor` routes each escalated actor failure to a per-source
  `Decision`: `Escalate` (forward to a sink), `Stop` (contain), `Restart` (re-spawn
  through a `Respawner` within a per-source budget), and `Backoff` (deferred,
  exponentially paced restart applied by the host via `Tick`, timed through an
  injectable `state.Clock`). It plugs into `ActorSystem.WithEscalationHandler` and
  records the decisions it applied.
- **Live migration.** `Capture` snapshots a running instance, its actor tree, and its
  machine definition into a wire-shippable `Checkpoint`; `Restore` rebuilds it on
  another node, resuming in place and reconstructing actors from the target palette
  (`WithActorBehaviors`). The move is gated on schema compatibility via
  `state/evolution`, refusing a breaking target with `ErrIncompatibleMigration`.

[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/cluster/v0.1.0
