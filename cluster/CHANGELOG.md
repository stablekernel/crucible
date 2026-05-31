# Changelog

All notable changes to `crucible/cluster` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
  process — the reference `Transport` for tests and single-process development — with
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
