---
title: What is crucible/cluster
description: A host-side distribution runtime that spreads a state machine and its actors across nodes with remote delivery, supervision, and live instance migration over a pluggable transport.
sidebar:
  order: 1
---

<!-- IMAGE-SLOT: cluster-overview-nodes (a sky-squid smith routing molten streams between several connected crucibles on different anvils, one casting being carried to another anvil mid-pour; ember/copper on steel) 16:9 -->

`crucible/cluster` is the host-side **distribution runtime** for the
[`state`](/crucible/start/introduction/) kernel: remote actors, supervision, and
live instance migration. `state` runs a machine and its child-machine actors in
one process; cluster spreads that across nodes. A parent on one node addresses
and drives an actor running on another, failures are supervised with
restart/backoff strategies, and a running instance can be migrated to a different
node, all over a pluggable `Transport`, with the kernel left pure and stdlib-only.

The runtime is **additive** over the kernel. It consumes seams the kernel already
reserves (the opaque `ActorRef` whose `Node` locator names the owning host, the
injectable `ActorSystem`, the `Snapshot`/`Restore` pair, and the typed
`ActorEscalation`/`EscalationHandler`) and needs no kernel change beyond the
additive `ActorRef.Node` locator. The core is itself stdlib-only; transport
dependencies live behind the `Transport` interface, out of the core.

## Remote actors

A `System` wraps a node's local `state.ActorSystem` with a node identity and an
optional `Transport`. Operations on a ref this node owns are delegated locally;
operations on a ref another node owns are routed over the transport.

```go
tr := cluster.NewInMemoryTransport()

nodeA := cluster.NewSystem("node-a", actorSysA, cluster.WithTransport(tr))
nodeB := cluster.NewSystem("node-b", actorSysB, cluster.WithTransport(tr))
tr.Register("node-a", nodeA)
tr.Register("node-b", nodeB)

// Spawn a worker on node-b from node-a, then drive it through the returned ref.
ref, err := nodeA.Spawn(ctx, "node-b", "worker", "w-1", nil)
_, err = nodeA.Deliver(ctx, ref, "start") // routed to node-b
```

The ref is opaque: the holder never learns where the actor runs. A ref this node
owns has an empty `Node`; a remote ref carries the owning node. A `System` with no
`Transport` serves its local actors transparently and reports `ErrNoTransport` for
a remote ref. The in-tree `InMemoryTransport` connects node-scoped systems in one
process; a real network transport (see [`transport`](/crucible/transport/overview/))
implements the same `Transport` interface.

## Supervision

A `Supervisor` turns the kernel's typed `ActorEscalation` into a per-source policy.
Wire it with `ActorSystem.WithEscalationHandler(sup.Handle)`; each failed actor is
routed to a `Decision` by the src it was spawned from:

| Decision | Behavior |
| --- | --- |
| `Escalate` | forward the failure to a sink up the hierarchy (the default) |
| `Stop` | contain the failure at this level |
| `Restart` | re-spawn through a `Respawner` (the `System`), bounded by a per-src budget; on exhaustion, escalate |
| `Backoff` | defer the re-spawn behind an exponentially growing delay; the host applies due restarts via `Tick` |

```go
sup := cluster.NewSupervisor(
	cluster.WithRestart("worker", 3),                                  // up to 3 immediate restarts
	cluster.WithBackoff("flaky", 5, 100*time.Millisecond, time.Minute, 2.0),
	cluster.WithEscalationSink(parentHandler),
)
sup.SetRespawner(node)
actorSys.WithEscalationHandler(sup.Handle)
// ... drive backoff restarts from a timer loop:
for range ticker.C { sup.Tick(ctx) }
```

Backoff reads time through an injected `state.Clock` (`WithClock`, default the
system clock), so it is deterministic under a `state.FakeClock` in tests.

## Live migration

`Capture` snapshots a running instance, its actor tree, and its machine definition
into a wire-shippable `Checkpoint`; `Restore` rebuilds it on another node, resuming
in place. The move is **gated on schema compatibility**. `Restore` diffs the source
and target machine definitions with
[`state/evolution`](/crucible/analysis/evolution/) and refuses a breaking target
with `ErrIncompatibleMigration`, so an instance never resumes against a definition
that would misread its state.

```go
cp, err := cluster.Capture(inst, actorSys, machine)        // on the source node
// ... ship cp (it is all JSON) to the target node ...
inst, sys, err := cluster.Restore(ctx, cp, targetMachine,  // on the target node
	cluster.WithActorBehaviors(palette))
// err is ErrIncompatibleMigration if targetMachine is a breaking change.
```

## Where it fits

cluster is feature-complete on the in-memory transport, which is enough to build
and test a distributed topology in one process. To carry deliveries, spawns, and
time-travel queries between real nodes over the network, pair it with
[`transport`](/crucible/transport/overview/), the gRPC implementation of the same
`Transport` seam.
