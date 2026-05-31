# dispatch

The flagship showcase: the food-delivery order saga run under the full Crucible runtime.

> Pre-1.0: the suite is on a `v0.x` line and APIs may shift between minor versions.

```
import "github.com/stablekernel/crucible/examples/dispatch"
```

## What it is

`dispatch` takes the order-lifecycle statechart from the
[`fooddelivery`](../fooddelivery) example — a rich machine with hierarchy, parallel
regions, actors, invoked services, a timed SLA watchdog, and a compensation saga —
and runs it under the whole Crucible suite, one capability at a time.

This first capability proves the machine. Before any order is dispatched, `Prove`
establishes that the saga is well-formed:

- every key lifecycle stage (`Active`, `Delivered`, `Canceled`, `Rejected`,
  `Overdue`) is reachable, verified exactly and guard-agnostically;
- the Watchdog region's `OnTime` and `Overdue` leaves are mutually exclusive — they
  are sequential leaves of one region, so they are never simultaneously active;
- no transition guard is a contradictory dead branch; and
- the analyzer finds no nondeterministic competing transitions it can rule out.

The result is a `ProofReport` a host can assert on at startup, in a test, or in a
release gate.

## Quick start

```go
model, err := fooddelivery.NewModel()
if err != nil {
	log.Fatal(err)
}

report, err := dispatch.Prove(model)
if err != nil {
	log.Fatal(err)
}

if !report.Sound() {
	log.Fatalf("order saga is not well-formed: %+v", report)
}
```

## Durable execution

The next capability runs the proven saga under the
[`durable`](../../durable) runtime, so an order survives a process crash and its
lifecycle can be replayed read-only after the fact. The saga is reused wholesale —
the model, the payment services (`fooddelivery.ServiceRegistry`), and the
kitchen/courier actor behaviors (`fooddelivery.KitchenBehavior` /
`fooddelivery.CourierBehavior`) — driven through the durable `Handle` API rather
than the example's in-process Rig.

Two properties are demonstrated:

- **Crash and recovery**, against a real on-disk `durable.FileStore`. `RunCrashRecovery`
  drives an order to its live `Active` fulfillment configuration, drops the runner
  and handle to simulate a process crash, then reconstructs the order from the store
  alone with `durable.Recover` — its state, payment authorization hold, and folded
  milestone log intact — and drives the recovered order on to `Delivered`. The
  authorize service ran exactly once, on the live path; recovery replays its recorded
  result without re-invoking it.
- **Read-only time travel**, against a history-retaining `durable.MemStore`
  (`WithHistory`). `RunTimeTravel` records the same happy path, then uses
  `durable.Steps` and `durable.StateAt` to reconstruct the order's state at each
  recorded step — and at an earlier point in its lifecycle — without re-running any
  service or actor.

```go
recovery, err := dispatch.RunCrashRecovery(ctx, storeDir)
if err != nil {
	log.Fatal(err)
}
// recovery.RecoveredConfig is [Cooking OnTime]; recovery.FinalConfig is [Delivered].

travel, err := dispatch.RunTimeTravel(ctx)
if err != nil {
	log.Fatal(err)
}
// travel.EarlierConfig is an earlier configuration, distinct from travel.FinalConfig.
```

Later capabilities build on this proven, durable core — running the saga across a
cluster, over a transport, and under observation — each added without disturbing the
proof.
