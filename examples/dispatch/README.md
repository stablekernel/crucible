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

Later capabilities build on this proven core — running the saga durably, across a
cluster, over a transport, and under observation — each added without disturbing the
proof.
