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
and runs that one machine under the whole Crucible suite. The same order saga is shown
to be, in turn, proven, durable, distributed, polyglot, and observable.

## What it demonstrates

- **Proof** — `Prove` establishes the order saga is well-formed before any order is
  dispatched: key stages reachable, Watchdog leaves mutually exclusive, no dead guard.
- **Durable execution** — `RunCrashRecovery` runs the saga across a process crash and
  reconstructs it from the store alone; `RunTimeTravel` replays its lifecycle read-only.
- **Distributed fulfillment** — `RunDistributedFulfillment` runs the kitchen and courier
  as remote cluster actors over real gRPC, with a worker-side supervisor restarting a
  crashed actor.
- **Polyglot guard** — `RunPolyglotEquivalence` proves the "generous order" guard decides
  identically in the in-tree CEL engine and in a WebAssembly guest.
- **Observability** — `RunObservedSaga` drives the saga to `Delivered` while emitting a
  span and a metric per transition through Crucible's vendor-neutral telemetry seam.

## Proof

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

## Distributed execution

The next capability hosts the same proven fulfillment actors across a cluster, driven
over real gRPC. Where the durable runtime runs the kitchen and courier as in-process
actors of one order instance, `RunDistributedFulfillment` runs the *same* behaviors —
`fooddelivery.KitchenBehavior` and `fooddelivery.CourierBehavior` — as **remote cluster
actors** on separate worker nodes, dispatched from a coordinator node. It proves the
fulfillment actors are location-transparent: the coordinator never knows or cares where
they run.

The flow stands up a three-node cluster wired over gRPC, carried in-memory by a
`bufconn` listener so the whole cluster runs inside one process (and inside the
`Example`) without binding a TCP port:

- the coordinator **spawns** the kitchen on `worker-a` and the courier on `worker-b`,
  each over the gRPC wire, addressing them only by an opaque `state.ActorRef`;
- `worker-a` runs a `cluster.Supervisor` (`cluster.WithRestart("kitchen", 2)`) that,
  when its freshly-spawned kitchen actor crashes, **restarts** it within budget;
- the coordinator then **delivers** the `KitchenCook` and `CourierDrive` signals across
  the wire, driving the restarted kitchen and the courier each to completion.

Each worker node is typed to the signal of the actor it hosts — the kitchen and courier
advance on distinct event types, so the node's host machine decodes each wire-delivered
signal into the exact type its actor expects. The coordinator, which only marshals the
raw signal it is handed, drives both workers regardless of its own type.

```go
report, err := dispatch.RunDistributedFulfillment(ctx)
if err != nil {
	log.Fatal(err)
}
// report.Spawned places the kitchen on worker-a and the courier on worker-b;
// report.SupervisorDecision is cluster.Restart with report.Restarts == 1;
// report.Delivered counts the signals driven across the wire after the restart.
```

## Polyglot guard

The next capability proves the order saga's admission guard is **polyglot**: the same
"generous order" predicate — `subtotal + tip >= 6000` — runs identically whether
evaluated by the in-tree CEL engine or by a foreign engine compiled to WebAssembly. The
machine is untouched; only the engine that decides "generous" is swapped, through the
engine-agnostic `fooddelivery.WithGenerousGuard` seam.

`fooddelivery.NewModel` builds the generous guard as a **named, registry-bound,
engine-agnostic** node (`fooddelivery.GenerousGuardName`). By default it compiles the CEL
predicate `fooddelivery.GenerousGuardSource()`; a consumer can instead bind any engine
under the same name. The polyglot guest (`testdata/generousguest`) is a tiny wasip1
reactor — `//go:wasmexport alloc` / `eval` over the JSON ABI — that reads the order
context and returns `{"ok": subtotal+tip >= 6000}`. It is compiled to WebAssembly at test
time (`GOOS=wasip1 GOARCH=wasm go build -buildmode=c-shared`) and run through wazero; no
binary is committed.

`RunPolyglotEquivalence` builds both models and drives each through the Authorized
decision across orders chosen to **isolate the generous branch** — every order is
non-fast-lane and below the expedite threshold, so the Core big-basket branch can never
admit and only the generous guard can. One generous order (`subtotal+tip = 6500`) must be
admitted by both engines; one frugal order (`subtotal+tip = 4500`) must be blocked by
both. Because the suite exercises both verdicts, the agreement is non-vacuous.

```go
report, err := dispatch.RunPolyglotEquivalence(ctx, wasmBytes)
if err != nil {
	log.Fatal(err)
}
// report.Cases records each order's CEL and WASM outcome;
// report.Equivalent is true iff every order agreed AND both an admit and a reject
// were observed — proof the WebAssembly guard and the CEL guard decide identically.
```

## Observability

The final capability runs the proven, durable saga while **observing** every
transition through Crucible's [`telemetry`](../../telemetry) seam — a vendor-neutral
tracing and metrics interface with no backend baked in. There is no kernel hook into
the state machine; the host wraps its own drive calls, opening a span and incrementing
a counter around each transition.

`RunObservedSaga` drives the order to `Delivered` and, for each transition, emits an
`order.transition` span and an `order.transitions` counter increment, each tagged with
the `from`/`to` stage — so the telemetry narrates the order's path. Telemetry arrives as
an injected `telemetry.Provider`: a host wires an slog, otel, or datadog adapter, while
the default `telemetry.Nop()` runs the saga silently and allocation-free. The function
returns an `ObservedReport` of the observed facts (transition count, path, final stage),
so the run is verifiable from its return value with the emitted telemetry as the
human-facing trace.

```go
logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
tel := telemetry.Nop().Apply(
	telemetry.WithTracer(crucibleslog.NewTracer(crucibleslog.WithLogger(logger))),
	telemetry.WithMeter(crucibleslog.NewMeter(crucibleslog.WithLogger(logger))),
)

report, err := dispatch.RunObservedSaga(ctx, tel)
if err != nil {
	log.Fatal(err)
}
// report.Transitions is 3; report.FinalStage is Delivered; the logger captured a span
// and a metric per transition, tagged with the from/to stage.
```

## The capstone

`TestCapstone_*` ties the whole story together: it runs the same order machine through
all five capabilities in sequence — proven, durable, distributed, polyglot, observed —
asserting each stage's headline result, so the showcase reads as a single narrative.
