---
title: Fire-and-forget fan-out
description: Sink is the only emit path and returns nothing; failures route to the logger and metrics. Reservoir batches, Poller samples.
sidebar:
  order: 3
---

`Manifold.Sink` is the **only** emit path, and it returns nothing:

```go
func (m *Manifold) Sink(ctx context.Context, payload any)
```

For each attached outlet, the Manifold calls `outlet.Sink(ctx, payload)` and
sorts the result:

- **success** → the `sink.sunk` counter,
- **`ErrUnregistered`** → the `sink.skipped` counter (a normal "this outlet does
  not handle this type"), nothing logged,
- **any other error** → the `sink.failed` counter, recorded on the emit span,
  and logged at `ERROR` on the configured `*slog.Logger`.

One outlet failing never stops the others, and it never propagates to the
caller. That is the contract: the call site fires and moves on.

## Why no synchronous result

There is deliberately no `SinkWait`. A buffered outlet (see
[Reservoir](#reservoir-batching) below) can only confirm *admission to its
buffer*, not the eventual write, so a synchronous "all confirmed" return would
be a dishonest guarantee. When you genuinely need confirmation for one critical
destination, hold that `Outlet` directly and call it — you get an honest,
per-destination error:

```go
if err := auditOutlet.Sink(ctx, payload); err != nil {
    return err // 500, retry, compensate — your call
}
m.Sink(ctx, payload) // everything else fans out fire-and-forget
```

Errors that do surface (through a held outlet) are typed and wrap cleanly:

```go
var se *sink.Error
if errors.As(err, &se) {
    log.Error("sink failed", "outlet", se.Outlet, "phase", se.Phase, "type", se.PayloadType)
}
```

## Reservoir — batching

Wrap any outlet in a `Reservoir` to buffer payloads and release them in batches,
by **size** or on an **interval**:

```go
batched := sink.Reservoir(s3Outlet,
    sink.WithBatchSize(100),
    sink.WithBatchInterval(5*time.Second),
)
m.Attach(batched)
```

On flush, if the wrapped outlet implements `BatchOutlet` the Reservoir calls
`SinkBatch` once; otherwise it loops `Sink`. It records `sink.batch_size` and
`sink.flush_latency_ms`, drops past an optional `WithMaxBuffered` cap (counted on
`sink.dropped`), and reads its clock through `WithReservoirClock` — so tests
drive flushes deterministically with **no sleeps**. The returned value is itself
an `Outlet` (and a `Flusher` and `Shutdowner`), so it composes anywhere an outlet
goes.

## Poller — periodic sampling

Where a Reservoir reacts to payloads pushed in, a `Poller` *pulls*: on an
interval it runs a `CollectFunc` that yields payloads, and sinks each to a target
outlet.

```go
p := sink.NewPoller(metricsOutlet, func(ctx context.Context, emit func(any)) {
    emit(QueueDepth{N: queue.Len()})
}, sink.WithPollInterval(time.Second))
p.Start(ctx)
defer p.Stop()
```

`Start` is idempotent and fluent; `Stop` cancels and waits. Like the Reservoir,
the Poller takes its clock as an option for deterministic tests.

<!-- IMAGE-SLOT: sink-reservoir-pour — a foundry reservoir/ladle filling to a fill-line then tipping a measured batch into a mold, a second ladle on an interval timer; sky-squid tending; ember/copper on steel — 16:9 -->
![Reservoir buffering and releasing in batches](../../../assets/placeholders/hero.svg)
