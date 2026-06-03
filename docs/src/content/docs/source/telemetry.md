---
title: Telemetry and lifecycle
description: source consumes crucible/telemetry; one shared tracer and meter give a per-message span with decode/route/fire/persist/ack children, plus graceful drain and a readiness signal.
sidebar:
  order: 7
---

<!-- IMAGE-SLOT: source-telemetry-thread -->

source **consumes** [`crucible/telemetry`](/crucible/reference/), the suite's
vendor-neutral tracing and metrics interface. It defines no observability
abstraction of its own and pulls in no telemetry vendor. You pass one shared
`Tracer` and `Meter`, the same ones the rest of your service and the
[`state`](/crucible/start/introduction/) kernel use.

```go
h := source.NewHopper(
    source.WithLogger(logger), // *slog.Logger
    source.WithTracer(tracer), // telemetry.Tracer
    source.WithMeter(meter),   // telemetry.Meter
)
```

Every seam defaults to a no-op: a discarding `slog` handler,
`telemetry.NopTracer()`, `telemetry.NopMeter()`. An un-instrumented Hopper
allocates no backend and does no IO on the hot path. Observability is opt-in,
never a required dependency.

## What it records

A consume is one span with a child span per stage, so a trace tells the whole
story of a message: decode, route, fire, persist, ack.

| Instrument | Kind | Meaning |
|---|---|---|
| `source.consume` | span | one message, with decode/route/fire/persist/ack children |
| `source.lag` | gauge | consumer lag |
| `source.ack` · `source.nak` | counter | acked / redelivered messages |
| `source.retry` · `source.dlq` | counter | retried / dead-lettered messages |
| `source.transition_applied` | counter | transitions applied by the state-machine bridge |
| `source.transition_rejected` | counter | guard rejections (invalid-for-state) |

## Spans nest under the cause

The per-message span is started on the context the engine threads through every
stage, so each stage's own spans nest underneath it, and when a transition fans
back out through a [`sink`](/crucible/sink/telemetry/) Manifold the emit span
nests under the fire stage. One trace correlates consume, transition, and the
resulting writes.

## Graceful drain and readiness

Lifecycle is bounded and predictable. On ctx cancel the Hopper stops fetching,
finishes in-flight work, commits, and closes, so there is no goroutine leak and
no commit-before-process. A readiness signal reports when the consumer is live, a
gap the Delivery broker had. A wedged backend cannot hang teardown past the
deadline you set.
