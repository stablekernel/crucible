---
title: The Inlet and Hopper model
description: An Inlet adapts a backend; the Hopper runs the consume loop; the Handler returns a Result; optional capabilities are detected by interface.
sidebar:
  order: 2
---

<!-- IMAGE-SLOT: source-inlet-hopper-cutaway -->

Two types carry the structural model, mirroring sink's `Outlet`/`Manifold`
one-for-one. Everything else is literal.

## Inlet: a single backend

An `Inlet` is one place messages come from. It opens subscriptions and acks; the
vendor record stays private behind an `As()` escape hatch.

```go
type Inlet interface {
    Subscribe(ctx context.Context, cfg SubscribeConfig) (Subscription, error)
    Close() error
}

type Subscription interface {
    Receive(ctx context.Context, h Handler) error      // runs until ctx cancel
    Stream(ctx context.Context) iter.Seq2[Message, error] // the read-loop alternative
    Close() error                                       // graceful drain
}
```

Every message is backend-neutral. No vendor type appears in the public surface;
the rare reach-through goes through `As()`:

```go
type Message interface {
    Key() []byte
    Value() []byte
    Headers() Headers      // typed accessors, not a magic-string map
    Subject() string       // topic (Kafka) or subject (JetStream)
    PartitionKey() string  // partition identity (Kafka) or "" (JetStream)
    Cursor() Cursor        // opaque, resumable position (offset or stream seq)
    As(target any) bool    // documented escape hatch to the vendor message
}
```

## Handler and Result: the engine acts, you decide

A `Handler` is your business logic. It returns a `Result`; the Hopper does the
acking. There are no stateful message objects with a hidden ack channel.

```go
type Handler func(ctx context.Context, m Message) Result

type Result struct {
    Action  Action         // Ack | Nak | Term | InProgress | Manual
    Requeue time.Duration  // Nak delay
    Class   Classification // Retryable | Poison | Drop | Throttle | InvalidForState
    Err     error
}
```

The ack is **handler-return-driven**, with a manual override and a `Term`
outcome. The contract is **ack always after durable success**: at-least-once is
the default, never commit-before-process.

| Action | Meaning |
|---|---|
| `Ack` | processed successfully; commit past this message |
| `Nak(delay)` | transient failure; redeliver, optionally after a delay |
| `Term` | poison or invalid-for-state; do not retry, route to DLQ |
| `InProgress` | long handler still working; extend the ack deadline |
| `Manual` | the handler acked itself through `As()` (batched commit, double-ack) |

A typed handler resolves the generic `T` through the instance-scoped codec
registry:

```go
type TypedHandler[T any] func(ctx context.Context, m Typed[T]) Result
```

## Hopper: the consume engine

A `Hopper` owns the consume loop. Construct it with functional options, the same
shape as sink's Manifold, and every seam has a no-op default so a zero-option
build is fully functional and silent.

```go
h := source.NewHopper(
    source.WithLogger(log),    // *slog.Logger; default discards
    source.WithTracer(tracer), // telemetry.Tracer; default no-op
    source.WithMeter(meter),   // telemetry.Meter; default no-op
    source.WithMaxInFlight(256),
)
```

The Hopper drives [ordered-key concurrency](/crucible/source/concurrency/), the
codec decode, the [reliability middleware](/crucible/source/reliability/) chain,
and the lifecycle (graceful drain on ctx cancel).

## Optional capabilities, detected by interface

Backend-specific behavior is an **optional capability interface**, discovered
once by type assertion inside the engine, never a lowest-common-denominator lie
and never an unwrap ladder at your call site. The core `Inlet` is the honest
common path; an adapter satisfies only the capabilities its backend truly
supports.

| Capability | What it adds |
|---|---|
| `Seekable` | replay by time or position |
| `ConsumerGroups` | rebalance hooks (`OnAssigned`/`OnRevoked`) |
| `SharedDurable` | shared durable consumer (the JetStream grouping analog) |
| `PartitionOrdered` · `OrderedDelivery` | per-partition or single-threaded ordered reads |
| `Batched` | batched fetch and handling |
| `Transactional` | transactional consume-to-produce |
| `Deduper` | a dedup seam |
| `LagReporter` | consumer-lag reporting |

Which adapter satisfies which is spelled out on the
[adapters page](/crucible/source/adapters/). The next page covers how the Hopper
keeps a statechart instance's events in order while running other keys in
parallel.
