---
title: Adapters
description: Each backend is its own optional module owning its vendor SDK; the kafka and jetstream adapters satisfy different optional capabilities, spelled out honestly.
sidebar:
  order: 4
---

<!-- IMAGE-SLOT: source-adapter-mounts -->

Every backend is its **own optional Go module** with its own `go.mod`, kept out
of `go.work` and built `GOWORK=off`. The source core imports no vendor SDK; you
add `crucible/source/kafka` only if you consume from Kafka, and its franz-go
dependency never touches a service that does not. Each module exposes a narrow
surface and ships a testcontainers integration leg against the real broker.

## kafka (franz-go)

`crucible/source/kafka` is a group consumer over franz-go:

```go
in, _ := kafka.New(kafka.WithBrokers("localhost:9092"), kafka.WithGroup("orders-svc"))
sub, _ := in.Subscribe(ctx, source.SubscribeConfig{Topic: "orders"})
sub.Receive(ctx, handler)
```

It does cooperative rebalance with drain-on-revoke, mark-commit-after-process,
pause/resume backpressure, and seek/replay, and it is EOS-ready for transactional
consume-to-produce. The shard is the partition; the
[high-water-mark commit subtlety](/crucible/source/concurrency/#the-kafka-high-water-mark-subtlety)
is handled in the engine.

## jetstream (nats.go)

`crucible/source/jetstream` is a pull consumer over nats.go:

```go
in, _ := jetstream.New(jetstream.WithURL("nats://localhost:4222"), jetstream.WithStream("ORDERS"))
sub, _ := in.Subscribe(ctx, source.SubscribeConfig{Subject: "orders.>"})
sub.Receive(ctx, handler)
```

It does `Ack`/`Nak`/`Term`/`InProgress`, `MaxAckPending` backpressure, the
`OrderedConsumer` for single-threaded ordered delivery, and replay through the
deliver-by-start options. A JetStream durable consumer is the grouping analog of
a Kafka consumer group, but it has no partitions and no assignment callbacks, so
JetStream does **not** pretend to be a `ConsumerGroups` backend.

## redis (go-redis)

`crucible/source/redis` is a consumer-group reader over go-redis:

```go
in, _ := redis.New(redis.WithAddr("localhost:6379"), redis.WithGroup("orders-svc"), redis.WithConsumer("worker-1"))
sub, _ := in.Subscribe(ctx, source.SubscribeConfig{Topics: []string{"orders"}})
m, _ := sub.Next(ctx)
_ = sub.Settle(ctx, m, source.Ack())
```

It reads a Redis Stream with `XREADGROUP`, acks with `XACK`, leaves a naked
entry in the pending list for a later `XPENDING` plus `XCLAIM` redelivery, and
routes a terminated entry to a configured dead-letter stream before acking the
original. Replay is by entry ID through `XRANGE`, and lag comes from
`XINFO GROUPS` with an `XLEN` fallback. A Redis consumer group is the grouping
analog of a Kafka consumer group, but it has no partitions and no assignment
callbacks, so Redis does **not** pretend to be a `ConsumerGroups` backend.

## Capability table per backend

Capabilities are detected by interface assertion, once, inside the engine. The
table is honest: an adapter satisfies a capability only when its backend truly
supports it, and a compile-time `var _ Seekable = ...` assertion in each module
keeps it accurate.

| Capability | kafka | jetstream | redis | Notes |
|---|---|---|---|---|
| `Seekable` | yes | yes | yes | live `SetOffsets` on Kafka; consumer-recreate on JetStream; entry-ID `XRANGE` on Redis |
| `ConsumerGroups` | yes | no | no | Kafka rebalance hooks; JetStream and Redis have no partition assignment |
| `SharedDurable` | no | yes | yes | the JetStream durable-consumer and Redis consumer-group grouping analogs |
| `PartitionOrdered` | yes | no | no | Kafka per-partition order |
| `OrderedDelivery` | no | yes | no | JetStream `OrderedConsumer`, single-threaded |
| `Batched` | yes | yes | no | batched fetch on Kafka and JetStream |
| `Transactional` | yes | no | no | Kafka EOS only; JetStream and Redis do not, and do not fake it |
| `Deduper` | yes | yes | no | dedup seam (see [reliability](/crucible/source/reliability/)) |
| `LagReporter` | yes | yes | yes | consumer-lag gauge; Redis reports group lag from `XINFO GROUPS` |

The divergences are documented, never papered over. A `Nak(delay)` is a real
delayed redelivery on JetStream but is best-effort on Kafka (pause plus reseek),
and that is called out where it matters.

## Roadmap and bring your own

`source/cdc` (Debezium/OpenCDC change-data-capture) is on the roadmap. The
catalog is a convenience: any type
that satisfies the `Inlet` interface is an inlet, and the
[`memsource`](/crucible/source/reliability/#testing-the-loop) in-memory adapter
is itself an `Inlet`, so you can drive the whole engine in a unit test with no
broker at all.
