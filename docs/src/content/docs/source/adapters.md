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
pause/resume backpressure, and seek/replay, and it supports transactional
consume-process-produce (see [Exactly-once (Kafka EOS)](#exactly-once-kafka-eos)
below). The shard is the partition; the
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

## Capability table per backend

Capabilities are detected by interface assertion, once, inside the engine. The
table is honest: an adapter satisfies a capability only when its backend truly
supports it, and a compile-time `var _ Seekable = ...` assertion in each module
keeps it accurate.

| Capability | kafka | jetstream | Notes |
|---|---|---|---|
| `Seekable` | yes | yes | live `SetOffsets` on Kafka; consumer-recreate on JetStream |
| `ConsumerGroups` | yes | no | Kafka rebalance hooks; JetStream has no partition assignment |
| `SharedDurable` | no | yes | the JetStream durable-consumer grouping analog |
| `PartitionOrdered` | yes | no | Kafka per-partition order |
| `OrderedDelivery` | no | yes | JetStream `OrderedConsumer`, single-threaded |
| `Batched` | yes | yes | batched fetch on both |
| `Transactional` | yes | no | Kafka EOS only; JetStream does not, and does not fake it |
| `Deduper` | yes | yes | dedup seam (see [reliability](/crucible/source/reliability/)) |
| `LagReporter` | yes | yes | consumer-lag gauge |

The divergences are documented, never papered over. A `Nak(delay)` is a real
delayed redelivery on JetStream but is best-effort on Kafka (pause plus reseek),
and that is called out where it matters.

## Exactly-once (Kafka EOS)

Most ingress libraries stop at offset EOS: the broker commits offsets in a
transaction so a record is consumed once. `source` goes one step further on
Kafka and ties the records a transition emits to that same commit, so a
consume-process-produce cycle is atomic: the produced records and the consumed
offset are committed together, or neither is.

This is the `Transactional` capability, satisfied by the Kafka adapter only.
JetStream and the in-memory adapter do not implement it, so the capability is
absent rather than faked; on those backends use the at-least-once path. Build a
transactional inlet with a stable, unique transactional id:

```go
in, _ := kafka.New(
    kafka.WithSeedBrokers("localhost:9092"),
    kafka.WithTransactional("orders-eos-v1"), // the producer fencing token
)
sub, _ := in.Subscribe(ctx, source.SubscribeConfig{Topics: []string{"orders"}, Group: "orders-svc"})

if tx, ok := sub.(source.Transactional); ok {
    // Begin fences the produce side and the offset of m in one transaction.
    err := tx.Begin(ctx, m, func(ctx context.Context, t source.Tx) error {
        return t.Produce(ctx, source.ProducedRecord{Topic: "orders.out", Value: out})
    })
    // err == nil means the produced records AND m's offset committed atomically.
}
```

`Begin` runs your work function inside a Kafka producer transaction. If it
returns nil, the records you produced through the `Tx` are flushed and the
consumed offset is committed as one unit. If it returns an error, or a rebalance
fences the producer, the transaction aborts: the produced records are discarded,
the offset is not committed, and the input is redelivered. `Begin` is the full
settle path for that message, so the engine takes no further settle action.

Under the hood the adapter builds a franz-go `GroupTransactSession` with
read-committed fetch isolation (so it never reads another producer's uncommitted
records) and no auto-commit (the transaction commits offsets). No franz-go type
appears in the seam: you produce neutral `source.ProducedRecord` values, and the
adapter maps them to the wire.

### Limits

- **Kafka only.** EOS here is the Kafka transactional protocol. JetStream and
  other backends fall back to the at-least-once path; they do not pretend to
  offer it.
- **One transactional id per logical consumer.** The id is a producer fencing
  token. Two live consumers sharing one id fence each other; keep it stable
  across restarts and unique per consumer.
- **Idempotent downstream still matters across the persist gap.** When the bridge
  persists machine state inside the transaction, a broker abort after a
  successful save can leave the instance advanced but the offset uncommitted. The
  redelivery is then deduplicated by the machine's state version (see
  [driving a statechart with EOS](/crucible/source/with-state/#exactly-once-consume-process-produce-kafka-eos)),
  so it acks as a no-op rather than double-applying.

## Roadmap and bring your own

`source/redis` (Redis Streams) and `source/cdc` (Debezium/OpenCDC
change-data-capture) are on the roadmap. The catalog is a convenience: any type
that satisfies the `Inlet` interface is an inlet, and the
[`memsource`](/crucible/source/reliability/#testing-the-loop) in-memory adapter
is itself an `Inlet`, so you can drive the whole engine in a unit test with no
broker at all.
