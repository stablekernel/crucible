# source/kafka

A [`crucible/source`](../) ingress adapter that consumes records from Apache
Kafka (and API-compatible brokers such as RedPanda) through the pure-Go
[franz-go](https://github.com/twmb/franz-go) client. Runtime dependencies: the
standard library, `crucible/source`, and `franz-go` only.

```go
inlet, _ := kafka.New(
    kafka.WithSeedBrokers("localhost:9092"),
    kafka.WithClientID("orders-consumer"),
    kafka.WithDLQTopic("orders.DLQ"),
)
defer inlet.Close()

sub, _ := inlet.Subscribe(ctx, source.SubscribeConfig{
    Topics: []string{"orders"},
    Group:  "orders",
})

// Hand the subscription to a source.Hopper, which drives the consume loop,
// decoding, per-partition ordering, and settlement.
```

## Ack model

Delivery is at-least-once: the adapter never commits an offset before its
handler reports success. The franz-go client is configured with
`AutoCommitMarks`, so only records the engine settles successfully are marked,
and the marked offsets are committed on graceful drain and on rebalance
(`OnPartitionsRevoked`). Each handler `source.Result` maps onto Kafka as:

| Result        | Kafka behavior                                              |
| ------------- | ----------------------------------------------------------- |
| `Ack`         | mark the record for commit (commit-after-process)           |
| `Nak`         | never mark; pause, re-seek to the record's offset, resume — the record is fetched again in this session |
| `NakAfter(d)` | same as `Nak`, waiting out `d` between pause and re-seek (best-effort) |
| `Term`        | produce the record to the dead-letter topic, then mark      |
| `InProgress`  | no-op (Kafka has no per-message ack deadline)               |
| `Manual`      | no-op (the handler settled via `Message.As` + the client)   |

### Divergence: Nak delay and cross-restart redelivery

Kafka has no native per-message redelivery. A `Nak` (plain or delayed) is
emulated by pausing the record's partition, waiting out the requested delay
(zero for a plain `Nak`), re-seeking the partition to the record's own offset,
and resuming — so the record is redelivered deterministically **within the live
subscription**. Costs to know about: the delay head-of-line-blocks the paused
partition; records already fetched but not yet yielded are still delivered
before the redelivered record; and concurrent settles on the same partition can
commit past the nacked offset before the re-seek lands.

Across process restarts and rebalances, redelivery rides committed offsets: a
concurrently committed higher offset can advance past a nacked record, so
cross-restart redelivery is **best-effort**, not guaranteed. This is the one
divergence from JetStream's native nak semantics.

## Capabilities

The subscription satisfies these optional `source` capability interfaces,
discovered by the engine via type assertion — no franz-go type leaks into the
exported API:

- `Seekable` — live offset reposition via `SetOffsets` (and `ListOffsets` for
  time-based seeks), the basis for replay.
- `ConsumerGroups` — `GroupID` plus assign/revoke hooks; the adapter
  drain-and-commits marked offsets on a graceful revoke and skips the commit on
  an ungraceful loss.
- `PartitionOrdered` — per-partition order, the guarantee the Hopper keys its
  ordered lanes on (`PartitionKey()` is `"topic/partition"`).
- `LagReporter` — end-offset minus committed offset across assigned partitions.
- `Transactional` — Kafka exactly-once consume-process-produce, available when
  the inlet is built with `WithTransactional()`.

`BlockRebalanceOnPoll` gives the engine a safe processing window: a rebalance
cannot move partitions mid-batch; the subscription releases the rebalance only
between fetches.

## Vendor escape hatch

No franz-go type appears in an exported signature. Reach the underlying
`*kgo.Client` through `Inlet.As(**kgo.Client)`, and a delivered record through
`source.Message.As(**kgo.Record)`. The client lifecycle is the inlet's unless
one is injected with `WithClient`, in which case it is the caller's.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
