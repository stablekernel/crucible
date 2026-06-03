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
| `Nak`         | do **not** mark; the record is re-read on restart/rebalance |
| `NakAfter(d)` | best-effort: pause partition, wait `d`, re-seek, resume     |
| `Term`        | produce the record to the dead-letter topic, then mark      |
| `InProgress`  | no-op (Kafka has no per-message ack deadline)               |
| `Manual`      | no-op (the handler settled via `Message.As` + the client)   |

### Divergence: Nak delay

Kafka has no native per-message redelivery delay. `NakAfter(d)` is emulated by
pausing the record's partition, waiting out `d` (or the context), re-seeking to
the record's own offset, and resuming. A plain `Nak` simply declines to mark,
so the record is re-delivered on the next restart or rebalance. This is the
documented divergence from JetStream's native delayed nak.

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
