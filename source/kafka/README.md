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
| `Term`        | produce the record to the dead-letter topic, then mark (transactional subscriptions: rejected with `ErrTermInsideTransaction`; route poison through `Begin` instead). If the DLQ produce fails, `Settle` returns the error and the record is **not** marked — it is redelivered; nothing is silently lost      |
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

### Divergence: Term inside a transaction

On a subscription built with `WithTransactional`, settling `Term` directly is
rejected with `ErrTermInsideTransaction`: a DLQ produce outside the open EOS
session would not be atomic with the consumed offset. Inside
`Transactional.Begin`, produce the poison record to the dead-letter topic via
the handed `source.Tx` and return nil — the DLQ record and the input offset
then commit as one atomic unit.

## Capabilities

The subscription satisfies these optional `source` capability interfaces,
discovered by the engine via type assertion. The neutral seam stays
vendor-free — no franz-go type crosses the `source.Inlet` / `Subscription` /
`Message` surface; the typed option and escape-hatch seams below deliberately
do expose it:

- `Seekable` — live offset reposition via `SetOffsets` (and `ListOffsets` for
  time-based seeks), the basis for replay. Partitions are enumerated from the
  group's committed offsets when present, else discovered from broker metadata,
  so seeking works before the first commit.
- `ConsumerGroups` — `GroupID` plus assign/revoke hooks; the adapter
  drain-and-commits marked offsets on a graceful revoke and skips the commit on
  an ungraceful loss.
- `PartitionOrdered` — per-partition order, the guarantee the Hopper keys its
  ordered lanes on (`PartitionKey()` is `"topic/partition"`).
- `LagReporter` — end offset minus committed offset across committed
  partitions. Before the group's first commit there is no baseline: `Lag`
  reports an error (`ErrNoCommittedOffsets`, matchable with `errors.Is`)
  rather than a misleading zero.
- `Transactional` — Kafka exactly-once consume-process-produce, available when
  the inlet is built with `WithTransactional()`.

`BlockRebalanceOnPoll` gives the engine a safe processing window: a rebalance
cannot move partitions mid-batch; the subscription releases the rebalance only
between fetches. A `NakAfter(d)` pause holds that window for at most `d` per
event: the partition pause blocks further fetches of that partition, but the
rebalance itself proceeds at the next release boundary.

## Cold start (initial start offset)

A brand-new consumer group — one with no committed offsets — starts at the
**earliest retained record** of each assigned partition (franz-go's default).
Build the inlet with `WithStartOffset(kafka.StartLatest)` to skip backlog and
consume only records produced after joining instead. The option maps onto
`kgo.ConsumeStartOffset`; partitions that already have a committed offset
always resume from the commit regardless of this setting.

## Vendor escape hatch

The neutral surface carries no vendor types. Typed power seams expose franz-go
deliberately: `WithSASL` takes `sasl.Mechanism` values, `WithBalancer` takes
`kgo.GroupBalancer` values, `WithClientOptions` appends raw `kgo.Opt` entries,
and `WithClient` injects a pre-built `*kgo.Client`. Reach that client through
`Inlet.As(**kgo.Client)` and a delivered record through
`source.Message.As(**kgo.Record)`. The client lifecycle is the inlet's unless
one is injected with `WithClient`, in which case it is the caller's.

## Stability

Stable at v1.0.0: the adapter surface is frozen (see the
[CHANGELOG](CHANGELOG.md) for the frozen-contract terms). The typed power
seams (`WithSASL`, `WithBalancer`, `WithClientOptions`, `WithClient`) may
track franz-go releases.

## Setup notes

- The dead-letter topic must exist before the first `Term` settles: the
  adapter's DLQ producer does not enable topic auto-creation. Create it up
  front (as the integration suite does with `CreateTopics`).
- Managed Kafka (e.g. MSK): authenticate by constructing franz-go
  `sasl.Mechanism` values (`pkg/sasl/plain`, `.../scram`, `.../oauthbearer`,
  or the AWS MSK IAM token provider) and passing them via `WithSASL` with
  `WithTLS`; this means importing franz-go directly for the mechanism
  constructors, which is expected.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
