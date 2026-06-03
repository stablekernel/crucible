# source/redis

A [`crucible/source`](../) ingress adapter that consumes a Redis Stream through
a consumer group. Runtime dependency:
[`github.com/redis/go-redis/v9`](https://github.com/redis/go-redis).

```go
in, err := redis.New(
    redis.WithAddr("localhost:6379"),
    redis.WithGroup("orders-svc"),
    redis.WithConsumer("worker-1"),
    redis.WithDLQStream("orders.dlq"),
    redis.WithBlock(5*time.Second),
    redis.WithCount(64),
)
sub, _ := in.Subscribe(ctx, source.SubscribeConfig{Topics: []string{"orders"}})

m, _ := sub.Next(ctx)              // or drive sub with a source.Hopper
_ = sub.Settle(ctx, m, source.Ack())
```

Construct an `Inlet` with `New` and functional options (`WithAddr`/`WithClient`,
`WithGroup`, `WithConsumer`, `WithDLQStream`, `WithBlock`, `WithCount`,
`WithMinIdle`). The returned `Subscription` reads entries with `Next` (via
`XREADGROUP`) and applies a handler `source.Result` with `Settle`. Backpressure
comes from the `Count` batch size plus the block window. Reach the underlying
`*redis.Client` through `Inlet.As`, or the per-entry `redis.XMessage` through
`Message.As`; no vendor type appears in the adapter's own signatures.

A stream entry maps onto `source.Message` as follows: the `value` field becomes
the raw `Value`, the `crucible-key` field (when set) becomes the routing `Key`
(otherwise the stream name), every field is exposed as a `Header`, `Subject` is
the stream, and `Cursor` is the entry ID.

## Settle vocabulary

A consumer group reads with `XREADGROUP`; every delivered entry stays in the
group's Pending Entries List (PEL) until it is settled.

- **Ack** calls `XACK`, removing the entry from the PEL.
- **Nak** leaves the entry in the PEL. A consumer reclaims and redelivers it by
  scanning the backlog with `XPENDING` + `XCLAIM` once it has idled past the
  minimum; call `NakRedeliver` (on a timer or between read cycles) to drive that
  pass. The cadence is a deployment choice, so the engine does not force it.
- **Term** (and **Reject**) append the entry's fields plus dead-letter metadata
  (`crucible-dlq-original-id`, `crucible-dlq-stream`, `crucible-dlq-class`,
  `crucible-dlq-error`) to the configured `WithDLQStream`, then `XACK` the
  original. With no dead-letter stream configured, a terminated entry is acked
  and dropped.
- **InProgress** is a no-op: Redis has no per-message deadline to extend.
- **Manual** is a no-op; the handler settled the entry itself through
  `Message.As` and the client.

## Capabilities

The subscription honestly advertises the Redis-shaped capabilities by type
assertion: `source.SharedDurable` (the consumer group is the competing-consumer
analog), `source.Seekable` (replay by entry ID via `XRANGE`, with `SeekToTime`
translating a timestamp into an entry ID), and `source.LagReporter` (group lag
from `XINFO GROUPS`, falling back to `XLEN`).

## Redis divergences from the source contract

- **No `ConsumerGroups`.** A Redis consumer group has no partitions and no
  assignment lifecycle, so the adapter does not implement
  `source.ConsumerGroups`. The group load-balances across processes instead,
  surfaced as `source.SharedDurable`. `PartitionKey` is always `""`, so the
  Hopper shards by `Key`.
- **No `Transactional`.** Redis Streams offer no consume-side transaction, so
  the adapter does not implement `source.Transactional`; the capability is
  absent rather than faked.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
