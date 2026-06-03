# source/jetstream

A [`crucible/source`](../) ingress adapter that consumes a NATS JetStream stream
through a durable pull consumer. Runtime dependency:
[`github.com/nats-io/nats.go`](https://github.com/nats-io/nats.go) (the
`jetstream` subpackage, not the legacy `JetStreamContext`).

```go
in, err := jetstream.New(
    jetstream.WithURL(nats.DefaultURL),
    jetstream.WithStream("ORDERS"),
    jetstream.WithDurable("orders-worker"),
    jetstream.WithAckWait(30*time.Second),
    jetstream.WithMaxAckPending(256),
)
sub, _ := in.Subscribe(ctx, source.SubscribeConfig{Topics: []string{"orders.>"}})

m, _ := sub.Next(ctx)              // or drive sub with a source.Hopper
_ = sub.Settle(ctx, m, source.Ack())
```

Construct an `Inlet` with `New` and functional options (`WithURL`/`WithConn`,
`WithStream`, `WithDurable`, `WithAckWait`, `WithMaxDeliver`, `WithMaxAckPending`,
`WithPullMaxMessages`, `WithFilterSubjects`, `WithJetStream`). The returned
`Subscription` pulls messages with `Next` and applies a handler `source.Result`
with `Settle`. Backpressure comes from `MaxAckPending` plus the pull batch size.
Reach the underlying `*nats.Conn` through `Inlet.As`, or the per-message
`jetstream.Msg` through `Message.As`; no vendor type appears in the adapter's
own signatures.

## Capabilities

The subscription honestly advertises the JetStream-shaped capabilities by type
assertion: `source.SharedDurable` (a durable consumer is the competing-consumer
analog), `source.Seekable` (replay), `source.OrderedDelivery` (a single-stream
`OrderedConsumer`, opt-in before the first read), and `source.LagReporter`
(pending + ack-pending count).

## JetStream divergences from the source contract

- **No `ConsumerGroups`.** JetStream has no partitions and no assignment
  lifecycle, so the adapter does not implement `source.ConsumerGroups`. A shared
  durable consumer load-balances across processes instead, surfaced as
  `source.SharedDurable`.
- **Replay is consumer-recreate.** A `source.Seekable` seek tears down the
  current consumer and rebuilds it with a `DeliverByStartTime` /
  `DeliverByStartSequence` / `DeliverAll` / `DeliverNew` policy. It is not an
  in-place cursor move; it takes effect on the next `Next`, and in-flight
  messages from the prior consumer are abandoned.
- **No `Transactional`.** JetStream offers no consume-side transaction, so the
  adapter does not implement `source.Transactional`; the capability is absent
  rather than faked.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
