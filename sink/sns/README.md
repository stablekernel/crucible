# sink/sns

A [`crucible/sink`](../) destination that publishes payloads to Amazon SNS.
Runtime dependencies: `crucible/sink` and the AWS SDK v2 SNS service client.

```go
reg := sns.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderShipped) sink.Op[sns.Client] {
    return sns.Publish("arn:aws:sns:us-east-1:123456789012:orders", "shipped:"+o.ID)
})

m := sink.NewManifold()
m.Attach(sns.New(awsSNSClient, reg))
m.Sink(ctx, OrderShipped{ID: "ORD-1"})
```

`Client` is a narrow interface requiring only `Publish` and `PublishBatch`,
satisfied structurally by `*sns.Client` from the AWS SDK v2. Register a
transformer per payload type; an unregistered payload is skipped
(`sink.ErrUnregistered`).

## Op constructors

| Constructor | Description |
|---|---|
| `Publish(topicARN, message string)` | Sends a single message to a topic. |
| `PublishInput(input *sns.PublishInput)` | Sends with full SDK input control (subject, attributes, FIFO fields, etc.). |
| `PublishBatch(input *sns.PublishBatchInput)` | Sends up to ten messages in a single batch request. |

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
