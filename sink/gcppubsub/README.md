# sink/gcppubsub

A [`crucible/sink`](../) destination that publishes payloads to Google Cloud
Pub/Sub. The SDK dependency is `cloud.google.com/go/pubsub/v2`.

```go
reg := gcppubsub.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[gcppubsub.Publisher] {
    return gcppubsub.Publish([]byte(o.ID), map[string]string{"type": "OrderPlaced"})
})

// client is a *pubsub.Client; reuse one publisher per topic.
topic := client.Publisher("orders")
defer topic.Stop()

m := sink.NewManifold()
m.Attach(gcppubsub.New(gcppubsub.Adapt(topic), reg))
m.Sink(ctx, OrderPlaced{ID: "A-1"})
```

`Publisher` is the narrow surface the destination needs: it publishes one
message and blocks for the server-assigned message ID. `Adapt` bridges a real
`*pubsub.Publisher` to it by calling `Publish` and then blocking on the result
handle's `Get`, turning the SDK's asynchronous publish into a synchronous
operation. Register a transformer per payload type; an unregistered payload is
skipped (`sink.ErrUnregistered`).

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
