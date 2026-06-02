# sink/nats

A [`crucible/sink`](../) destination that publishes payloads to a NATS subject.
Runtime dependencies: [`github.com/nats-io/nats.go`](https://github.com/nats-io/nats.go)
and `crucible/sink`.

```go
reg := nats.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[nats.Client] {
    return nats.Publish("orders.placed", []byte(o.ID))
})

nc, _ := gonats.Connect(gonats.DefaultURL)
m := sink.NewManifold()
m.Attach(nats.New(nc, reg)) // nc is *gonats.Conn
m.Sink(ctx, OrderPlaced{ID: "A-1"})
```

`Client` is the narrow interface the destination needs (`Publish` and
`PublishMsg`), satisfied structurally by `*nats.Conn`. Register a transformer
per payload type; an unregistered payload is skipped (`sink.ErrUnregistered`).

Use `Publish` for plain subject+data publishes. Use `PublishMsg` when you need
to set NATS headers, a reply subject, or other message-level fields.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
