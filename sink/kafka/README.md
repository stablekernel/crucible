# sink/kafka

A [`crucible/sink`](../) destination that publishes payloads to Apache Kafka
through the pure-Go [franz-go](https://github.com/twmb/franz-go) client. Runtime
dependencies: the standard library, `crucible/sink`, and `franz-go` only.

```go
reg := kafka.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[kafka.Producer] {
    return kafka.Produce("orders", []byte(o.ID), []byte("placed"))
})

client, _ := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
defer client.Close()

m := sink.NewManifold()
m.Attach(kafka.New(kafka.NewProducer(client), reg))
m.Sink(ctx, OrderPlaced{ID: "A-1"})
```

`Producer` is the narrow surface the destination needs: a single `Produce`
method. `NewProducer` adapts a `*kgo.Client` onto it (calling `ProduceSync`),
which keeps the broker SDK out of every exported signature and lets tests drive
the destination with a hand-rolled fake. Register a transformer per payload
type; an unregistered payload is skipped (`sink.ErrUnregistered`).

The client's lifecycle is the caller's: this package neither dials nor closes
it.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
