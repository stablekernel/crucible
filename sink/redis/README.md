# sink/redis

A [`crucible/sink`](../) destination that publishes payloads to Redis. It
supports two write surfaces: Redis Streams ([XAdd]) and Pub/Sub ([Publish]).
Runtime dependency: `github.com/redis/go-redis/v9`.

```go
reg := redis.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[redis.Client] {
    return redis.XAdd("orders", map[string]any{"order_id": o.OrderID})
})
sink.Register(reg, func(_ context.Context, n NotificationSent) sink.Op[redis.Client] {
    return redis.Publish("alerts", n.Body)
})

m := sink.NewManifold()
m.Attach(redis.New(rdb, reg)) // rdb is *goredis.Client
m.Sink(ctx, OrderPlaced{OrderID: "A-1"})
```

`Client` is the narrow surface the destination needs (`XAdd`, `Publish`),
satisfied structurally by `*redis.Client` from `github.com/redis/go-redis/v9`.
Register a transformer per payload type; an unregistered payload is skipped
(`sink.ErrUnregistered`).

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
