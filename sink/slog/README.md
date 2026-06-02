# sink/slog

A [`crucible/sink`](../) destination that emits payloads as structured log
records via the standard library's `log/slog`. Runtime dependencies: the
standard library and `crucible/sink` only.

```go
reg := slogsink.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderShipped) sink.Op[*slog.Logger] {
    return slogsink.Log(slog.LevelInfo, "order.shipped",
        slog.String("order_id", o.ID),
    )
})

m := sink.NewManifold()
m.Attach(slogsink.New(logger, reg))
m.Sink(ctx, OrderShipped{ID: "O-1"})
```

The client is `*slog.Logger`. Op constructors:

| Constructor | Description |
|-------------|-------------|
| `Log(level, msg, attrs...)` | Emit at any `slog.Level` |
| `Info(msg, attrs...)` | Emit at `slog.LevelInfo` |
| `Error(msg, attrs...)` | Emit at `slog.LevelError` |

Register a transformer per payload type; an unregistered payload is skipped
(`sink.ErrUnregistered`).

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
