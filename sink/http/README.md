# sink/http

A [`crucible/sink`](../) destination that delivers payloads via HTTP POST using
the standard library's `net/http`. Runtime dependencies: the standard library and
`crucible/sink` only, no third-party HTTP client.

```go
reg := httpsink.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderShipped) sink.Op[httpsink.Doer] {
    body, _ := json.Marshal(o)
    return httpsink.Post("https://hooks.example.com/orders", "application/json", body)
})

m := sink.NewManifold()
m.Attach(httpsink.New(http.DefaultClient, reg))
m.Sink(ctx, OrderShipped{OrderID: "A-1"})
```

`Doer` is the narrow surface the destination needs (`Do(*http.Request) (*http.Response, error)`),
satisfied by `*http.Client`. A non-2xx response is returned as an error that
includes the status text. The response body is always drained and closed.
Register a transformer per payload type; an unregistered payload is skipped
(`sink.ErrUnregistered`).

`PostJSON` is a convenience constructor that marshals any value to JSON before
posting:

```go
sink.Register(reg, func(_ context.Context, o OrderShipped) sink.Op[httpsink.Doer] {
    return httpsink.PostJSON("https://hooks.example.com/orders", o)
})
```

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
