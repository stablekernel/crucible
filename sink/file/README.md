# sink/file

A [`crucible/sink`](../) destination that appends one JSON line per payload to
an `io.Writer` or an append-only file. Runtime dependencies: the standard
library and `crucible/sink` only.

```go
// Write to any io.Writer (in-memory, network, etc.)
outlet := file.New(os.Stdout)
_ = outlet.Sink(ctx, OrderShipped{OrderID: "ORD-1", SKU: "WIDGET"})
// stdout: {"order_id":"ORD-1","sku":"WIDGET"}

// Or open and own a file:
outlet, err := file.Open("/var/log/events.jsonl")
if err != nil { /* handle */ }
defer outlet.(sink.Shutdowner).Shutdown(ctx)

m := sink.NewManifold()
m.Attach(outlet)
m.Sink(ctx, OrderShipped{OrderID: "ORD-2", SKU: "GIZMO"})
```

Each payload is marshaled with `encoding/json` and written as a single line
followed by a newline (`\n`), producing a valid
[JSONL](https://jsonlines.org/) stream. The outlet accepts every payload type
without a registry; a marshal failure returns a `*sink.Error` with
`PhaseApply` and `Outlet=="file"`.

Optional capabilities:

- **`sink.Flusher`**: calls `Sync()` on the underlying writer when available
  (e.g. `*os.File`). A no-op for writers that do not implement `Sync`.
- **`sink.Shutdowner`**: closes the file when the outlet was created by
  `Open`. Idempotent; a no-op for `New`-created outlets.

The outlet is safe for concurrent use.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
