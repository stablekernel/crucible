# sink/sql

A [`crucible/sink`](../) destination that persists payloads through the standard
library's `database/sql`. Runtime dependencies: the standard library and
`crucible/sink` only — no driver, no ORM.

```go
reg := sql.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[sql.Tx] {
    return sql.Exec("INSERT INTO orders(id) VALUES (?)", o.ID)
})

m := sink.NewManifold()
m.Attach(sql.New(db, reg)) // db is *sql.DB, *sql.Tx, or *sql.Conn
m.Sink(ctx, OrderPlaced{ID: "A-1"})
```

`Tx` is the narrow surface the destination needs (`ExecContext`), satisfied by
`*sql.DB`, `*sql.Tx`, and `*sql.Conn`. Register a transformer per payload type;
an unregistered payload is skipped (`sink.ErrUnregistered`).

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
