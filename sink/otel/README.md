# sink/otel

A [`crucible/sink`](../) destination that turns payloads into OpenTelemetry
metric recordings and span events. Runtime dependency: the OpenTelemetry API
(`go.opentelemetry.io/otel`) plus `crucible/sink`.

```go
reg := otel.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[otel.Meter] {
    return otel.Counter("orders.placed", 1, attribute.String("id", o.ID))
})
sink.Register(reg, func(_ context.Context, p PaymentTaken) sink.Op[otel.Meter] {
    return otel.Histogram("payment.amount", p.Amount)
})

m := sink.NewManifold()
m.Attach(otel.New(meter, reg)) // meter is any metric.Meter from a MeterProvider
m.Sink(ctx, OrderPlaced{ID: "A-1"})
```

`Meter` is the narrow surface the destination needs (`Int64Counter`,
`Float64Histogram`, `Float64Gauge`), satisfied structurally by any
`metric.Meter`. Register a transformer per payload type; an unregistered payload
is skipped (`sink.ErrUnregistered`).

## Operations

| Constructor | Instrument / target | Method |
| ----------- | ------------------- | ------ |
| `Counter(name, delta, attrs...)`   | `Int64Counter`     | `Add`      |
| `Gauge(name, value, attrs...)`     | `Float64Gauge`     | `Record`   |
| `Histogram(name, value, attrs...)` | `Float64Histogram` | `Record`   |
| `SpanEvent(name, attrs...)`        | active span in ctx | `AddEvent` |

Metric instruments are created at apply time against the injected `Meter`; a
creation error is returned to the caller (the `Emitter` wraps it as a
`*sink.Error` with `sink.PhaseApply`). `SpanEvent` reads the span with
`trace.SpanFromContext` and is a no-op when no span is recording.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
