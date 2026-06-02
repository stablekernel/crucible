# sink/prometheus

A [`crucible/sink`](../) destination that pushes metrics to a Prometheus
Pushgateway. Runtime dependencies: the standard library and `crucible/sink`
only -- no Prometheus client library.

```go
reg := prometheus.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[prometheus.Doer] {
    return prometheus.PushMetrics("http://pushgateway:9091", "orders", []prometheus.Metric{
        {
            Name:   "orders_placed_total",
            Type:   prometheus.TypeCounter,
            Value:  "1",
            Labels: map[string]string{"region": o.Region},
        },
    })
})

m := sink.NewManifold()
m.Attach(prometheus.New(http.DefaultClient, reg))
m.Sink(ctx, OrderPlaced{Region: "us-east-1"})
```

`Doer` is the narrow HTTP surface the destination needs (`Do(*http.Request)
(*http.Response, error)`), satisfied by `*http.Client` and any test double.

Two Op constructors are provided:

- `Push(gatewayURL, job, body string)` -- POST a pre-formatted Prometheus text
  exposition body.
- `PushMetrics(gatewayURL, job string, metrics []Metric)` -- serialize a
  `[]Metric` slice and POST it.

An unregistered payload is skipped (`sink.ErrUnregistered`). A non-2xx
response from the Pushgateway is returned as an error and wrapped by the
Emitter as `*sink.Error{Outlet:"prometheus", Phase:PhaseApply}`.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
