# sink/statsd

A [`crucible/sink`](../) destination that aggregates payloads into StatsD
metrics and emits them on an interval and on demand. Runtime dependencies:
`crucible/sink` and one StatsD SDK (`github.com/DataDog/datadog-go/v5`), whose
client satisfies the narrow `Client` interface declared here.

```go
client, _ := statsd.Dial("127.0.0.1:8125") // or wire any Client implementation

reg := statsd.NewMetricRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) statsd.Metric {
    return statsd.Metric{Type: statsd.TypeCount, Name: "orders.placed", Int: 1, Rate: 1}
})

agg := statsd.NewAggregator(client,
    statsd.WithRegistry(reg),
    statsd.WithInterval(10*time.Second),
)

m := sink.NewManifold()
m.Attach(agg)
m.Sink(ctx, OrderPlaced{ID: "A-1"})
// m.Flush(ctx) and m.Shutdown(ctx) drive the aggregator's Flush and Shutdown.
```

The `Aggregator` is the primary surface. It folds counters (summed) and gauges
(last write wins) by metric identity (name plus sorted tags), buffers
histograms, distributions, timings, and sets as raw samples, and emits them to
the `Client` on the flush interval and on `Flush` or `Shutdown`. The buffer is
swapped atomically under a mutex so a slow client never blocks producers. It
implements `sink.Outlet`, `sink.Flusher`, and `sink.Shutdowner`.

`Client` is the narrow surface the destination needs (`Count`, `Gauge`,
`Histogram`, `Distribution`, `Timing`, `Set`), satisfied structurally by the
StatsD SDK client. A payload with no folding rule is skipped
(`sink.ErrUnregistered`).

A secondary Emitter path (`NewRegistry` and `New`, plus the `Count`/`Gauge`/...
`Op` constructors) emits one StatsD call per `Sink` with no in-process
aggregation, for cases where a downstream agent already aggregates.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
