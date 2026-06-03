# sink

Fire-and-forget fan-out emitter for the Crucible suite. One `Manifold.Sink(ctx,
payload)` call fans a payload out to every attached destination (SQL, DynamoDB,
StatsD, a webhook, a log) without the call site knowing which are wired.

```go
m := sink.NewManifold(sink.WithLogger(log), sink.WithTracer(tr), sink.WithMeter(mt))
m.Attach(
    dynamo.New(client, dynRegistry),
    sink.Reservoir(s3.New(client, s3Registry), sink.WithBatchSize(100)),
)
m.Sink(ctx, payload)   // fire-and-forget fan-out; the only emit path
defer m.Shutdown(ctx)

// Need confirmation for one critical destination? Talk to it directly:
if err := auditOutlet.Sink(ctx, payload); err != nil { /* retry, 500, ... */ }
```

- **Fire-and-forget.** `Manifold.Sink` returns nothing; outlet failures go to the
  logger and the `sink.failed` metric. A destination held directly returns an
  honest per-call error.
- **Dependency-light core.** The core imports only the standard library and
  `crucible/telemetry`. Each destination is its own module, so you pull only the
  SDKs you use.
- **No globals.** Every `Registry` is constructed and injected; no `init` state.
- **Deterministic time.** `Reservoir` and `Poller` take an injected clock.
- **Conformance.** `sinktest.OutletConformance` validates any `Outlet` against
  the contract.

## Stability

Experimental (pre-v1). Feature-complete and intended to become v1.0.0 after
cross-module review; until then the API may change.

## License

Apache-2.0. See [LICENSE](../LICENSE) and [NOTICE](../NOTICE).
