# sink/bridge

Composes a [`crucible/state`](../../state) machine with a [`crucible/sink`](../)
`Manifold` so every state transition fans out to all attached destinations,
without either core importing the other.

```go
m := sink.NewManifold(sink.WithTracer(tr)).Attach(dynamo.New(...), http.New(...))

machine := state.Forge[S, E, C]("order").
    Use(bridge.Middleware[S, E, C](m, bridge.WithTracer(tr))). // fan transitions out
    // ... states and transitions ...
    Quench(state.Strict())

machine.Cast(order).Fire(ctx, Submit) // the transition fans out through m
```

Two adapters:

- **`Middleware`** wraps `Fire`. Because `Fire` carries a `context.Context`, the
  middleware starts a `state.transition` span and propagates its context into
  `Manifold.Sink`, so the `sink.Sink` span (and each outlet's span) nests under
  the transition span through the shared `crucible/telemetry` tracer. Use this
  when trace correlation matters.
- **`Inspector`** adapts a `Manifold` to `state`'s `Inspector` observer (wired
  with `state.WithInspector`). It is the one-line "fan everything out" path, but
  `state.Inspector` carries no context, so emit spans do not nest. See the note
  below.

## A note on the observer seam

`state.Inspector.Inspect(ev)` takes no `context.Context`, so the ergonomic
observer path cannot propagate trace context; correlation requires the
`Middleware` seam. A minimal additive context-carrying inspector variant in
`crucible/state` would let the observer path nest spans too; until then,
`Middleware` is the trace-correlating bridge.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
