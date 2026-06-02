# sink/eventbridge

A [`crucible/sink`](../) destination that puts events onto Amazon EventBridge.
Runtime dependencies: `crucible/sink` and the AWS SDK v2 EventBridge service client.

```go
reg := eventbridge.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[eventbridge.Client] {
    return eventbridge.PutEvent("orders", "com.example.orders", "OrderPlaced", `{"id":"`+o.ID+`"}`)
})

m := sink.NewManifold()
m.Attach(eventbridge.New(awsEBClient, reg))
m.Sink(ctx, OrderPlaced{ID: "ORD-1"})
```

`Client` is a narrow interface requiring only `PutEvents`, satisfied structurally
by `*eventbridge.Client` from the AWS SDK v2. Register a transformer per payload
type; an unregistered payload is skipped (`sink.ErrUnregistered`).

## Partial failures

EventBridge returns HTTP 200 even when some entries in a `PutEvents` call fail.
Both Op constructors inspect `PutEventsOutput.FailedEntryCount` and return an
error when it is greater than zero. The Emitter wraps that error as a
`*sink.Error` with `Phase == sink.PhaseApply` and `Outlet == "eventbridge"`.

## Op constructors

| Constructor | Description |
|---|---|
| `PutEvent(eventBusName, source, detailType, detail string)` | Puts a single event onto the named event bus. |
| `PutEvents(input *eventbridge.PutEventsInput)` | Puts one or more events with full SDK input control (resources, trace header, endpoint ID, etc.). |

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
