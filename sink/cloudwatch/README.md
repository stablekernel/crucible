# sink/cloudwatch

A [`crucible/sink`](../) destination that writes log events to Amazon CloudWatch Logs.
Runtime dependencies: `crucible/sink` and the AWS SDK v2 CloudWatch Logs service client.

```go
reg := cloudwatch.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[cloudwatch.Client] {
    return cloudwatch.PutLogEvent("/app/orders", "placed", "placed:"+o.ID)
})

m := sink.NewManifold()
m.Attach(cloudwatch.New(awsCWLogsClient, reg))
m.Sink(ctx, OrderPlaced{ID: "ORD-1"})
```

`Client` is a narrow interface requiring only `PutLogEvents`, satisfied structurally
by `*cloudwatchlogs.Client` from the AWS SDK v2. Register a transformer per payload
type; an unregistered payload is skipped (`sink.ErrUnregistered`).

## Op constructors

| Constructor | Description |
|---|---|
| `PutLogEvent(logGroup, logStream, message string)` | Sends a single log event with the current timestamp in milliseconds. |
| `PutLogEvents(input *cloudwatchlogs.PutLogEventsInput)` | Sends with full SDK input control (multiple events, sequence token, etc.). |

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
