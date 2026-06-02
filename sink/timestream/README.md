# sink/timestream

A [`crucible/sink`](../) destination that writes time-series records to Amazon
Timestream via the AWS SDK v2. Runtime dependencies: `crucible/sink` and
`github.com/aws/aws-sdk-go-v2/service/timestreamwrite`.

```go
reg := timestream.NewRegistry()
sink.Register(reg, func(_ context.Context, m MetricRecorded) sink.Op[timestream.Client] {
    db := "metrics"
    table := "readings"
    return timestream.WriteRecords(&timestreamwrite.WriteRecordsInput{
        DatabaseName: &db,
        TableName:    &table,
        Records:      []types.Record{{MeasureName: &m.Name, MeasureValue: &m.Value}},
    })
})

m := sink.NewManifold()
m.Attach(timestream.New(tsClient, reg)) // tsClient is *timestreamwrite.Client
m.Sink(ctx, MetricRecorded{Name: "cpu", Value: "0.42"})
```

`Client` is the narrow interface the destination needs (`WriteRecords`),
satisfied structurally by `*timestreamwrite.Client`. Register a transformer per
payload type; an unregistered payload is skipped (`sink.ErrUnregistered`).

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
