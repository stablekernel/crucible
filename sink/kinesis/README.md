# sink/kinesis

A [`crucible/sink`](../) destination that writes payloads to Amazon Kinesis
Data Streams. Runtime dependencies: `crucible/sink` and the AWS SDK v2
Kinesis service client.

```go
reg := kinesis.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderShipped) sink.Op[kinesis.Client] {
    return kinesis.PutRecordOf(kinesis.PutRecordParams{
        StreamName:   "orders",
        PartitionKey: o.OrderID,
        Data:         []byte(o.OrderID),
    })
})

m := sink.NewManifold()
m.Attach(kinesis.New(kinesisClient, reg)) // kinesisClient is *kinesis.Client from aws-sdk-go-v2
m.Sink(ctx, OrderShipped{OrderID: "ord-42"})
```

`Client` is a narrow two-method interface (`PutRecord`, `PutRecords`) satisfied
structurally by `*github.com/aws/aws-sdk-go-v2/service/kinesis.Client`. Tests
use a hand-rolled fake, no AWS credentials needed.

## Op constructors

| Constructor | Description |
|---|---|
| `PutRecord(input)` | Single record via a `*kinesis.PutRecordInput` you build. |
| `PutRecordOf(params)` | Single record via stream name/ARN, partition key, and bytes. |
| `PutRecords(input)` | Batch of records via a `*kinesis.PutRecordsInput` you build. |
| `PutRecordsOf(stream, arn, entries)` | Batch of records via convenience entry slice. |

Unregistered payload types are skipped (`sink.ErrUnregistered`). Apply errors
are wrapped by the Emitter as `*sink.Error` with `Phase == PhaseApply` and
`Outlet == "kinesis"`.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
