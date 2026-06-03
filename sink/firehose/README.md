# sink/firehose

A [`crucible/sink`](../) destination that writes payloads to Amazon Data
Firehose. Runtime dependencies: `crucible/sink` and the AWS SDK v2 Firehose
service client.

```go
reg := firehose.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderShipped) sink.Op[firehose.Client] {
    return firehose.PutRecordOf("orders", []byte(o.OrderID))
})

m := sink.NewManifold()
m.Attach(firehose.New(firehoseClient, reg)) // firehoseClient is *firehose.Client from aws-sdk-go-v2
m.Sink(ctx, OrderShipped{OrderID: "ord-42"})
```

`Client` is a narrow two-method interface (`PutRecord`, `PutRecordBatch`)
satisfied structurally by
`*github.com/aws/aws-sdk-go-v2/service/firehose.Client`. Tests use a
hand-rolled fake, no AWS credentials needed.

## Op constructors

| Constructor | Description |
|---|---|
| `PutRecord(input)` | Single record via a `*firehose.PutRecordInput` you build. |
| `PutRecordOf(deliveryStream, data)` | Single record via delivery stream name and bytes. |
| `PutRecordBatch(input)` | Batch of records via a `*firehose.PutRecordBatchInput` you build. |

`PutRecordBatch` returns `ErrPartialFailure` (wrapping the count) when the
SDK call succeeds but `FailedPutCount > 0`, so partial failures are never
silently dropped.

Unregistered payload types are skipped (`sink.ErrUnregistered`). Apply errors
are wrapped by the Emitter as `*sink.Error` with `Phase == PhaseApply` and
`Outlet == "firehose"`.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
