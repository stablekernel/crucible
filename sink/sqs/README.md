# sink/sqs

A [`crucible/sink`](../) destination that publishes payloads to Amazon SQS.
Runtime dependencies: `crucible/sink` and
`github.com/aws/aws-sdk-go-v2/service/sqs`.

```go
reg := sqs.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderShipped) sink.Op[sqs.Client] {
    return sqs.SendMessage("https://sqs.us-east-1.amazonaws.com/123/orders", o.OrderID)
})

m := sink.NewManifold()
m.Attach(sqs.New(sqsClient, reg)) // sqsClient is *awssqs.Client or any Client
m.Sink(ctx, OrderShipped{OrderID: "ORD-42"})
```

`Client` is the narrow SQS surface the destination needs (`SendMessage`,
`SendMessageBatch`), satisfied structurally by `*sqs.Client` from
`github.com/aws/aws-sdk-go-v2/service/sqs`. Register a transformer per payload
type; an unregistered payload is skipped (`sink.ErrUnregistered`).

## Op constructors

| Constructor | Description |
|-------------|-------------|
| `SendMessage(queueURL, body string)` | Single message with a string body |
| `SendMessageFrom(input *sqs.SendMessageInput)` | Full SendMessageInput for attributes, delay, etc. |
| `SendMessageBatchOp(queueURL string, entries []types.SendMessageBatchRequestEntry)` | Up to 10 messages in one batch call |

`SendMessageBatchOp` returns an error if SQS reports any partial failure in
`out.Failed` (HTTP 200 with per-entry errors), so callers do not need to
inspect the output themselves.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
