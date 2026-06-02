# sink/dynamo

A [`crucible/sink`](../) destination that persists payloads to Amazon DynamoDB
through the AWS SDK for Go v2. Runtime dependencies: the DynamoDB service client
(`github.com/aws/aws-sdk-go-v2/service/dynamodb`) and `crucible/sink`.

```go
reg := dynamo.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[dynamo.Client] {
    return dynamo.PutItem(&dynamodb.PutItemInput{
        TableName: aws.String("orders"),
        Item:      map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: o.ID}},
    })
})

m := sink.NewManifold()
m.Attach(dynamo.New(client, reg)) // client is *dynamodb.Client
m.Sink(ctx, OrderPlaced{ID: "A-1"})
```

`Client` is the narrow surface the destination needs (`PutItem`, `UpdateItem`,
`DeleteItem`, `TransactWriteItems`, `BatchWriteItem`), satisfied structurally by
`*dynamodb.Client`. Register a transformer per payload type; an unregistered
payload is skipped (`sink.ErrUnregistered`).

The write surface is exposed as `Op` constructors: `PutItem`, `UpdateItem`,
`DeleteItem`, `TransactWrite`, and `BatchWrite`. Each accepts the corresponding
`*dynamodb.XInput` and returns the raw SDK error, which the emitter wraps as a
`*sink.Error` with `PhaseApply`. `BatchWrite` returns only the request-level
error; per-item `UnprocessedItems` are not retried automatically.

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
