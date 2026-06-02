# sink/s3

A [`crucible/sink`](../) destination that persists payloads to Amazon S3 via
the AWS SDK v2. Runtime dependencies: `crucible/sink` and
`github.com/aws/aws-sdk-go-v2/service/s3`.

```go
reg := s3sink.NewRegistry()
sink.Register(reg, func(_ context.Context, e DocumentUploaded) sink.Op[s3sink.Client] {
    return s3sink.PutObject(e.Bucket, e.Key, e.Body)
})

m := sink.NewManifold()
m.Attach(s3sink.New(awsS3Client, reg))
m.Sink(ctx, DocumentUploaded{Bucket: "my-bucket", Key: "docs/report.pdf", Body: data})
```

`Client` is a narrow interface covering only `PutObject` and `DeleteObject`.
The real `*s3.Client` satisfies it without any adapter. Register a transformer
per payload type; an unregistered payload is skipped (`sink.ErrUnregistered`).

## Operations

| Constructor        | Description                                        |
| ------------------ | -------------------------------------------------- |
| `PutObject`        | Upload `[]byte` body to a bucket/key               |
| `PutObjectWith`    | Upload using a pre-built `*s3.PutObjectInput`      |
| `DeleteObject`     | Remove an object by bucket/key                     |
| `DeleteObjectWith` | Remove using a pre-built `*s3.DeleteObjectInput`   |

## Stability

Experimental (pre-v1). The API may change until the suite locks v1.0.0.

## License

Apache-2.0. See [LICENSE](../../LICENSE) and [NOTICE](../../NOTICE).
