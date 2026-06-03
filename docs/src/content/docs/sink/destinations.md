---
title: Destinations
description: Each destination is its own optional module with a narrow client interface; an Emitter maps payload types to operations via a registry.
sidebar:
  order: 4
---

<!-- IMAGE-SLOT: sink-destination-molds: a rack of distinct casting molds (each etched with a destination glyph: a database, a cloud, a gauge, a stream), interchangeable on the same manifold runner; sky-squid swapping one in; 16:9 -->
![Interchangeable destination molds on one manifold](../../../assets/sink-destination-molds.png)

Every destination is its **own optional Go module**. The sink core imports no
vendor SDK; you add `crucible/sink/dynamo` only if you sink to DynamoDB, and its
AWS dependency never touches a service that does not. Each module exposes a
**narrow client interface** (only the methods it actually calls), so it is
easily faked in tests with no live cloud and no mocking framework.

## The Emitter pattern

Most destinations are an `Emitter`: a typed client `C` plus a `Registry` that
maps each payload type to an `Op[C]` (an operation against that client).

```go
type Op[C any] interface {
    Apply(ctx context.Context, client C) error
}
```

You register a transformer per payload type; an unregistered type returns
`ErrUnregistered`, which the Manifold treats as a silent skip. There is **no
package-global registry**: every `Registry` is constructed and injected, so two
emitters never share state.

```go
reg := sql.NewRegistry()
sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[sql.Tx] {
    return sql.Exec("INSERT INTO orders(id) VALUES (?)", o.ID)
})
m.Attach(sql.New(db, reg)) // db is *sql.DB, *sql.Tx, or *sql.Conn
```

A lookup miss is a skip; an `Apply` failure is wrapped as `*sink.Error` with
`PhaseApply`, the outlet name, and the payload type.

## Worked examples

- **`sql`**: stdlib `database/sql` through a narrow `Tx` interface
  (`ExecContext`), satisfied by `*sql.DB`, `*sql.Tx`, and `*sql.Conn`. **Zero
  driver dependency**: the model destination, and the one to read first.
- **`dynamo`**: Amazon DynamoDB over a narrow client interface covering the
  full write surface (`PutItem`, `UpdateItem`, `DeleteItem`, `TransactWriteItems`,
  `BatchWriteItem`). The richest `Op[Client]` example.
- **`statsd`**: a stateful **aggregator** that folds counts and gauges by
  `(name, tags)` and flushes to a DogStatsD client on an interval (injected
  clock). The stateful-outlet example, implementing `Flusher` and `Shutdowner`.

Their full APIs are in the [reference](/crucible/reference/) (`sink`,
`sink/sql`, `sink/dynamo`, `sink/statsd`).

## The full catalog

Every destination follows the same shape: narrow interface, `Op` constructors,
hand-rolled fakes, an `Example`, and a `sinktest.OutletConformance` check:

| Module | Destination |
|---|---|
| `sink/sql` | `database/sql` (stdlib, no driver dep) |
| `sink/dynamo` | Amazon DynamoDB |
| `sink/s3` | Amazon S3 |
| `sink/sqs` · `sink/sns` | Amazon SQS / SNS |
| `sink/kinesis` · `sink/firehose` | Amazon Kinesis / Data Firehose |
| `sink/eventbridge` | Amazon EventBridge |
| `sink/cloudwatch` | Amazon CloudWatch Logs |
| `sink/timestream` | Amazon Timestream |
| `sink/statsd` | StatsD / DogStatsD (aggregating) |
| `sink/otel` | OpenTelemetry metrics + span events |
| `sink/prometheus` | Prometheus Pushgateway (stdlib `net/http`) |
| `sink/http` | Webhook POST (stdlib `net/http`) |
| `sink/slog` | Structured log records (stdlib `log/slog`) |
| `sink/file` | Append-only JSONL (stdlib) |
| `sink/redis` | Redis Streams |
| `sink/nats` | NATS |
| `sink/kafka` | Apache Kafka |
| `sink/gcppubsub` | Google Cloud Pub/Sub |

The `http`, `slog`, and `file` destinations are **stdlib-only**, with no
third-party dependency at all.

## Verifying your own outlet

Writing a destination the suite does not ship? The `sinktest` package validates
any `Outlet` against the contract: skip-on-unregistered, error propagation, safe
concurrent use, idempotent flush and shutdown:

```go
func TestMyOutlet(t *testing.T) {
    sinktest.OutletConformance(t, func() sink.Outlet { return newMyOutlet() })
}
```

## Bring your own

Need a destination not listed, or a one-off? Skip the module entirely: a
`sink.OutletFunc` is an outlet, and `sink.NewEmitter` builds one from any client
type and registry you already have. The catalog is a convenience, never a
gate.
