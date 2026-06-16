# Changelog

Crucible uses **per-module SemVer**: each module is released and versioned on its
own line, so there is no single global version number. Each module keeps its own
`CHANGELOG.md` next to its code; this file is an index to them. See
[STABILITY.md](STABILITY.md) for what each module's stability label means.

## Stable

- [`state`](state/CHANGELOG.md) — statechart engine (v1.x, frozen contract)
- [`state/expr`](state/expr/CHANGELOG.md) — CEL-backed guards

## Tooling

- [`gen`](gen/CHANGELOG.md) — eject codegen
- [`cmd/crucible`](cmd/crucible/CHANGELOG.md) — headless IR CLI

## Host-side runtimes (experimental)

- [`durable`](durable/CHANGELOG.md) — durable-execution runtime
- [`cluster`](cluster/CHANGELOG.md) — distribution runtime
- [`wasm`](wasm/CHANGELOG.md) — WebAssembly guard runtime
- [`telemetry`](telemetry/CHANGELOG.md) — tracing/metrics seam
  ([`datadog`](telemetry/datadog/CHANGELOG.md), [`otel`](telemetry/otel/CHANGELOG.md) adapters)

## IO edges (experimental)

- [`sink`](sink/CHANGELOG.md) — egress fan-out. Each adapter keeps its own changelog
  under `sink/<name>/CHANGELOG.md` (bridge, cloudwatch, dynamo, eventbridge, file,
  firehose, gcppubsub, http, kafka, kinesis, nats, otel, prometheus, redis, s3, slog,
  sns, sql, sqs, statsd, timestream).
- `source` — ingress. Per-inlet changelogs:
  [`kafka`](source/kafka/CHANGELOG.md), [`statemachine`](source/statemachine/CHANGELOG.md).

## Examples

- [`examples/dispatch`](examples/dispatch/CHANGELOG.md)
