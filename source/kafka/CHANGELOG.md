# Changelog

All notable changes to `crucible/source/kafka` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] — 2026-08-23

The first stable release. It freezes the adapter surface — `Inlet` with `New`
and the `With*` options, the `Subscription` and its capability
implementations, the ack model below, and the sentinel errors — with the
neutral `source` seam carrying no franz-go types. The typed power seams
(`WithSASL`, `WithBalancer`, `WithClientOptions`, `WithClient`, `Inlet.As`,
`Message.As`) deliberately expose franz-go and may track franz-go releases;
everything else follows the frozen-contract terms of `source` v1.0.0.

### Added

- Kafka source ingress adapter: an `Inlet` (built with `New` and functional
  options — `WithSeedBrokers`, `WithSASL`, `WithTLS`, `WithBalancer`,
  `WithClientID`, `WithDLQTopic`, `WithMaxPollRecords`, `WithTransactional`,
  `WithStartOffset`, `WithClientOptions`, `WithClient`) opening a
  `source.Subscription` over franz-go. The consume loop polls with
  `PollRecords`, hands records to the engine as a neutral `source.Message`,
  and settles per the handler `Result` (ack model below).
- Capability interfaces, discovered by type assertion: `Seekable` (live
  `SetOffsets`, time seeks via `ListOffsets`; partitions enumerated from
  committed offsets with broker-metadata fallback, so seeking works on a cold
  group), `ConsumerGroups` (assign/revoke/lost hooks with drain-and-commit on
  revoke), `PartitionOrdered`, `LagReporter` (end-minus-committed across
  committed partitions; `ErrNoCommittedOffsets` on a cold group), and
  `Transactional` (Kafka EOS). `BlockRebalanceOnPoll` provides a safe
  processing window. The underlying `*kgo.Client` and `*kgo.Record` are
  reachable only through `As`.
- Exactly-once consume-process-produce. `WithTransactional(id)` builds a
  `GroupTransactSession` with read-committed fetch isolation and no
  auto-commit, and `Begin` runs fn inside a producer transaction: records
  produced through the handed `source.Tx` are flushed and `m`'s consumed
  offset is committed in one atomic unit, or the transaction aborts (on a
  work error or a rebalance fence) and the input is redelivered. Poison flows
  through Begin too (see Term below).
- `WithStartOffset(StartEarliest | StartLatest)`: typed cold-start policy for
  a brand-new consumer group (default earliest, franz-go's default);
  partitions with commits always resume from the commit.
- Sentinel errors `ErrNoCommittedOffsets` and `ErrTermInsideTransaction`.

### Ack model (frozen)

- `Ack` marks the record for commit (commit-after-process); marks commit on
  graceful drain and on revoke.
- `Nak` (plain or `NakAfter(d)`) never marks: the partition is paused,
  re-seeked to the record's offset, resumed — deterministic redelivery within
  the live subscription, with `d` as the pause delay (best-effort;
  head-of-line-blocks the partition; concurrent commits can pass the nacked
  offset, so cross-restart redelivery is best-effort).
- `Term` produces to the dead-letter topic, then marks. On a transactional
  subscription, direct `Term` reports `ErrTermInsideTransaction`; route
  poison through `Begin` (produce the DLQ record via `Tx`, return success) so
  the DLQ write and the offset commit are atomic.
- `InProgress`/`Manual` are no-ops. Duplicate settles are safe; duplicate
  marks are broker-idempotent.

### Changed

- `Nak` redelivery: plain `Nak` now re-seeks immediately (previously it only
  declined to mark, deferring redelivery to the next restart/rebalance). This
  makes the core at-least-once claim true within a live subscription.
- `SeekToStart/SeekToEnd/SeekToTime` no longer silently no-op before the first
  offset commit (metadata fallback); `Lag` errors with
  `ErrNoCommittedOffsets` instead of returning 0 for a cold group.
- After `Subscription.Close`, `Next`/`NextBatch` never poll again: they yield
  only already-buffered records, then block until in-flight settles land and
  return `source.ErrDrained` (previously a closed subscription with in-flight
  records could yield new records).
- Package documentation corrects the vendor-boundary claim: no franz-go type
  crosses the neutral seam; typed option seams deliberately expose franz-go.

### Removed

- The integration test's `testcontainers-go`, `redpanda`, and `kadm`
  requirements left this module's `go.mod`: the RedPanda end-to-end leg now
  lives in the test-only `source/kafka/integration` module, so downstream
  module graphs inherit only franz-go.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/source/kafka
[1.0.0]: https://github.com/stablekernel/crucible/releases/tag/source%2Fkafka%2Fv1.0.0
