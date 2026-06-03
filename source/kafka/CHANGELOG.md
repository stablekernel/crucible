# Changelog

All notable changes to `crucible/source/kafka` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Kafka source ingress adapter: an `Inlet` (built with `New` and functional
  options — `WithSeedBrokers`, `WithSASL`, `WithTLS`, `WithBalancer`,
  `WithClientID`, `WithDLQTopic`, `WithMaxPollRecords`, `WithTransactional`,
  `WithClientOptions`, `WithClient`) opening a `source.Subscription` over
  franz-go. The consume loop polls with `PollRecords`, hands records to the
  engine as a neutral `source.Message`, and settles per the handler `Result`:
  Ack marks for commit (`AutoCommitMarks`, commit-after-process), Nak declines
  to mark (best-effort delay via pause + re-seek + resume), Term produces to the
  configured dead-letter topic then marks, InProgress and Manual are no-ops.
- Capability interfaces, discovered by type assertion with no vendor types in
  the exported surface: `Seekable` (live `SetOffsets`, time seeks via
  `ListOffsets`), `ConsumerGroups` (assign/revoke/lost hooks with
  drain-and-commit on revoke), `PartitionOrdered`, `LagReporter`, and
  `Transactional` (Kafka EOS). `BlockRebalanceOnPoll` provides a safe
  processing window. The underlying `*kgo.Client` and `*kgo.Record` are
  reachable only through `As`.
- Exactly-once consume-process-produce. `WithTransactional(id)` builds a
  `GroupTransactSession` with read-committed fetch isolation and no auto-commit,
  and the subscription's `source.Transactional.Begin(ctx, m, fn)` runs `fn`
  inside a producer transaction: records produced through the handed `source.Tx`
  are flushed and `m`'s consumed offset is committed in one atomic unit, or the
  transaction aborts (on a work error or a rebalance fence) and the input is
  redelivered. Produced records are neutral `source.ProducedRecord` values; no
  franz-go type crosses the seam.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/source/kafka
