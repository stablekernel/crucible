# Changelog

All notable changes to `crucible/source` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] — 2026-08-23

Release engineering only: no code changes. The v1.0.0 tag's release workflow
failed for environment reasons (the validate job built with the repository
workspace present, and golangci-lint could not run on the Go 1.25 matrix leg
against a module targeting a newer language version); v1.0.1 republishes the
identical source so the tag validates cleanly end to end.

## [1.0.0] — 2026-08-23

The first stable release. It freezes the neutral ingress surface — `Inlet`,
`Subscription`, `Message`/`Headers`/`Cursor`, `Handler`/`BatchHandler`/`Result`
with the `Ack`/`Nak`/`NakAfter`/`Term`/`Reject`/`Skip`/`InProgress`/`Manual`
vocabulary, `Middleware`/`Chain`, the `Hopper` engine, every `Option`, the
capability interfaces (`Seekable`, `ConsumerGroups`, `PartitionOrdered`,
`LagReporter`, `Transactional`, `Batched`, `Idempotent`), and the sentinel
errors — under the same frozen-contract terms as `state`: from 1.0.0 onward,
new capabilities arrive as additive packages, modules, and options rather than
breaking changes.

### Delivery contract (frozen)

- Delivery is at-least-once within a live subscription: a message is acked only
  after its handler reports success, and `Nak` redelivers. Across process
  restarts and rebalances, backends whose redelivery rides persisted offsets
  (Kafka) resume from the last committed position, which a concurrently
  committed higher offset can advance past a nacked record; those backends
  redeliver exactly within a session and best-effort across restarts. Each
  adapter documents its precise semantics (see `source/kafka/README.md`).
- `Subscription.Close` begins a graceful drain: after it returns, `Next`/
  `NextBatch` never yield new messages — only already-buffered ones — and once
  in-flight messages settle, `Next` reports `ErrDrained`.
- Duplicate `Settle` calls for one message are safe: they never corrupt
  in-flight accounting or double-advance a backend position.

### Added (this cycle)

- `ErrDrained` drain semantics pinned by regression tests for both `Next` and
  `NextBatch` (close-then-Next, redelivery storm with bounded goroutines,
  drain-on-cancel with a mid-flight backend settle, duplicate-settle
  idempotence).

### Changed

- `ActionNak` documentation now states per-backend redelivery semantics in one
  voice with the adapters: JetStream naks natively (with optional delay);
  Kafka pauses and re-seeks the partition so the record is fetched again
  in-session, honoring `Result.Requeue` as a pause delay.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/source
[1.0.0]: https://github.com/stablekernel/crucible/releases/tag/source%2Fv1.0.0
