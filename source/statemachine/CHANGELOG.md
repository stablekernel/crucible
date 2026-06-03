# Changelog

All notable changes to `crucible/source/statemachine` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Drive` binding: routes a `source.Message` to an instance key and event, loads
  the instance through a `Store`, fires the event, hands the emitted effects to a
  `Sink`, persists the new state, and acks only after the durable commit.
- `DriveFunc` binding: the stateless mode, firing each message against a
  caller-supplied `FireFunc` with no persistence.
- `Store` interface and `Record` type for durable instance persistence, with the
  in-memory `MemStore`, `ErrConflict` for optimistic-concurrency races, and no
  hard dependency on a concrete backend.
- State-version idempotency: a redelivered `(key, eventID)` already applied
  returns `source.Skip`; the `EventID` extractor is configurable via
  `WithEventID` (`DefaultEventID` reads the `message-id` header, falling back to
  the cursor).
- State-aware rejection: an event illegal for the current state returns
  `source.Reject` carrying a `*source.GuardRejection`, distinct from a transient
  error's `source.Nak`.
- `CheckEvents`/`EventAlphabet`/`Conformance` for analyzable consumption: validate
  a router's event union against the machine's event alphabet.
- `WithSink`, `WithEventID`, `WithTracer`, and `WithSpanName` options.

[Unreleased]: https://github.com/stablekernel/crucible/tree/main/source/statemachine
