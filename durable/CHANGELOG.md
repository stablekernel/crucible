# Changelog

All notable changes to `crucible/durable` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Recording-clock buffer race.** Closing a step's `Record` (draining the
  recording clock) and the clock-read index reads used to mirror timer deadlines
  now take the recording clock's mutex, so they are safe against a `Now()` the
  scheduler reads from a host's timer goroutine — the concurrency the recording
  clock's documentation promises.
- **FileStore directory durability.** Atomic checkpoint writes now fsync the
  parent directory after the rename, so the rename itself survives a crash and the
  documented crash-durability holds, not only the file's bytes.
- **Actor replay error.** A recorded actor done-data payload that fails to decode
  is now surfaced as a replay error instead of being silently dropped, so a
  corrupt journal fails loudly rather than re-firing the parent with nil data.

### Removed

- **`WithRetainTail`.** The per-checkpoint option was inert: time-travel retention
  is a store-level capability (`NewMemStore` `WithHistory`, the `HistoryStore`
  seam), so the option changed nothing. The `CheckpointOption` seam itself is
  retained for additive per-checkpoint policy.

## [0.1.0]

The first release of the host-side durable-execution runtime for the
`crucible/state` kernel. The runtime is additive over the kernel: it consumes
the already-reserved `Snapshot.Journal`, `EffectEnvelope.EffectID`, and
injectable `Clock` / `ServiceRunner` / `ActorSystem` seams without changing the
kernel, which stays pure and stdlib-only.

### Added

- **Durable runtime.** `Runner` wraps a `state.Machine` and a `Store`. `Start`
  creates a fresh instance (persisting a baseline checkpoint); `Runner.Fire`
  drives an instance statelessly (load, replay, fire, re-record per call);
  `Handle.Fire` drives a held instance directly for hot paths. Every step is
  write-ahead appended before it is acknowledged.
- **Recovery.** `Recover` reconstructs an instance purely from the `Store`
  (loading the latest checkpoint and replaying the recorded tail through the pure
  transition function) and returns a `Handle` that continues recording.
- **Three nondeterministic seams.** A recording clock (`WithRunnerClock`)
  journals every `Now()` reading so timers fire at the same recorded instants on
  recovery, wall-clock-independent, with armed deadlines surviving checkpoint
  compaction; invoked services (`WithServiceRegistry`, `Handle.RunService`) run
  once and replay their recorded result; child-machine actors
  (`WithActorPalette`, `Handle.DeliverToActor`) run once and replay the recorded
  parent transition.
- **Exactly-once effect dispatch.** Domain effects routed through the kernel are
  applied through a caller-supplied `EffectHandler` (`WithEffectHandler`) exactly
  once over the instance's lifetime, deduplicated by a deterministic `EffectID`
  through the `Store`'s dispatch set.
- **Stores.** The `Store` interface plus two stdlib-only reference
  implementations: `MemStore` (in-memory, thread-safe, `WithHistory` for full
  record retention) and `FileStore` (on-disk, append-only journal with atomic
  checkpoints and crash-safe write-temp-plus-rename). Persistent database
  backends implement `Store` out of tree.
- **Time-travel reader.** `StateAt` reconstructs an instance's state as of any
  recorded step, read-only, running no service, actor, clock, or effect. Backed
  by the `HistoryStore` seam, falling back to `Load` otherwise.
- **Tuning options.** `WithCheckpointEvery` to bound recovery replay,
  `WithEventCodec` for event serialization, and a per-append idempotency key
  (`WithIdempotencyKey`).

[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/durable/v0.1.0
