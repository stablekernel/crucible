# Changelog

All notable changes to `crucible/durable` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`Record.Actors` and `RunningActors`** record the child-machine actors running after
  a step, when it changed them, so `Recover` spawns only those. A Store persists the field
  like the others.

- **Fencing.** `WithFence` on `Store.Append` and `WithCheckpointFence` on
  `Store.Checkpoint` carry a caller-issued token, and a Store refuses a token lower
  than the highest it has seen for the instance with `ErrFenced`, writing nothing.
  `WithOwnerFence` sets the token a Runner carries on every Append and Checkpoint;
  `MarkDispatched` is not fenced. `AppendFence`,
  `CheckpointFence` and `AppendIdempotencyKey` let a backend outside this package
  read its options. `MemStore` and `FileStore` enforce fencing; `FileStore` persists the
  highest token in a `fence` file and holds its fencing within one open store. A
  Store first sees a token on that owner's first write, so `Recover` alone doesn't
  fence the old owner: a stale write before then is accepted, and the new owner's
  write at that step gets `ErrStepConflict`.

- **`WithMaxEventDataBytes`** caps a `WithEventData` payload. `ErrEventDataEncoding` reports
  a refused payload, and `ErrRecordTooLarge` a Record too large for a `FileStore` line.
  `Record.EventData` and `Record.EventDataError` hold the payload.

- **`ErrHandleFailed`** reports a Handle out of step with its journal.

- **`Handle.View`** reads the instance under the Handle's step lock.

- **`Handle.Drain`** waits until no step of the Handle is still recording and no effect
  is still being sent. Call it before `Recover` after `ErrHandleFailed`, with a context
  of its own, and don't `Recover` until it returns nil. On a healthy Handle it covers
  only work already under way.

- **`ErrReentrantStep`** reports a call back into a Handle that carries the context one
  of its steps passed to a callback: a service's context, or the `Ctx` a guard, action
  or reducer gets (#245). A call back without that context deadlocks instead.

- **`StepReporter`**, implemented by `MemStore` and `FileStore`, lets `Recover` number the
  next step past every recorded one after a checkpoint from before checkpoints carried
  it. Without it, such a checkpoint followed by tick barriers made every `Append`
  conflict.

- **`WithEventDataDecoder`** decodes a `WithEventData` payload as the caller's type,
  given the event it rode with. `DecodeAs[T]` is the usual body of each case.

### Changed

- **A journal this version writes isn't for an older one.** An actor entry can now say
  its parent Fire was refused, which an older `durable` fails `Recover` on, or, for an
  entry with no event, replays as the zero event. Don't downgrade a Store this version
  has written to.
- **A journal an older version wrote replays as it did.** Its actor entries carry no
  refusal, so `Recover` still fails on one whose parent Fire was refused, and an output
  recorded with a delivery's last Fire still replays into that transition. Only steps
  this version records get the fixes below.
- Requires Go 1.27.4.
- **A Handle's steps are serialized across goroutines.** `Fire`, `Tick`, `RunService` and
  `DeliverToActor` hold a lock while they apply to the kernel and record the step, so a
  host's timer loop no longer loses acknowledged steps. Effects dispatch after the lock
  is released, so an effect handler can still call back into its Handle; a checkpoint
  waits for every earlier step's effects to dispatch. A service, guard, action, reducer
  or actor behavior runs under the lock, so it can't act on the Handle itself; an action
  reaches the Handle by returning an effect. A service, guard, action or reducer that
  calls back with the context its step passed gets the new `ErrReentrantStep`. A step
  waiting for the lock returns its context's error once the context ends, and changes
  nothing. The new `Handle.View` reads the instance under the lock; `Instance()` is
  outside it.
- **A failed effect dispatch fails the Handle**, and so does an effect handler that
  panics, which now returns `ErrEffectDispatch` instead of panicking out of `Fire`. The
  effect stays unmarked in the journal, so no later checkpoint compacts it away, and
  `Recover` dispatches it again.
- **A step that may not have been recorded fails the Handle.** That call and every later
  one return `ErrHandleFailed`, wrapping the cause. A step that was recorded while
  another goroutine failed the Handle also returns `ErrHandleFailed`, saying the step was
  recorded; `Recover` sends any of its effects. Call `Drain` before `Recover`, so a
  step another goroutine is still recording is loaded, and an effect it is still sending
  isn't sent twice. `Recover` a new Handle
  with the same options to continue. A service whose result wasn't recorded is in flight again, so
  `RunService` runs it a second time. An `InvokeActor` actor restarts from its initial
  state, and a `Spawn` actor doesn't return. If the cause is `ErrFenced`, a newer owner
  holds the instance: stop.
- **A `WithEventData` payload reaches `Assign` decoded, live as well as on replay.**
  `WithEventDataDecoder` decodes it as the caller's type, chosen per event, with
  `DecodeAs[T]`. A payload that decodes as a different type than the caller passed
  refuses the `Fire` with `ErrEventDataEncoding`, so without a decoder a struct or an
  integer is refused rather than reaching `Assign` as a map or a `float64`. An error
  arrives as an error with the same message, any invalid UTF-8 replaced, and `errors.Is`
  no longer matches it.
- **`Handle.Fire` can refuse a payload.** One with no JSON form that decodes again, or
  over `WithMaxEventDataBytes` (8 MiB by default), returns the new `ErrEventDataEncoding`
  before the kernel runs. Fires of that size used to pass, because the payload wasn't
  journaled.
- **`FileStore.Append` refuses a Record over its 16 MiB line limit** with the new
  `ErrRecordTooLarge`. It used to write a line `Load` couldn't read back unless a
  checkpoint compacted it away. An oversized line now drops its `Snapshot` first, which
  recovery never reads, so large checkpoints keep working.
- **A `Store` implementation has new work to do.** It must enforce fence tokens, return
  `ErrStepConflict` and keep `Record.EventData` and `Record.EventDataError`, or
  `WithOwnerFence` protects nothing and payloads are lost. Existing implementations
  still compile, so nothing flags this.
- **`MemStore.Append` without `WithIdempotencyKey` refuses a Record JSON can't encode,**
  as `FileStore` already did.

- **`Store.Append` rejects a different record for a recorded step.** It used to keep
  the first and report success, which acknowledged a step the journal didn't hold.
  It now returns `ErrStepConflict`, including for a step a checkpoint has compacted.
  The same record is still an idempotent no-op, and an Append with
  `WithIdempotencyKey` still dedups on the key alone. A `FileStore` ledger line from
  before this change has no digest, so its step keeps the old no-op behavior. A
  recovered instance that reuses a step number with a different record now fails with
  `ErrStepConflict` instead of silently dropping the step.

### Fixed

- **A failed checkpoint no longer makes the next step conflict.** `Tick`, `RunService`
  and `DeliverToActor` now advance the next step as soon as the append lands.
- **A deferred checkpoint survives an unrelated caller's cancel.** A checkpoint that
  waited for an earlier step's effects to dispatch, and was canceled by the context of
  the caller whose step finished last, isn't reported to that caller, since its own
  step succeeded, and the next step writes it; a failure of that retry is returned to
  the step that tried it. A retry the Store refuses as not advancing means the first
  write landed, and counts as written. Any other checkpoint failure is returned, and
  after that only the next step the policy makes due tries again.
- **A Tick, RunService or DeliverToActor whose context ended partway isn't journaled.** A canceled `RunService`, `Tick`
  or `DeliverToActor` used to record a result the live instance never applied, which
  recovery then committed, or for an actor, failed on. Each now returns `ErrHandleFailed`
  wrapping the error and records nothing. One rule covers them: a step records nothing
  once its context ended by the end of the step, or a Fire it drove was refused with a
  context's error, which catches a context middleware derived from the caller's. A
  callback that returns its own context error, such as an HTTP client's timeout, counts
  as a context ending too.
  A Fire refused for another reason, such as a failing guard, is still recorded, and
  replay now meets the same refusal instead of failing Recover on it. After `Recover`
  the service stays in flight and must tolerate running again, the timer is re-armed, an
  `InvokeActor` actor restarts from its initial state, and a `Spawn` actor doesn't
  return. `Recover` and `StateAt` now fail on a canceled context, rather than returning
  state short of the journal.
- **`Fire` follows the same rule for the actors it spawns.** When an actor a Fire's
  state spawned failed to start and a context's ending refused its onError, the Fire was
  recorded, and `Recover` then applied the onError live never applied. Now `Fire` returns
  `ErrHandleFailed` and records nothing, though its event applied: fire it again on the
  recovered Handle. A Fire that drove no Fire a context refused still records, even when its
  context ends once the kernel has applied the event.
- **An actor's output replays with the Fire that carried it.** When one delivery drove
  several parent Fires, such as an onDone entering a state whose actor then failed to
  start, the output was recorded with the last Fire, so replay gave it to the wrong
  transition. It's recorded with the settling Fire, which `state`'s new
  `ActorSystem.WithParentFireObserver` identifies.
- **`Recover` spawns only the actors the live run had running.** It spawned every actor
  the recovered state declared, so an actor that had settled or been stopped while its
  parent stayed in that state ran again: a settle the parent refused, one that applied
  without leaving the state, or an explicit stop. Each step now records the set of
  running actors when it changes, in the new `Record.Actors`, and a checkpoint keeps it,
  so `Recover` spawns the declared actors that set holds. A journal from before this
  records no set, and `Recover` spawns every declared actor, as before.
- **A parent Fire that middleware denied replays.** Denied before the kernel saw it, it
  is recorded as refused, and replay meets the same refusal instead of failing `Recover`.
- **A panic during a step fails the Handle.** A panic in host code under the step lock,
  such as an actor behavior a Fire's absorb started, left the Handle healthy but ahead of
  its journal, so the next step recorded on top of state the journal never held and
  `Recover` failed. The Handle now fails first, and the panic goes on to the caller.
- **The next step survives a checkpoint that compacts tick barriers.** A checkpoint
  now records the step that follows it, in a `nextStep` field of its envelope. Recovery
  used the Trace count, which runs low past a tick barrier, so the next Fire could reuse a
  recorded step, and high past a multi-Trace actor delivery, which skipped ordinals. A
  checkpoint written before this change can still reuse a step: a different record fails
  with `ErrStepConflict`, and an identical one is acknowledged without a new Record.
- **`WithEventData` survives recovery.** `Handle.Fire` records the payload in the new
  `Record.EventData` and `Record.EventDataError`, and `Recover` and `StateAt` supply it
  on replay. This needs `state.FireEventData` (and the observer above needs
  `state.ActorSystem.WithParentFireObserver`), so `durable` requires `state` v1.1.0 or
  later, and its `require` must move there before a `durable` tag.
- **Recording-clock buffer race.** Closing a step's `Record` (draining the
  recording clock) and the clock-read index reads used to mirror timer deadlines
  now take the recording clock's mutex, so they are safe against a `Now()` the
  scheduler reads from a host's timer goroutine — the concurrency the recording
  clock's documentation promises.
- **FileStore directory durability.** Atomic checkpoint writes now fsync the
  parent directory after the rename, so the rename itself survives a crash and the
  documented crash-durability holds, not only the file's bytes. The directory sync
  is split by build tag (`filestore_dirsync_other.go` for POSIX, a no-op
  `filestore_dirsync_windows.go` for Windows) because Windows does not permit
  opening a directory handle for sync — the file rename plus the file's own fsync
  are sufficient for Windows crash durability.
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
