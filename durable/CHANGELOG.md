# Changelog

All notable changes to `crucible/durable` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`Record.EndedActors` and `ActorSet`** record the declared child-machine actors that
  ran and have ended, settled or stopped, so `Recover` doesn't spawn them again. A Store
  must keep an empty set distinct from nil.
- **`Record.UnstartedActors`** records the declared actors that aren't running and
  haven't ended, because their spawn failed with no onError declared, or its
  onError was refused because a context ended, so `Recover` can tell them
  from the actors that were running. A Store must keep an empty set distinct from nil.
- **`Record.Format`** records the journal format of the version that wrote the Record,
  which is 2 from this version. Replay arms the timers and services an actor-driven
  parent Fire's state declares only for a Record of format 1 or later. `Recover` sends a
  tick's and a service settle's effects, and replay hands a tick's timer transitions'
  effects to the service runner, only for a Record of format 2 or later.
  **For Store authors:** a Store must keep `Format`, `EndedActors` and
  `UnstartedActors`, and keep an empty set distinct from nil. A Store that drops them, or
  maps an empty set to nil as an SQL array column or a protobuf repeated field can,
  makes `Recover` spawn ended actors again, and replay arm nothing for a Record without
  its `Format`. MemStore and FileStore keep all three.

- **`EffectInstance`** reads the instance from the ctx an `EffectHandler` receives, because
  effect ids repeat across instances.
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

- **An effect handler needs a `DispatchStore`.** This is a behavior change. On main, a
  Runner with `WithEffectHandler` and a Store that doesn't implement `DispatchStore`
  silently skipped every effect: `Fire`'s, and `Recover`'s re-send, never reached the
  handler. Now `Start`, `Recover` and the stateless `Runner.Fire` refuse that
  configuration with the new `ErrNoDispatchStore`, before they read or write the Store
  or call the handler. It is a permanent configuration error and doesn't match
  `ErrEffectDispatch`, so a caller that retries `Recover` on a dispatch failure doesn't
  loop on it. A Store with no handler wired is unaffected, and
  `MemStore` and `FileStore` implement `DispatchStore`. A Store author whose backend
  doesn't track dispatched effects either implements `Dispatched` and
  `MarkDispatched` or drops the handler.
- **A journal this version writes isn't for an older one.** An actor entry can now say
  its parent Fire was refused, which an older `durable` fails `Recover` on, or, for an
  entry with no event, replays as the zero event. An older `durable` also ignores the
  recorded ended actors and spawns them again, and replays a step that records actor
  entries as a delivery, skipping its event, settle or tick. It also spawns a running
  actor as new, so a message the actor sent its parent as it started applies twice. Don't downgrade a Store this version has
  written to.
- **A journal an older version wrote replays as it did.** Its actor entries carry no
  refusal, so `Recover` still fails on one whose parent Fire was refused, and an output
  recorded with a delivery's last Fire still replays into that transition. Only steps
  this version records get the fixes below.
- Requires Go 1.27.4.
- **`Start` fails on a context refusal.** If a context's ending (ctx's, or one
  middleware derived from it) refuses a parent Fire an initial actor drove, such as a
  failed spawn's onError, `Start` returns an error wrapping the context's error and no
  Handle. It used to return a Handle whose state lacked that Fire, while `Recover`
  would apply it. The baseline is recorded, so `Recover` the instance. A ctx that ends
  with no Fire refused doesn't fail `Start`. `Recover` likewise fails when a context's
  ending refuses a parent Fire its own spawns drove, rather than returning a Handle the
  journal can't reach.
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
  `Recover` dispatches it again, failing with `ErrEffectDispatch` while the handler still
  does. A handler that panics in `Start` or `Recover` returns `ErrEffectDispatch` too, and
  so does a `DispatchStore` whose `Dispatched` or `MarkDispatched` panics or returns an
  error, in a step, `Recover` or the stateless `Runner.Fire`, with the Store's error in
  the chain. A failed `MarkDispatched` fails the Handle too, and `Recover` applies that
  effect again. The handler's error now matches through `errors.Is`.
- **A `KindedEffect` that can't marshal no longer fails a step.** A step records it by
  its id and kind with no payload, as it records an effect that isn't a `KindedEffect`,
  and its `EffectEnvelope.Meta` holds the marshal error's message under
  `crucible.marshalError`; a `MarshalJSON` that panics is recorded the same way, with the
  panic's message there. Replay re-derives the effect's value, so it dispatches as any
  effect does: once live with a handler wired, once on the `Recover` that retries a
  failed dispatch, and once on a `Recover` whose handler was wired only then. This
  relaxes `Fire`, and the stateless `Runner.Fire`, which failed with the marshal error
  before; `Tick` and `RunService` now record such an effect too.
- **A service result reaches `Assign` decoded, live as well as on replay,** so a retried
  effect can't differ from the first attempt: a struct arrives as a map, an integer as a
  `float64`, which loses precision above 2^53, and an error as an error with the same
  message, any invalid UTF-8 replaced, which `errors.Is` and `errors.As` no longer match.
  So onError's `Assign` and the `ServiceRunner`'s `LastError` see a plain error even for
  an unbound service's `*state.UnboundRefError`. An `Error` method that panics settles
  with a message saying the service error panicked, its invalid UTF-8 replaced too. The journal records this decoded value. A result whose JSON
  form won't decode, such as a number beyond `float64`, fails `RunService` with
  `ErrHandleFailed` and records nothing, as a result that won't encode does, so
  `Recover` doesn't fail on it. Either is refused only after its settle applied: the
  onDone `Assign` runs once on an internal stand-in for the result (for one with no
  JSON form, the raw result), and the state onDone enters starts its actors and
  services, before `RunService` fails; after `Recover` the service runs again, and its
  `Assign` with it. So keep a service's result JSON round-trippable, and have an onDone
  `Assign` check the payload's type rather than assume it. This may change, to refuse
  such a result before any `Assign` runs.
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

- **A timer transition starts and stops services and actors.** `Tick` handed a fired
  timer transition's effects only to the scheduler, so a timer that left a state left
  its service in flight and its actor running, and one that entered a state started no
  service and spawned no actor. A service left in flight could then settle after a
  checkpoint that no longer held it, journaling a settle replay couldn't apply, so every
  `Recover` failed with "no in-flight service to settle". Now `Tick` hands every fired
  transition's effects to the service runner, in fire order, and then to the actor
  system, as a `Fire` that produced them all does, live and on replay, so a parent Fire
  an actor spawned for one timer drives sees the services every timer of the tick
  started, and stops one it leaves rather than leave it in flight: a timer that leaves a
  state stops its service and its actor, and one that enters a state starts its service
  and spawns its actor. The tick's Record journals the parent Fires an actor it spawned
  drove, replayed after its timers, and a parent Fire a context's ending refused fails
  the Handle and records nothing, as for `Fire`. So does a `ctx` that has ended by the
  time the actor system took the spawns and messages to actors the timers handed it,
  by the rule `Fire` shares: an actor's start or delivery refused under it is lost
  without any Fire reporting it. A tick that handed the actor system neither, with no
  Fire a context refused, records even when its `ctx` ends once its timers have fired
  and their transitions applied. Replay does this for a tick of journal
  format 2 or later; an older version's tick replays as it ran, with its services left
  in flight. This is the Tick-driver fix #236 tracked. The services and actors a timer
  starts run under the tick's step context, as a `Fire`'s do, so an actor's behavior or
  a parent Fire's middleware that calls back into the same Handle gets
  `ErrReentrantStep` at once rather than deadlock on the step lock the `Tick` holds.
- **A checkpoint without `WithServiceRegistry` keeps the settled services.** A Handle
  recovered without a service registry restored them from its checkpoint but wrote its
  next checkpoint without them, so a later `Recover` with the registry found the service
  not settled, and `RunService` ran its `ServiceFn` again. A checkpoint now keeps them
  with or without a registry. `Recover` without a registry still refuses a journal tail
  that holds a service's settle.
- **`Recover` and `StateAt` fail when a Store loses `Record.Format`.** The Start baseline
  and every checkpoint record the journal format too, and a Record after one with a lower
  format fails replay, where it used to replay as an older journal's and silently skip
  the timers and services an actor-driven Fire armed.
- **A step that touches no actor doesn't recompute the actor sets.** Each append rebuilt
  the declared actors from every active state; it now reuses the last sets while the
  configuration, the running actors and the step's spawns are unchanged.
- **A running actor whose respawn fails isn't recorded as unstarted.** A failed `Recover`
  respawn with no onError to catch it made the next `Recover` spawn the actor as new, so
  its start message applied twice. It now stays out of both sets and respawns quietly.
- **A chained timer's deadline is the one the scheduler armed it at.** A `Tick` recorded
  the deadline of a timer its fired transition chained from the tick's last clock read,
  which is later than the read the scheduler armed it at. With a real clock, a checkpoint
  persisted the later deadline, so after `Recover` the timer fired late, and the drift
  grew along an `after` chain. `Tick` and its replay now take each chained timer's
  deadline from the scheduler, with `state`'s new `Scheduler.Due`, so a fired
  transition that reads the clock itself doesn't move it.
- **A failed checkpoint no longer makes the next step conflict.** `Tick`, `RunService`
  and `DeliverToActor` now advance the next step as soon as the append lands.
- **A deferred checkpoint survives an unrelated caller's cancel.** A checkpoint that
  waited for an earlier step's effects to dispatch, and was canceled by the context of
  the caller whose step finished last, isn't reported to that caller, since its own
  step succeeded, and the next step writes it; a failure of that retry is returned to
  the step that tried it. A retry the Store refuses as not advancing means the first
  write landed, and counts as written. Any other checkpoint failure is returned, and
  after that only the next step the policy makes due tries again.
- **Effects reach the handler from `Start`, `Tick` and `RunService`.** They were only
  dispatched from `Fire`. `Start` applies the initial entry's effects after encoding the
  baseline and before writing it, and only if the entry succeeded. Those effects are
  at-least-once: a handler failure, a failed baseline write or overlapping `Start` calls
  re-apply them, so key any dedupe on `EffectInstance` plus the effect id. Effects from a
  parent transition an actor drives, including during `Fire`, are still not dispatched.
  `Recover` re-dispatches what replaying a step produces, as it did for a `Fire`: for a
  `Tick` or service step, only in a Record of format 2 or later, so it doesn't send
  effects from an older version's `Tick` and service steps, which never dispatched
  them. It goes by the Record's format, not its `Effects` envelopes, so a Store that
  doesn't keep `Record.Effects` loses no effect a crash left unsent. As for a `Fire`'s,
  it loads the Store's dispatched set once for each Record with effects to send.
- **A Tick, RunService or DeliverToActor whose context ended partway isn't journaled.** A canceled `RunService`, `Tick`
  or `DeliverToActor` used to record a result the live instance never applied, which
  recovery then committed, or for an actor, failed on. Each now returns `ErrHandleFailed`
  wrapping the error and records nothing. One rule covers them: a step records nothing
  once its context ended by the end of the step, or a Fire it drove was refused with a
  context's error, which catches a context middleware derived from the caller's. A
  guard binding, action or middleware of that Fire that returns its own context error, such as
  an HTTP client's timeout, counts as a context ending too. A service function's error
  doesn't: a service that returns one settles through its onError, and the step
  journals.
  A Fire refused for another reason, such as a failing guard, is still recorded, and
  replay now meets the same refusal instead of failing Recover on it. After `Recover`
  the service stays in flight and must tolerate running again, the timer is re-armed, an
  `InvokeActor` actor restarts from its initial state, and a `Spawn` actor doesn't
  return. `Recover` and `StateAt` now fail on a context that ends before the journal is
  replayed, rather than returning state short of the journal. A context that ends once
  `Recover` has spawned its actors, with no parent Fire refused, doesn't fail it, as for
  `Start`, since their behaviors already ran. A `DeliverToActor` whose context ended by
  the end of the delivery records nothing, even when it drove no parent Fire: the
  ending may have made the kernel refuse the actor's own Fire, losing the event.
- **`Fire` follows the same rule for the actors it spawns.** When an actor a Fire's
  state spawned failed to start and a context's ending refused its onError, the Fire was
  recorded, and `Recover` then applied the onError live never applied. Now `Fire` returns
  `ErrHandleFailed` and records nothing, though its event applied: fire it again on the
  recovered Handle. So does a Fire whose context has ended by the time the actor system
  took the work the Fire handed it, a spawn or a message to an actor (`SendTo`,
  `ForwardEvent`), as for `Tick`: an actor's start or delivery refused under it is lost
  without any Fire reporting it. A Fire that handed the actor system neither and drove
  no Fire a context refused still records, even when its context ends once the kernel
  has applied the event: stopping an actor, or starting or stopping a timer or service,
  runs nothing the context can refuse.
- **An actor's output replays with the Fire that carried it.** When one delivery drove
  several parent Fires, such as an onDone entering a state whose actor then failed to
  start, the output was recorded with the last Fire, so replay gave it to the wrong
  transition. It's recorded with the settling Fire, which `state`'s new
  `ActorSystem.WithParentFireObserver` identifies.
- **`Recover` doesn't spawn an actor the live run had ended.** It spawned every actor
  the recovered state declared, so an actor that had settled or been stopped while its
  parent stayed in that state ran again: a settle the parent refused, one that applied
  without leaving the state, or an explicit stop. Each step now records the declared
  actors that ran and ended, in the new `Record.EndedActors`, and a checkpoint keeps the
  set, so `Recover` spawns the declared actors that aren't in it. An actor whose spawn
  failed with no onError declared never ran, so `Recover` tries it again, as before,
  and so does one whose onError a context's ending refused, since the step that drove
  it records nothing; one whose onError applied counts as ended, since the journal
  holds that onError, and, from the refused-onError fix below, so does one whose
  onError was refused for another reason replay meets again. A journal from before this
  records no set, and `Recover` spawns every declared actor, as before.
- **A failed spawn's onError, and a settle an absorb drove, are journaled.** When an
  actor a `Fire`, `RunService`, `Start` or `Recover` spawned couldn't start and its
  onError applied, nothing recorded the onError, so `Recover` either applied it again
  by retrying the spawn, or, once the spawn worked, never applied it. Likewise a settle
  a Fire's effects drove, such as one from a `SendTo` to a running actor, wasn't
  recorded, so `Recover` lost it. These are now recorded with their step, or, for
  `Start`'s and `Recover`'s spawns, with the step after, and replay re-fires each where
  it ran. An actor output that has no JSON form now fails the Handle here too, as it
  does for a delivery.
- **A spawn whose onError was refused deterministically isn't retried.** A failed
  spawn counted as handled only when its onError Fire applied, so one whose onError
  was refused for a reason other than a context ending — a guard returning false —
  landed in `UnstartedActors`, and every `Recover` spawned it again: the behavior,
  and any side effect it ran, re-executed each time, and each recovery journaled
  another refused onError. Such a refusal now counts as handled — the journal
  already records it, and replay meets it again — so the actor counts as ended and
  `Recover` leaves it alone. A spawn whose onError a context's ending refused still
  retries, since the step that drove it records nothing, as does one with no onError
  declared, as before. An invoked actor with no onError fires the zero event, which the
  parent refuses as having no transition, and `durable` reads that refusal
  (`state.ErrNoTransition`) as "none declared". So a declared onError the parent has no
  transition for retries too, and a wildcard or a transition that takes the zero event
  counts as caught; #250 tracks a better way to tell them apart. A host refusal worded
  like the kernel's counts as handled.
- **A message an actor sends its parent as it spawns applies once.** A `Fire`,
  `RunService`, `Start` or `Recover` didn't journal it, and `Recover` spawned the actor
  again to send it again: it landed after the steps that followed it live, or never, if
  it had ended the actor. A delivery did journal it, for an actor its onDone or onError
  spawned, so `Recover` applied it twice. Every step now journals every parent Fire it
  drove, and `Start`'s and `Recover`'s go with the step after, so replay re-fires each
  where it ran. `Recover` spawns an actor that was running with `state`'s new
  `ActorSystem.AbsorbRespawn`, which keeps such messages from the parent, and the onDone
  or onError of a nested actor its start spawns, which the journal holds too. An actor whose
  spawn failed with no onError never ran, so its messages are new: they reach the parent
  and are journaled with the next step. A journal from before this records no unstarted
  actors, and `Recover` spawns every actor as new, as before.
- **A state an actor's onDone, onError or message enters runs its timers and services.**
  The actor system armed only that state's actors, so its service wasn't in flight, and
  `RunService` reported none, and its timer never fired; a checkpoint then recorded the
  service as settled, so `Recover` didn't start it either. Such a Fire now arms them,
  live, at `Start` and `Recover`, and on replay, as a host `Fire` does. Replay arms them
  only for a Record this version wrote, which records its `Format`: an older version
  took no clock read for them, and its Records replay as they did. A service's
  settle re-arms its state's timers on replay too, and `DeliverToActor` and `RunService`
  now journal the clock reads their step took, so a checkpoint on that step can't leave
  a read for the next to replay. `Recover` re-arms a checkpoint's timers before it
  spawns the running actors, so a Fire one of theirs drives can cancel one. A `Tick`
  drops from the timers a checkpoint records exactly the ones it fired, live and on
  replay: one that came due between its clock reads, and a chained one it armed and
  fired, used to stay, so a `Recover` from a later checkpoint fired it again.
- **A delivery journals each parent Fire under the actor that drove it,** where it used to
  record every one under the actor delivered to. It no longer copies the instance's
  history twice per delivery to do it.
- **`Recover` doesn't start a service that settled while its state stayed active.** A
  checkpoint taken after such a settle, on a state that stayed put, left the service in flight again on recovery, so
  `RunService` ran it a second time. A checkpoint now records the declared services that
  settled while their state stayed active, and `Recover` doesn't start them. One that
  never settled, such as one whose state a `Tick` entered or re-entered (a `Tick`
  starts no service), isn't recorded, so `Recover` starts it, as before. A checkpoint
  from before this records none, and `Recover` starts every declared service.
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
