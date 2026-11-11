---
title: What is crucible/durable
description: A host-side durable-execution runtime that records every nondeterministic value a state instance consumes and replays them to rebuild it byte-identically after a crash.
sidebar:
  order: 1
---

<!-- IMAGE-SLOT: durable-overview-replay (a sky-squid smith re-pouring a recorded molten stream back into the same mold so the casting comes out identical after the forge was relit; ember/copper on steel) 16:9 -->

`crucible/durable` is the host-side **durable-execution runtime** for the
[`state`](/crucible/start/introduction/) kernel. `state` is a pure statechart
engine: firing an event is a deterministic function of the instance's recorded
inputs. durable makes a running instance survive a process crash by recording
every nondeterministic value the instance consumes (clock readings,
invoked-service results, actor outcomes) and persisting each step before it is
acknowledged. Recovery **replays** those recorded values back through the same
pure transition function, so a recovered instance reaches exactly the
configuration, context, and history of a run that never crashed, without
re-invoking any external source.

The runtime is **additive** over the kernel. It consumes seams the kernel already
reserves (`Snapshot.Journal`, the `EffectEnvelope.EffectID` correlation slot, and
the injectable `Clock` / `ServiceRunner` / `ActorSystem` drivers) and requires no
change to the kernel, which stays pure and stdlib-only. Heavy dependencies
(database drivers, cloud SDKs) never enter this module: persistent backends
implement the `Store` interface out of tree.

## Guarantees

- **Deterministic replay.** A recovered instance is byte-identical to one that
  never crashed, because recovery replays the recorded driving events and
  nondeterministic results rather than re-executing their sources.
- **Exactly-once effects.** A domain effect is applied exactly once over the
  instance's lifetime (the live run plus any number of recoveries), even though
  the replay loop is at-least-once. Each effect carries a deterministic
  `EffectID` and is deduplicated through the `Store`'s dispatch set.
- **Durability across restart.** Every `Fire` step is write-ahead appended to the
  `Store` before it is acknowledged, so a crash after a successful `Fire` never
  loses the step. Periodic checkpoints bound the tail that recovery replays.

## The shape of it

Wire a `Runner` around a machine and a `Store`. `Start` creates a fresh durable
instance; `Fire` drives it; `Recover` rebuilds it from the `Store` after a crash.

```go
runner := durable.NewRunner(machine, durable.NewMemStore())

// Start a fresh instance: persists a baseline checkpoint.
h, err := runner.Start(ctx, "order-42", OrderInput{ /* ... */ })

// Drive it. Each Fire write-ahead appends a Record before acknowledging.
_, err = h.Fire(ctx, "submit")

// ...process crashes, comes back up...

// Recover purely from the Store: load the latest checkpoint, replay the tail.
h, err = durable.Recover(ctx, machine, store, "order-42")
_, err = h.Fire(ctx, "confirm") // continues recording from the live tip
```

For a hot path firing many events in sequence, keep the `Handle` from `Start` or
`Recover` and call `Handle.Fire` directly to avoid a `Store` round-trip per step.
For a stateless handler that fires a single event per request, `Runner.Fire`
loads, replays, fires, and re-records in one call.

## The seams

Each source of nondeterminism is isolated behind an injectable driver, recorded
the first time, and replayed verbatim on recovery:

| Seam | Wire with | Recorded as |
| --- | --- | --- |
| **Clock** (timers) | `WithRunnerClock` | `JournalClockRead`, replayed so timers fire at the same recorded instants, wall-clock-independent; armed deadlines survive checkpoint compaction |
| **Invoked services** | `WithServiceRegistry` + `Handle.RunService` | `JournalServiceResult`; a recorded result replays through the kernel's settle seam without re-running the service, and an unrecorded one (a context canceled during the service, a failed `Append`, a crash) runs again when the host calls `RunService` |
| **Child-machine actors** | `WithActorPalette` + `Handle.DeliverToActor` | `JournalActorMessage`; recovery re-fires a recorded parent transition; a settled actor doesn't re-run, and a running one restarts from its initial state on any `Recover` |
| **Domain effects** | `WithEffectHandler` | dispatch set, applied exactly once via deterministic `EffectID` dedup |

Use `WithCheckpointEvery(n)` to tune how often a full snapshot is written: a
shorter interval bounds recovery replay, a longer one cuts checkpoint cost.

## Stores

`Store` is the persistence seam. A durable instance is an ordered log of
`Record`s (one per `Fire` step) layered over periodic full-snapshot checkpoints.
Two stdlib-only reference implementations ship in-tree:

- **`MemStore`** (`NewMemStore`): in-memory, thread-safe, not durable across
  restarts. For tests, examples, and single-process development. `WithHistory`
  retains the full record history, enabling time-travel below.
- **`FileStore`** (`NewFileStore`): on-disk, a directory of per-instance
  subdirectories, each an append-only journal, an atomic checkpoint, an
  idempotency ledger, a dispatched-effect log and, once fenced, a `fence` file.
  `Append` refuses a Record over its 16 MiB line limit with `ErrRecordTooLarge`.
  Each append flushes to stable storage; each checkpoint uses
  write-temp-plus-rename for crash-safe atomicity.
  Use it for durability across restarts without a database.

A write can carry a fence token (`WithFence`, `WithCheckpointFence`). A `Store`
keeps the highest token it has seen for an instance and refuses a lower one with
`ErrFenced`, writing nothing, so an owner that lost an instance is refused once the
new owner has written. `FileStore` fencing holds only within one open store.
`WithOwnerFence` sets the token a `Runner` carries on every `Append` and
`Checkpoint`. `Append` also rejects a different record at a recorded step with
`ErrStepConflict`. An out-of-tree `Store` reads the token with `AppendFence` and
`CheckpointFence`.

Persistent database backends (PostgreSQL, DynamoDB, and the like) implement
`Store` out of tree, so their drivers never burden this module's dependency or
vulnerability surface.

## Time-travel reader

`StateAt` reconstructs an instance's state as of any recorded step, read-only:
restoring the start baseline and replaying recorded events forward to the target
step, running no service, re-instantiating no actor, reading no wall clock, and
dispatching no effect:

```go
view, err := durable.StateAt(ctx, machine, store, "order-42", 3)
// view.Snapshot(), view.Instance(), view.Step(): detached and safe to read
```

Time-travel needs the full record history through the target step. A `Store` opts
in by implementing `HistoryStore` (the in-tree `MemStore` does so under
`WithHistory`); `StateAt` otherwise falls back to the latest checkpoint plus tail.

## Handles across goroutines

`Fire`, `Tick`, `RunService` and `DeliverToActor` are safe to call from different
goroutines: each holds the Handle's lock for the whole step. A host callback must not
call back into its Handle. `Instance()` is outside the lock.

A step that may not have been recorded, or a `Tick`, `RunService` or `DeliverToActor` whose
context ended partway, fails the Handle with `ErrHandleFailed`. `Recover`
a new Handle with the same options to continue, unless the cause is `ErrFenced`: then a
newer owner holds the instance, so stop.

## A note on serialized payloads

Events, service done-data, actor done-data, and actor messages are recorded as their
JSON form. A parent reducer that type-asserts a non-JSON Go type from `AssignCtx.Event`
observes the JSON-decoded shape on a replayed `onDone`. A `WithEventData` payload is
JSON-decoded live as well as on replay, so a reducer gets the same value both ways: a
struct is a map and an integer a `float64`. An error stays an error with the same
message, with any invalid UTF-8 replaced. `Handle.Fire` refuses a payload it can't
record, or one over `WithMaxEventDataBytes` (8 MiB by default), with
`ErrEventDataEncoding`, and nothing applies. A
typed-codec option to carry the concrete Go value across the journal boundary is
reserved for a later, additive change.
