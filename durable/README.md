# crucible/durable

The host-side **durable-execution runtime** for the
[Crucible](../README.md) [`state`](../state) kernel.

> **Status:** experimental, pre-1.0. The runtime is feature-complete and
> extensively tested; the API may still change before v1.

Import path: `github.com/stablekernel/crucible/durable`

## What it is

`state` is a pure statechart engine: firing an event is a deterministic function
of the instance's recorded inputs. `durable` makes a running instance survive a
process crash by recording every nondeterministic value the instance consumes —
clock readings, invoked-service results, actor outcomes — and persisting each
step before it is acknowledged. Recovery **replays** those recorded values back
through the same pure transition function, so a recovered instance reaches
exactly the configuration, context, and history of a run that never crashed,
without re-invoking any external source.

The runtime is **additive** over the kernel: it consumes seams the kernel
already reserves — `Snapshot.Journal`, the `EffectEnvelope.EffectID` correlation
slot, and the injectable `Clock` / `ServiceRunner` / `ActorSystem` drivers — and
requires no change to the kernel, which stays pure and stdlib-only. Heavy
dependencies (database drivers, cloud SDKs) never enter this module: persistent
backends implement the `Store` interface out of tree.

## Guarantees

- **Deterministic replay** — a recovered instance is byte-identical to one that
  never crashed, because recovery replays the recorded driving events and
  nondeterministic results rather than re-executing their sources.
- **Exactly-once effects** — a domain effect is applied exactly once over the
  instance's lifetime (the live run plus any number of recoveries), even though
  the replay loop is at-least-once. Each effect carries a deterministic
  `EffectID` and is deduplicated through the `Store`'s dispatch set.
- **Durability across restart** — every `Fire` step is write-ahead appended to
  the `Store` before it is acknowledged, so a crash after a successful `Fire`
  never loses the step. Periodic checkpoints bound the tail recovery replays.

## Quick start

Wire a `Runner` around a machine and a `Store`. `Start` creates a fresh durable
instance; `Fire` drives it; `Recover` rebuilds it from the `Store` after a
crash.

```go
runner := durable.NewRunner(machine, durable.NewMemStore())

// Start a fresh instance — persists a baseline checkpoint.
h, err := runner.Start(ctx, "order-42", OrderInput{ /* ... */ })
if err != nil {
	return err
}

// Drive it. Each Fire write-ahead appends a Record before acknowledging.
if _, err := h.Fire(ctx, "submit"); err != nil {
	return err
}

// ...process crashes, comes back up...

// Recover purely from the Store: load the latest checkpoint, replay the tail.
h, err = durable.Recover(ctx, machine, store, "order-42")
if err != nil {
	return err
}
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
| **Clock** (timers) | `WithRunnerClock` | `JournalClockRead` — replayed so timers fire at the same recorded instants, wall-clock-independent; armed deadlines survive checkpoint compaction |
| **Invoked services** | `WithServiceRegistry` + `Handle.RunService` | `JournalServiceResult` — the service runs once; recovery replays its result through the kernel's settle seam |
| **Child-machine actors** | `WithActorPalette` + `Handle.DeliverToActor` | `JournalActorMessage` — the behavior runs once; recovery re-fires the recorded parent transition |
| **Domain effects** | `WithEffectHandler` | dispatch set — applied exactly once via deterministic `EffectID` dedup |

Use `WithCheckpointEvery(n)` to tune how often a full snapshot is written (a
shorter interval bounds recovery replay; a longer one cuts checkpoint cost).

## Stores

`Store` is the persistence seam: a durable instance is an ordered log of
`Record`s (one per `Fire` step) layered over periodic full-snapshot checkpoints.
Two stdlib-only reference implementations ship in-tree:

- **`MemStore`** (`NewMemStore`) — in-memory, thread-safe, not durable across
  restarts. For tests, examples, and single-process development. `WithHistory`
  retains the full record history (enabling time-travel below).
- **`FileStore`** (`NewFileStore`) — on-disk: a directory of per-instance
  subdirectories, each an append-only journal, an atomic checkpoint, an
  idempotency ledger, and a dispatched-effect log. Each append flushes to stable
  storage; each checkpoint uses write-temp-plus-rename for crash-safe atomicity.
  Use it for durability across restarts without a database.

Persistent database backends (PostgreSQL, DynamoDB, …) implement `Store` out of
tree, so their drivers never burden this module's dependency or vulnerability
surface.

## Time-travel reader

`StateAt` reconstructs an instance's state as of any recorded step, read-only —
restoring the start baseline and replaying recorded events forward to the target
step, running no service, re-instantiating no actor, reading no wall clock, and
dispatching no effect:

```go
view, err := durable.StateAt(ctx, machine, store, "order-42", 3)
// view.Snapshot(), view.Instance(), view.Step() — detached and safe to read
```

Time-travel needs the full record history through the target step. A `Store`
opts in by implementing `HistoryStore` (the in-tree `MemStore` does so under
`WithHistory`); `StateAt` falls back to `Load` (latest checkpoint plus tail)
otherwise.

## Caveat: serialized payloads

Events, service done-data, actor done-data, and actor messages are recorded as
their JSON form. A parent reducer that type-asserts a non-JSON Go type from
`AssignCtx.Event` observes the JSON-decoded shape on the replayed `onDone`. A
typed-codec option to carry the concrete Go value across the journal boundary is
reserved for a later, additive change.

## Performance

Indicative hot-path numbers (Apple Silicon dev machine, `go test -bench`):

| Benchmark | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| bare `Instance.Fire` | ~1070 | 1241 | 37 |
| durable `Handle.Fire` (MemStore) | ~2780 | 3719 | 74 |
| `MemStore.Append` | ~336 | 722 | 3 |

The durable wrapper adds roughly **2.6×** over a bare `Fire` for the minimal
two-state step — the cost of serializing and appending one `Record`. Reproduce
with `go test -bench=. -benchmem -run=^$ ./durable/`.

## Stability

Stability label: **experimental** (pre-1.0; the API may change). Each module is
independently versioned per-module SemVer.

## Design & discussions

Design rationale lives on the GitHub
[Discussions board](https://github.com/stablekernel/crucible/discussions) under
the **State Machine** category.

## License

Apache-2.0. See the repository [LICENSE](../LICENSE) and [NOTICE](../NOTICE).
