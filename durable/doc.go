// Package durable is the host-side durable-execution runtime for the Crucible
// state kernel. It records the nondeterministic results a running instance
// consumes and persists them, so an instance can be checkpointed, crash, and
// resume by replaying recorded values back through the kernel's pure transition
// function rather than re-invoking their external sources.
//
// The package is additive over the state kernel: it consumes the kernel's
// already-reserved persistence seams — Snapshot.Journal ([]state.JournalEntry),
// the EffectEnvelope.EffectID correlation slot, and the injectable Clock,
// ServiceRunner, and ActorSystem drivers — without requiring any change to the
// kernel, which stays pure and stdlib-only.
//
// # Guarantees
//
// Deterministic replay: an instance recovered from its Store reaches exactly the
// same configuration, context, and history as a run that never crashed, because
// recovery replays the exact recorded driving events and nondeterministic results
// through the same pure transition function.
//
// Exactly-once effects: a domain effect emitted by a transition is applied
// exactly once over the instance's lifetime — the live run plus any number of
// recoveries — even though the runtime's replay loop is at-least-once. Each
// effect is stamped with a deterministic EffectID (step, ordinal, kind) and
// deduplicated through the Store's dispatch set.
//
// Durability across restart: every Fire step is written to the Store before it
// is acknowledged (write-ahead append), so a crash after a successful Fire never
// loses the step. A periodic Snapshot checkpoint bounds the tail that recovery
// must replay, so recovery cost is proportional to the tail length, not the
// whole run.
//
// # Caveats
//
// Payloads are serialized at the journal boundary. Event, service done-data,
// actor done-data, and actor messages are recorded as their JSON form; a parent
// reducer that type-asserts a non-JSON Go type from AssignCtx.Event observes the
// JSON-decoded shape on the replayed onDone. A typed-codec option to carry the
// concrete Go value across the boundary is reserved for a later, additive change.
//
// The kernel stays pure and dependency-free. The durable module is host-side:
// database drivers, cloud SDKs, and I/O libraries live in the caller or in out-
// of-tree Store backends, never in this module. The in-tree MemStore and
// FileStore are stdlib-only reference implementations.
//
// # Store
//
// Store is the persistence seam. A durable instance is an ordered log of Records
// (one per Fire step) layered over periodic full-Snapshot checkpoints. Load
// reconstructs an instance from its latest checkpoint plus the journal and effect
// tail recorded after it.
//
// MemStore is the in-memory reference implementation: thread-safe, stdlib-only,
// not durable across process restarts. Use it for tests, examples, and single-
// process development.
//
// FileStore is the on-disk reference implementation: a directory of per-instance
// subdirectories, each holding an append-only journal, an atomic checkpoint, an
// idempotency ledger, and a dispatched-effect log. Each Append flushes to stable
// storage; each Checkpoint uses write-temp+rename for crash-safe atomicity. Use
// FileStore when you need durability across restarts without a database.
//
// Persistent database backends (PostgreSQL, DynamoDB, and the like) implement
// the Store interface out of tree, so heavy drivers never burden this module's
// dependency or vulnerability surface.
//
// # Runner / Handle / Recover
//
// Runner is the durable wrapper around a state.Machine. Wire one with a Store,
// then call Start to create a fresh instance (persisting a baseline checkpoint)
// or Fire to drive events statelessly (loading, replaying, and re-recording for
// each call). For a hot path that fires many events in sequence, obtain a Handle
// from Start or Recover and call Handle.Fire directly, avoiding a Store round-
// trip between steps.
//
// Recover reconstructs a durable instance purely from the Store: it loads the
// latest checkpoint Snapshot and the tail of Records after it, restores the
// snapshot under a replay clock (firing nothing), and replays the tail's recorded
// driving events through the kernel to reach the live tip. The returned Handle
// continues recording subsequent fires.
//
// # The three nondeterministic seams
//
// A state machine is a pure function of its inputs. The durable runtime isolates
// each source of nondeterminism behind a seam, records the result the first time,
// and replays it verbatim on every subsequent recovery — so the kernel's
// transition function is never re-invoked for a value it has already consumed.
//
// Clock (WithRunnerClock): a running instance reads time only through its host
// scheduler, which arms and ticks delayed `after` transitions. The Runner wraps
// the real clock in a recording clock on the live path so every Now() reading the
// scheduler consumes is journaled as a JournalClockRead in the step's Record.
// On recovery a replay clock returns those readings in order, making timer-driven
// transitions wall-clock-independent: the same timers fire at the same recorded
// instants regardless of the wall clock at recovery time. A timer also survives
// checkpoint compaction: each checkpoint persists the absolute deadlines of the
// timers armed at that instant; on recovery a timer whose arming ScheduleAfter
// was compacted out of the tail is re-armed from its persisted deadline rather
// than lost.
//
// Invoked services (WithServiceRegistry): a service (`invoke`) runs exactly once
// on the live path; its result is journaled as a JournalServiceResult. On
// recovery the recorded result is replayed back through the same kernel settle
// seam — the service is never re-invoked and the same onDone or onError event
// re-fires with the same data.
//
// Child-machine actors (WithActorPalette): an actor's behavior runs exactly once
// on the live path; each parent transition the delivery drives is journaled as a
// JournalActorMessage. On recovery the recorded transition is re-fired directly
// through the parent with the recorded done-data — the actor behavior is never
// re-instantiated.
//
// # Idempotent effect dispatch
//
// A transition may emit a domain effect — sending an email, charging a card,
// publishing a message — as an Effect value the kernel routes out through
// FireResult.Effects without applying. The Runner applies it through the caller-
// supplied EffectHandler (WithEffectHandler), exactly once over the instance's
// lifetime.
//
// Each emitted effect is stamped with a deterministic EffectID derived from its
// step, its emission ordinal within that step, and its kind — every component a
// pure function of the recorded run, so the same effect carries the same id on
// the live path and on every recovery. The Runner write-ahead appends the step
// Record (carrying those ids) before dispatching, then applies each effect whose
// id is not already in the Store's dispatched set and marks it as it succeeds. A
// crash between append and dispatch leaves the effect recorded but un-marked, so
// recovery redispatches it; a crash after dispatch finds the id marked and skips
// it.
//
// Kernel driver effects (services, timers, actors) are absorbed by their host
// drivers and never reach the handler.
//
// # Time-travel reader
//
// StateAt reconstructs a durable instance's state as of any recorded step, read-
// only: it restores the start baseline and replays recorded events forward up to
// the requested step, running no service, re-instantiating no actor, reading no
// wall clock, and dispatching no domain effect. The Store is consulted only
// through reads; neither the recorded log nor the dispatched set is touched.
//
// Time-travel requires the full Record history through the target step. A Store
// that compacts its journal at each checkpoint can no longer reach a compacted
// step. A Store opts into full history by implementing HistoryStore; the in-tree
// MemStore does so under WithHistory. StateAt falls back to Load (latest
// checkpoint plus tail) when the Store does not implement HistoryStore.
package durable
