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
// Exactly-once effects: a domain effect emitted by a step (Runner.Start's initial entry
// is at-least-once, and a parent transition an actor drives isn't dispatched) is applied
// exactly once over the instance's lifetime — the live run plus any number of
// recoveries — even though the runtime's replay loop is at-least-once. The exception is
// a crash or failed mark after the handler returned, or a handoff between owners, where
// an effect can apply twice, so keep handlers idempotent on EffectInstance plus the EffectID. Each
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
// Payloads are serialized at the journal boundary. Event, service done-data, actor
// done-data, and actor messages are recorded as their JSON form. A WithEventData
// payload is decoded from its JSON form live as well as on replay, so a reducer gets
// the same value both ways; WithEventDataDecoder decodes it as the type the caller
// passed.
//
// A service result is decoded the same way, live and on replay. When
// WithEventDataDecoder covers the invocation's onDone event, the result arrives as the
// decoder's type; otherwise it arrives in JSON's generic form, a struct as a
// map[string]any and a number as a float64. A decoder that fails, or returns a type
// other than the service's result, parks the result: RunService returns
// ErrEventDataEncoding and no onDone or onError reducer runs, but the result is
// recorded and the Handle stays healthy, so the service never runs again. The decoder
// is part of the Runner's options, so the Handle that parked the result never sees a
// fix: Recover with the fixed decoder, then call RunService, and it settles the parked
// result. The decoder must return the type the service returned, by import path and
// name, or JSON's generic form, so settle parked results before moving or renaming
// that type, or tell the Runner what the recorded name now means with
// WithResultTypeAlias; after a move without an alias, only a decoder that returns the
// generic form settles them.
// A Fire or timer that leaves the state that declares the service stops it and
// discards the parked result. A service error stays message-only. Register the decoder before recording results: replay decodes the
// journal with the recovering Runner's decoder, held to the Go type the service
// returned, which every settle records, so a decoder changed after results were
// recorded to return another type fails Recover, unless it returns JSON's generic form
// or the recorded type's new name is aliased (WithResultTypeAlias), and one added later
// for the service's type changes what replay produces.
//
// An actor's done-data gets the same decode, live as on replay: when the decoder covers
// the parent's onDone event it arrives as the decoder's type, and without one in JSON's
// generic form, held to the recorded Go type, which an alias from WithResultTypeAlias
// can rename as a service result's. An actor's error is message-only too, and one whose
// Error method panics settles with a message saying so. An actor outcome is never
// parked: done-data that won't decode, or decodes as a type other than the actor
// produced, fails the Handle — the delivery, or the Fire, Tick or RunService whose
// absorb drove the settle — and records nothing, so the actor runs again on Recover.
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
// idempotency ledger, a dispatched-effect log and, once fenced, a fence file. Each
// Append flushes to stable
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
// the real clock in a recording clock on the live path so the Now() readings of a step
// are journaled as JournalClockRead entries in its Record. An idle Tick records nothing.
// On recovery a replay clock returns those readings in order, making timer-driven
// transitions wall-clock-independent: the same timers fire at the same recorded
// instants regardless of the wall clock at recovery time. A timer also survives
// checkpoint compaction: the baseline and each checkpoint persist the absolute
// deadlines of the timers armed at that instant, and recovery re-arms them at those
// deadlines before the tail replays, so a tail step can fire, cancel or replace them.
//
// Invoked services (WithServiceRegistry): a service (`invoke`) runs on the live path;
// its result is journaled as a JournalServiceResult. On recovery the recorded result is
// replayed back through the same kernel settle seam — that service is never re-invoked
// and the same onDone or onError event re-fires with the same data. A result that
// wasn't recorded has nothing to replay, so the service runs again when the host calls
// RunService.
//
// Child-machine actors (WithActorPalette): an actor's behavior runs on the live path;
// each parent transition the delivery drives is journaled as a JournalActorMessage. On
// recovery the recorded transition is re-fired directly through the parent with the
// recorded done-data, decoded for its onDone event as the live settle decoded it, so a
// settled actor never re-runs and its Assign sees the same value. Each step records the
// declared actors that ran and ended, settled or stopped, and Recover doesn't spawn
// those. An InvokeActor actor still running at recovery restarts from its initial
// state, because its own state isn't journaled, and a Spawn actor doesn't return.
//
// # Idempotent effect dispatch
//
// A transition may emit a domain effect — sending an email, charging a card,
// publishing a message — as an Effect value the kernel routes out through
// FireResult.Effects without applying. The Runner applies it through the caller-
// supplied EffectHandler (WithEffectHandler), exactly once over the instance's
// lifetime, with the exceptions under Exactly-once effects above.
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
// The dispatched set lives in a Store that implements DispatchStore, as MemStore and
// FileStore do. A Runner with an effect handler requires one: Start, Recover and the
// stateless Runner.Fire refuse any other Store with ErrNoDispatchStore before writing
// anything or calling the handler. Without a handler, any Store works and no effect is
// applied.
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
