// Package state is the pure, abstract state machine kernel of the Crucible
// suite — a portable, domain-agnostic engine for forging event-driven services
// in Go.
//
// Import path: github.com/stablekernel/crucible/state
//
// # What this kernel is
//
// state is an abstract, domain-agnostic state machine kernel built once and
// usable everywhere. It is generic over state, event, and context types
// (conceptually Machine[S, E, C]) and knows nothing about any particular
// application domain. The same machine definition runs unchanged from a unit
// test, a synchronous request handler, and an asynchronous event consumer.
//
// The kernel is stdlib-only. It imports only the Go standard library and
// performs no injected IO. This is the extreme end of the suite's "thin seams,
// no-op defaults, no forced dependencies" philosophy: a tiny dependency graph
// is a tiny attack surface, and the kernel stays a clean, extractable unit
// forever. The stdlib-only boundary is enforced mechanically by an import-graph
// test.
//
// # Pure-function step semantics
//
// Firing an event returns (newState, effects, trace) without performing any IO.
// The caller dispatches the effects however it likes — publish to a broker,
// write to a store, call an RPC. Effects are abstract at the kernel (the kernel
// never inspects the payload) and concrete at your domain layer. This is what
// makes one machine usable across tests, handlers, and consumers without
// change.
//
// An effect is discriminated data: every kernel-emitted effect reports a stable,
// serializable Kind (the KindedEffect interface) and serializes to an
// EffectEnvelope (kind + payload + meta), so effects can be journaled, deduped,
// rendered, and routed across a serialization boundary by kind rather than by Go
// type. An EffectRegistry decodes an envelope back to a concrete effect;
// built-in kinds are pre-registered and a host registers its own through
// RegisterEffect. An unknown effect kind is preserved on load and rejected only
// at dispatch, never silently dropped. Effects stay data the host applies — the
// kernel never executes them.
//
// # The definition IR is the spec
//
// The canonical machine is a serializable definition IR: pure data, lossless to
// and from JSON. Behavior is not embedded as closures in the IR; every guard,
// action, and effect is a named reference with serializable params, bound to
// host-provided implementations through a registry at freeze time. Binding
// fails loudly if any reference does not resolve.
//
// This is the config/implementation split: structure is
// dual-authored (code or, eventually, a visual UI) while behavior is always
// code, surfaced to authors as a named palette. The Go DSL and a future UI are
// two front-ends that emit the same IR; a machine authored in Go and a machine
// loaded from JSON are the same machine.
//
// # Foundry vocabulary
//
// The lifecycle API uses a small "foundry" verb vocabulary. The noun stays
// plain — the type is a Machine — only the verbs are themed:
//
//   - Forge   — open the builder DSL.
//   - Temper  — optional, non-failing dev-time diagnostics pass (lint / static
//     analysis), chainable before Quench.
//   - Quench  — freeze the definition into an immutable Machine; the always-call
//     finalizer that binds refs and panics on misconfiguration.
//   - Cast    — pour a running instance from the machine.
//   - Fire    — send an event to an instance and advance it.
//   - Assay   — check that an externally-constructed entity is legally in a
//     given state.
//
// Operations that favor discoverability over metaphor stay plain: PlanPath,
// Requirements, Trace, and the To*/LoadFromJSON serializers.
//
// # Design
//
// The public API follows the suite's functional-options convention: every
// public constructor and operation takes a variadic option tail. Required
// inputs stay positional; everything optional or extensible is an option; a
// zero-option call reads clean. New capability arrives as a new option —
// additive-only, never a signature or breaking change. The kernel idiom is
// fail-fast by default, with resilience and aggregation available opt-in via
// options.
//
// Observability is Trace-first: the structured Trace is the canonical surface,
// recording matched transitions, guard and policy evaluations, emitted effects,
// and the outcome as pure data. An optional WithLogger(*slog.Logger) (no-op by
// default) is the only logging seam; the kernel never logs unless asked and
// never imports a third-party logger. Determinism is preserved by injecting
// time and identifier seams rather than calling time.Now or rand directly.
//
// As a library, the kernel never exits the process — it never calls os.Exit or
// log.Fatal on an operational error. Panics are reserved strictly for
// programmer error at construction time (Quench).
//
// # Status
//
// The kernel implements the Forge/Temper/Quench build path, Cast/Fire pure step
// semantics with guards, actions, typed errors and an always-recorded Trace,
// Assay/Requirements, PlanPath (BFS), FireSeq/FireEach batch helpers, and
// lossless ToJSON/LoadFromJSON/Provide round-trip.
//
// Hierarchical and orthogonal states extend the same surface: a state may
// declare nested substates with an initial child (compound states) or parallel
// regions (orthogonal states). Superstates nest to arbitrary depth — a
// SuperState block may contain another SuperState block — and parallel regions
// may contain nested compounds. Events resolve child-first and bubble to
// ancestors; orthogonal regions each receive the event and resolve
// independently; transitions run the standard exit/entry cascade across the
// hierarchy; and final states drive done-event completion, including the
// all-regions-final join for parallel states. The hierarchy serializes, so a
// nested machine round-trips through JSON losslessly.
//
// History pseudo-states (shallow and deep) let a transition re-enter a compound
// state's last active configuration rather than its initial child; the
// pseudo-states serialize while the recorded per-instance configuration is
// runtime state threaded through the pure Fire step.
//
// Delayed (`after`) transitions are drivable: entering a state with an `after`
// transition emits a ScheduleAfter effect and exiting it a CancelScheduled effect
// (auto-cancel-on-exit), while Fire stays pure — a host Scheduler driver
// owns the real timer and re-fires the delayed event, with a deterministic
// FakeClock for testing.
//
// Invoked services (`invoke`) are drivable: entering a state that declares an
// invoke emits a StartService effect and exiting it before the service completes
// emits a StopService effect (auto-stop-on-exit), while Fire stays pure
// — a host ServiceRunner runs the bound service and re-fires the invocation's
// onDone (with the result) or onError (with the error) back through Fire, with a
// deterministic settle-by-id harness for testing. Child-machine actors (invoking
// another Machine as a sub-actor) and the actor model remain reserved-but-inert;
// they arrive with the instance mailbox.
package state
