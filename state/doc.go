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
// A Fire is transactional with respect to the step's outcome: it commits to the
// instance's internal state only on a fully-successful step. A Fire that fails
// partway through its cascade — an action or assign that errors or panics —
// returns the error and is a no-op on the instance's persisted state. The active
// configuration, current leaf, context, and recorded history all roll back to their
// pre-Fire values, and no effects are emitted (returned in FireResult.Effects), so
// a host replaying a failed Fire cannot double-apply the effects that ran before the
// error, and FireResult.NewState reports the original state rather than the abandoned
// target. The configuration still advances in place during a successful macrostep
// (entry actions, the done cascade, and the run-to-completion loop observe the
// advancing configuration); a failure discards that advance. The one thing a failed
// Fire cannot undo is a real-world side effect a host already applied from an effect
// emitted by an earlier, successful step — those are outside the kernel's pure state.
//
// The rollback restores the context VALUE the kernel holds, not the pointee behind a
// reference context. Under a value C this is total. Under a pointer or reference C
// (Machine[S, E, *Order], or a struct holding maps/slices) a reducer that mutates the
// pointee IN PLACE is not unwound — rollback restores the pointer header the kernel
// captured, not the data it points at. Keeping in-place mutation out of a reducer
// (reducers should return new values rather than write through a shared pointer) is
// the host's responsibility, the same value-context discipline the rest of this
// kernel assumes.
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
//   - Verify  — plain verb (favoring discoverability over metaphor): check that
//     an externally-constructed entity is legally in a given state.
//
// Operations that favor discoverability over metaphor stay plain: Verify,
// PlanPath, Requirements, Trace, and the To*/LoadFromJSON serializers.
//
// # Context: assigns and value semantics
//
// Context (the C type) is updated only through an assign — a pure reducer,
// AssignFn[C], that takes the prior context by value, the triggering event, and
// the ref's static params and returns the next context. This is the sole
// context-mutation site (the G1 contract): guards and actions receive context
// read-only, actions emit effects-as-data and never write context, and the kernel
// folds the assigns declared on a transition's exit, transition, and entry phases
// — in that order, declaration order within each phase, each reducer seeing the
// prior result — committing the folded value to the instance at the end of the
// step. Wire an assign with the Assign transition verb or the OnEntryAssign /
// OnExitAssign state verbs; register the reducer with Builder.Reducer (or
// Registry.Reducer). A service result or actor done-data reaches its onDone
// transition's assign through the re-fired done event's payload (AssignCtx.Event),
// delivered with the WithEventData fire option — no host side channel.
//
// Use a VALUE context type (Machine[S, E, Order], not Machine[S, E, *Order]).
// Under a value C the kernel's structural guarantees hold: a guard or action that
// writes the context copy it receives mutates a throwaway, so the instance is
// untouched (read-only falls out for free), and a service or actor observes a
// point-in-time snapshot value at invocation rather than an alias that could leak
// later mutations. A pointer C stays compilable as an ergonomics/performance
// escape hatch, but it forfeits these guarantees: the copy is a copied pointer to
// the same value, so a guard/action can mutate through the alias and a service can
// observe later mutations. With a pointer C the consumer owns that discipline; the
// structural read-only, clean-replay, and deterministic-analysis contracts hold
// only for a value C.
//
// # Determinism and ordering
//
// The pure step is also a deterministic step: given the same machine, the same
// starting configuration, and the same event, a Fire produces the same effects,
// the same context, and the same Trace — byte-for-byte, every time. Purity keeps
// a Fire from reading the clock or doing IO; determinism additionally freezes the
// ORDER in which the step emits effects, folds assigns, and advances states. This
// is what makes a Trace journalable and a run replayable: a consumer that records
// the event stream can re-derive the identical effect/context sequence later.
//
// The emission order is frozen as follows, and is golden-locked by a regression
// test so a reorder is a visible failure:
//
//   - Cascade phases run exit -> transition -> entry, in that fixed order. The
//     exit cascade runs innermost-first (the source leaf, then its ancestors up
//     to but not including the least common ancestor); the entry cascade runs
//     outermost-first (the least common ancestor's child down to the target, then
//     the descent into the target's initial children). A reentering self/ancestor
//     transition exits up to and including its target, then re-enters it.
//
//   - Within a single state's phase, effects (actions) run before assigns
//     (reducers), each in declaration order. The folded context of a phase becomes
//     the input to the next phase's assigns; the value committed to the instance
//     at the end of the step is the fold of every phase's assigns in cascade order.
//     Effects read the context as it stood at phase entry (read-only).
//
//   - Parallel regions are broadcast in REGION DECLARATION ORDER. When several
//     regions handle the same event in one macrostep, the earlier-declared
//     region's effects and assigns are emitted and folded before the later one's,
//     so a cross-region assign fold is deterministic and order-stable. Likewise a
//     parallel target's entry descends its regions in declaration order, and the
//     active configuration lists region leaves in that same order.
//
//   - The run-to-completion (RTC) microstep interleave is fixed: after the
//     triggering transition settles, the macrostep drains raised internal events
//     FIRST (FIFO, in the order they were raised), then fires one enabled eventless
//     ("always") transition, and repeats until the configuration is stable. Raised
//     events always precede eventless transitions within a microstep. The internal
//     queue is macrostep-local, so the interleave is reproducible and Fire stays
//     pure. A cycle is bounded and fails fast with a typed overflow error rather
//     than spinning.
//
//   - Auto-emitted lifecycle effects keep their cascade slot: a ScheduleAfter /
//     StartService / SpawnActor for an entered state is appended after that state's
//     entry effects and assigns; a CancelScheduled / StopService / StopActor for an
//     exited state after its exit effects and assigns — all in exit/entry order.
//
// The Trace records each of these in order: EffectsEmitted and AssignsApplied list
// the per-step effects and folds in emission order, ExitedStates and EnteredStates
// the cascade in execution order, and Microsteps the RTC interleave (each raised
// event and eventless step, plus per-region markers) as it happened. FireResult's
// Effects slice carries the same effects, in the same order, as data.
//
// Fire honors context cancellation at microstep boundaries. It checks ctx before
// the triggering transition runs and again between every run-to-completion
// microstep — the same "polls between steps" granularity WaitFor uses — but never
// mid-microstep: a single in-flight microstep (its exit/transition/entry cascade
// and the actions within it) always runs to completion before the next boundary
// check. A context that is canceled or past its deadline at any boundary aborts the
// macrostep and surfaces the cancellation cause (context.Canceled or
// context.DeadlineExceeded) on FireResult.Err, where errors.Is can match it. The
// abort routes through the same transactional rollback as a failed Fire, so a
// canceled Fire is a clean no-op on persisted state: the instance is left at its
// pre-Fire configuration and context, FireResult.NewState reports the original
// state, and FireResult.Effects is nil — no partially settled microstep leaks out.
//
// The ordering is structural, not incidental. Every emission, fold, and cascade
// walk iterates declaration-ordered slices — states, transitions, regions,
// children, refs — never a Go map. The kernel's maps (node and state indices, the
// behavior registry) are consulted only for keyed lookup, never iterated to drive
// order, so no map-iteration nondeterminism can leak into a Fire. This holds under
// a value context (see above); a pointer context forfeits the clean-replay
// guarantee because a guard or action can mutate through the shared alias.
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
// and the outcome as pure data. Observability is opt-in, in keeping with the
// suite's no-op-default convention. By default an instance runs in LITE trace
// mode: each Fire still returns a Trace, but only the always-present fields
// (Machine, Event, FromState, MatchedAt, Outcome) are populated — enough for
// structured logging and a settled result, with no per-step diagnostic
// allocation. FULL trace mode populates the rich per-step fields
// (GuardsEvaluated, EffectsEmitted, ExitedStates, EnteredStates, AssignsApplied,
// Microsteps, EventPayload, SelectedTransition) and is enabled by attaching any
// observer at Cast: WithFullTrace, WithInspector, or one of the history options
// below. WithLogger(*slog.Logger) (no-op by default) reads only the always-present
// fields, so a logger-only instance stays lite; the kernel never logs unless
// asked and never imports a third-party logger.
//
// Trace history is retained only when requested, and is bounded by default:
// WithHistory(n) keeps the last n settled traces in a ring buffer (n <= 0
// disables retention), so a long-lived instance never grows its history without
// limit. WithUnboundedHistory opts into unbounded retention when a consumer
// genuinely wants every trace. With no history option, History returns nil.
// Determinism is preserved by injecting time and identifier seams rather than
// calling time.Now or rand directly.
//
// As a library, the kernel never exits the process — it never calls os.Exit or
// log.Fatal on an operational error. Panics are reserved strictly for
// programmer error at construction time (Quench).
//
// # Concurrency
//
// An Instance is NOT safe for concurrent use; the host must serialize all access
// (Fire/Snapshot/WaitFor) to a given Instance. The Instance carries no mutex and no
// atomic: Fire mutates the configuration, context, and history in place while
// Snapshot and the WaitFor family read those same fields, so touching one Instance
// from more than one goroutine without external synchronization is a data race. Run
// each Instance on a single goroutine (or guard it with the host's own lock);
// distinct Instances are independent and may run in parallel. A Machine, by
// contrast, is immutable after Quench and may be shared across goroutines freely —
// it is the per-instance runtime state, not the definition, that is single-owner.
//
// WaitFor in particular invites cross-goroutine misuse because it reads as a
// blocking wait, but it is NOT one: it is a synchronous driver-poll loop that runs
// entirely on the caller's goroutine, advancing the driver (which fires the
// instance) and re-reading the snapshot each iteration with no lock. WaitFor must be
// called on the same goroutine that owns the instance; never park one goroutine in
// WaitFor while another fires the same instance. The only cross-goroutine signal it
// honors is ctx cancellation, which it polls between steps.
//
// # Timer deadlines are a host concern (v1.0 contract)
//
// A delayed (`after`) transition arms a host timer through a ScheduleAfter effect
// carrying its FULL declared delay. The kernel snapshot intentionally does NOT carry
// absolute deadlines or remaining durations for pending timers: PendingRefs.Timers
// holds stable schedule IDs only, and ResumeEffects re-arms each pending timer with
// its full declared delay — the same effect a fresh entry would emit. A non-durable
// host that re-arms straight from ResumeEffects therefore restarts every `after`
// timer from zero on each restore, i.e. timer drift on every restore cycle.
//
// This is a deliberate v1.0 contract decision: the absolute deadline of an `after`
// timer is a HOST concern. A durable host persists the timer's absolute deadline out
// of band and re-arms with the REMAINING time on restore (this is exactly what the
// durable subpackage does); the kernel keeps the pure snapshot deadline-free. Note
// the consequence: a host rolling its OWN persistence (rather than using durable)
// must persist and restore timer deadlines itself, or accept the drift. Carrying
// deadlines in the kernel snapshot (e.g. a Pending.TimerDeadlines field) is a
// post-1.0 ADDITIVE item, not part of the v1.0 frozen surface.
//
// # Guard eval-error asymmetry (kernel policy)
//
// The kernel is the policy owner for what happens when a guard ERRORS during
// evaluation, and the policy is deliberately asymmetric by guard trigger:
//
//   - An EVENTLESS (`Always`) guard that errors fails CLOSED: the transition is
//     treated as not-taken and no error surfaces, so a faulty guard can never
//     silently enable an eventless transition (it simply does not fire).
//   - An EVENT-DRIVEN guard that errors fails LOUD: the error surfaces as a Fire
//     error (OutcomeGuardPanic), so a host learns immediately that an event it
//     dispatched hit a broken guard rather than silently dropping the event.
//
// The rationale is that an eventless transition is the kernel's own
// run-to-completion drive — failing it closed keeps a macrostep from looping on a
// broken guard — whereas an event-driven transition is a host request that deserves
// an explicit error. The expr subpackage documents the same asymmetry from its side;
// this is the kernel-contract statement of it.
//
// # Stability (v1.0 freeze)
//
// At v1.0 the kernel's data model and contracts are frozen. The freeze COVERS the
// machine definition and its serialized IR (the JSON wire form), the context model
// (value-context assign semantics), the effect envelope (kind + payload + meta and
// the KindedEffect/EffectEnvelope surface), and the emission-ordering contract (the
// determinism and ordering rules above). From 1.0.0 onward these arrive new
// capability as ADDITIVE packages, modules, and options rather than breaking
// changes; a MAJOR schema bump is refused rather than guessed at.
//
// The freeze does NOT cover the advisory tooling subpackages shipped alongside the
// release: analysis (static model-checking over a machine's IR), evolution
// (classifying the difference between two machine definitions), conformance (the
// cross-implementation harness), and verify (with symbolic). These are part of the
// RELEASE but their APIs and finding-shapes are ADVISORY and may change in a minor
// release; depend on them as tooling, not as a frozen contract. The frozen core
// kernel never imports them.
//
// # Initial configuration entry
//
// Entering the initial configuration at Cast runs the same entry semantics a
// transition runs when entering those states: OnEntry actions and OnEntryAssign
// reducers, `after` timer arming, invoke/actor starts, an enclosing compound's
// done / OnDone when initial descent lands on a final leaf, and any enabled
// eventless ("always") transition that settles the first stable configuration.
// The resulting effects are buffered on the instance and read once, right after
// Cast: InitialEffects returns the full initial-entry effect stream, and
// StartEffects returns the invoke/actor start subset. A host absorbs them through
// the same runner it uses for Fire's effects. This closes the prior gap where a
// compound whose initial child is final did not raise that compound's done at
// Cast; the initial descent now settles done along the active spine exactly as a
// transition commit does.
//
// # Status
//
// The kernel implements the Forge/Temper/Quench build path, Cast/Fire pure step
// semantics with guards, actions, typed errors and an opt-in structured Trace,
// Verify/Requirements, PlanPath (BFS), FireSeq/FireEach batch helpers, and
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
// deterministic settle-by-id harness for testing.
//
// Child-machine actors are live: a state may invoke another Machine as a
// sub-actor (InvokeActor) or spawn one dynamically (Spawn), driven by a host
// ActorSystem that runs the child, routes its done-data to the parent's onDone and
// its failure to the parent's onError, and carries inter-actor messages (SendTo /
// SendParent / Respond / Forward) between mailboxes — all as host-dispatched
// effects, so the pure Fire step still owns no mailbox and performs no IO. When a
// child fails and the parent declared no onError, the failure does not vanish: the
// default is escalate-to-parent — a typed *ActorEscalation recorded on the system
// (LastEscalation), surfaced to the inspector, climbed up the supervision chain,
// and optionally routed to a host EscalationHandler. Supervision STRATEGIES
// (restart / resume / backoff) layer additively on that frozen default.
//
// # Guard expressions
//
// A transition guard is authored at one of three graduated tiers, all bindings of
// the same frozen Guard data contract (context + params -> bool), so a machine
// mixes tiers freely and the tier is a property of the guard, not the kernel:
//
//   - Core — a small, dependency-free expression built with the in-package builder
//     (Field("…").Eq/Lt/In/…, And/Or/Not, StateIn) over a fixed vocabulary —
//     boolean composition, typed compare, membership, and state-tests. It lowers to
//     a serializable GuardNode tree (GuardKindCore) the kernel evaluates IN-KERNEL,
//     adds no dependency, serializes losslessly, and stays transparent to tooling
//     and analysis.
//   - Rich — a mature embedded expression engine (CEL) for cross-stack evaluation
//     and richer logic (arithmetic, map construction) than Core admits. It lives in
//     the opt-in github.com/stablekernel/crucible/state/expr module so the kernel
//     itself stays stdlib-only; a Rich guard is checked against the ContextSchema at
//     freeze time and serializes as a GuardKindRich node.
//   - Escape — a plain Go func registered as a named guard (Registry.Guard). It is
//     the always-available, maximally-expressive tier; it is opaque to the analyzer
//     and does not cross a serialization boundary, so reserve it for logic the
//     declarative tiers cannot express.
//
// Core and Rich guards are STRUCTURALLY read-only — an expression cannot mutate
// context. An Escape Go-func guard is read-only by CONTRACT (documented; under a
// value context the kernel's value semantics make a mutation a throwaway anyway).
//
// # Context schema
//
// A machine may declare a ContextSchema — a serializable description of the
// context type's fields and their types. It is the type contract the declarative
// guard tiers check against: a Core or Rich expression that references a field is
// validated against the schema at freeze time rather than failing at run time, and
// the schema is the data contract a cross-language evaluator binds the same machine
// to. It is optional; an Escape Go-func guard needs none.
//
// # Versioning, snapshots, and journal seams
//
// A definition carries a SchemaVersion (the IR wire form), an optional machine ID
// and definition version, and serializes losslessly with unknown fields preserved,
// so a newer document round-trips through an older loader without corruption and a
// higher MAJOR schema version is refused rather than guessed at. An instance
// snapshots to a versioned Snapshot and restores under a lenient version posture
// (accept-and-upgrade within a compatible range, reject across a major boundary;
// strict machine-version checking is opt-in via RejectMachineVersionMismatch). The
// Trace records a structured EventPayload alongside the human Event label so a
// recorded event stream replays the exact event — the journal/durable-execution
// seam the deterministic step makes sound.
package state
