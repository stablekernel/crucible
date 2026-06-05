# Changelog

All notable changes to `crucible/state` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
A machine definition is treated as a schema: see the
[Evolution Guide](https://stablekernel.github.io/crucible/analysis/evolution/) for what
counts as an additive (minor) versus breaking (major) change. Use the
`state/evolution` package to classify a machine change and decide the bump.

## [1.0.0]

The first stable release. The data model and contracts are now fixed: a machine
definition, its serialized IR, the context model, the effect envelope, and the
emission-ordering contract are all frozen so that future capabilities arrive as
additive packages, modules, and options rather than breaking changes. See the
"Performance baseline (v1.0.0)" note at the end of this section for the
representative hot-path numbers.

### Added

- Versioned IR envelope. A definition's serialized form now carries an explicit
  `schemaVersion` (stamped by `ToJSON`, currently `"1.0"` via
  `CurrentSchemaVersion`), an optional machine `id` and `version`, opaque
  `input`/`output` slots, and a `meta` namespace (`map[string]any`) at the
  machine, state, transition, and ref granularity for layout, documentation,
  tags, binding descriptors, and other out-of-band annotation. `LoadFromJSON`
  preserves unknown fields on nested nodes (machine, state, transition, `Ref`,
  `GuardNode`) so a document written by a newer build round-trips losslessly
  through an older one, and rejects only a higher *major* schema version (the
  typed `*ErrUnsupportedSchema`). IR encoding is deterministic (stable key order)
  so a definition hashes and diffs reproducibly.
- Closed-enum extension policy. Every IR enum that may grow (guard op, state
  kind, param type, descriptor kind, effect kind) has a documented
  unknown-variant rule: an unrecognized value is preserved verbatim on load and
  rejected only at evaluation/dispatch, never silently dropped or coerced.
- Context schema. A machine may declare a `ContextSchema` (reusing the palette
  `ParamType` vocabulary) describing its context shape; `SchemaOf[C]()` derives
  one from a Go type by reflection, and `Builder.WithContextSchema` attaches it.
  The schema type-checks Core guard expressions at authoring time and is the
  cross-stack data contract a Rich (`state/expr`) or polyglot binding evaluates
  against. It round-trips through the IR.
- Graduated guard expressions as logic-as-data. Guards are authored across three
  tiers that are all bindings of one frozen `Guard` data contract:
  - **Core.** A stdlib expression vocabulary (typed compare `eq`/`ne`/`lt`/...,
    field reference, literal, membership, and the boolean spine `And`/`Or`/`Not`
    plus `stateIn`) extending the existing `GuardNode` tree. It evaluates
    in-kernel with zero dependencies, type-checks against the `ContextSchema`,
    and stays fully transparent to analysis and visualization.
  - **Rich.** A CEL-backed tier in the opt-in `state/expr` module (see that
    module's changelog), surfaced to the kernel as an ordinary named-ref guard.
  - **Escape.** A plain Go func, always available, opaque to tooling.
  Core guards are structurally read-only (an expression cannot mutate context);
  Escape guards are read-only by contract.
- Assign reducers, the sole context-mutation site under the value-semantics
  context contract. An `AssignFn[C]` is a total pure reducer
  (`ctxView, event, params → C`) registered by `Registry.Reducer` (alias
  `Builder.Reducer`) and wired onto a transition by `Builder.Assign(name)`,
  splitting registration (the noun verb `Reducer`) from wiring (the verb `Assign`)
  to mirror Guard/When and Action/Do. The
  kernel folds the assigns declared on a transition's exit, transition, and entry
  phases (in that order, declaration order within each phase, each seeing the
  prior result), and the folded value becomes the instance's context at commit.
  An assign emits no effect and returns no error; the triggering event (or a
  service/actor result on an `onDone` transition) is in scope as `AssignCtx.Event`.
  The serializable `AssignBinding`/`AssignRequest`/`AssignResult` mirror the
  guard and action bindings so a reducer can run out-of-process in a future
  transport.
- Read-only context projection. Guards, actions, and services observe context
  through a `ContextView` (a read-only projection), keeping the write path
  exclusively in assigns. The view is the in-process seam a serialized,
  cross-stack context contract is built on.
- Snapshot version identity and journal seams. A snapshot carries a
  `CurrentSnapshotVersion`; restore applies a lenient version policy
  (accept-and-upgrade within a compatible range, reject across major, the typed
  `*SnapshotVersionError`). The trace records a structured event payload, and the
  snapshot reserves `Journal []JournalEntry` and in-flight service/mailbox slots
  for a future durable-execution/replay runtime, recorded as data, no behavior
  promised at v1.
- Actor escalation surface. An unhandled child-actor failure now escalates to the
  parent (see Changed/BREAKING below). The escalation is observable on the
  `ActorSystem` via `LastEscalation` and routable via `WithEscalationHandler`
  (the `EscalationHandler` callback receives the `*ActorEscalation`); an
  inspector also sees it.
- Typed effect envelope with a kind registry: every kernel-emitted effect now
  carries a stable, serializable `Kind()` discriminant (the new `KindedEffect`
  interface, implemented by `SpawnActor`, `StopActor`, `StartService`,
  `StopService`, `ScheduleAfter`, `CancelScheduled`, `SendTo`, `SendParent`,
  `RespondToSender`, and `ForwardEvent`), so effects can be journaled, deduped,
  rendered, and routed across a serialization boundary by kind rather than by Go
  type assertion. A new serializable `EffectEnvelope` (`kind` + `payload` +
  optional `meta`, with a reserved-but-not-yet-stable `effectId` slot) is the wire
  form; `MarshalEffect` produces it and an `EffectRegistry` (built-ins
  pre-registered, host kinds added through the `RegisterEffect` functional option
  on `NewEffectRegistry`) decodes it back to a concrete effect. Per the
  closed-enum extension policy, an unrecognized effect kind is preserved verbatim
  on load (surfaced as `UnknownEffect`) and rejected only at dispatch
  (`EffectRegistry.Dispatchable` returns the typed `*ErrUnknownEffectKind`), never
  silently dropped or applied. Effects remain data the host applies; the kernel
  does not execute them. The `Effect` alias stays `any`, so bare domain effects
  are unaffected.
- Registry descriptors and `Registry.Palette()`: registered guards, actions,
  services, and actor behaviors are now discoverable with metadata and a
  parameter schema, so a builder API or UI can enumerate the host's behavior and
  render a form for each ref. A new serializable `Descriptor`
  (`Kind`/`Name`/`Description`/`Params`/`Reads`/`Writes`) and `ParamSpec`
  (`Name`/`Type`/`Required`/`Description`/`Default`/`Enum`) over a minimal
  `ParamType` set (string, int, float, bool, duration, enum) JSON-serialize
  cleanly for transport. Registration gains a backward-compatible options tail,
  `reg.Guard(name, fn, state.Describe("...").Param("min", state.IntParam)...)`, and a
  new `reg.Actor(name, ...)` declares actor behaviors (which bind at the
  `ActorSystem`) in the palette; registering without a descriptor still works and
  yields a minimal descriptor with just kind and name. `Palette()` returns every
  consumer-registered entry sorted deterministically (by kind, then name);
  `Builder.Palette()` / `Machine.Palette()` surface the same set for a DSL- or
  `Provide`-built machine, and `Provide` carries descriptors over from the supplied
  registry. A separate `BuiltinPalette()` lists the language-level built-ins
  (`spawn`/`stopActor`/`sendTo`/`sendParent`/`respond`/`forwardTo`/`cancel`
  actions and the `stateIn` guard), which are intentionally excluded from
  `Palette()`. Descriptors are metadata only; they never affect binding, lint, or
  `Fire` semantics.
- End-to-end exemplar: a realistic connection-lifecycle machine
  (`Disconnected → Connecting → Backoff → Connected{ Live: Heartbeat ‖ Work } →
  Closing → Closed`) that exercises hierarchy, parallel regions, deep-history
  resume, guard combinators (`And`/`Or`/`Not`) with the `stateIn` built-in,
  eventless run-to-completion, a delayed (`after`) retry backoff, an invoked dial
  service, and a spawned worker actor. It is driven end-to-end through the wired
  host runtime (`ActorSystem` + `Scheduler`/`FakeClock` + `ServiceRunner`) in an
  e2e test (happy path, deep-history reconnect, and a snapshot/restore-mid-run
  identity check) and exposed as a runnable `Example`.
- Benchmarks: an end-to-end `BenchmarkE2E_ConnectionLifecycle` over the exemplar,
  plus micro-benchmarks for the previously-uncovered hot paths: guard-combinator
  and `stateIn` evaluation, hierarchical and deep-nested `Fire`, history
  record/restore, actor spawn + dispatch + message delivery, the `after`
  schedule + fire cycle, snapshot + restore, invoke start + settle, and
  `analysis.ShortestPaths`/`SimplePaths` over a branchy machine. All report
  allocations and join the existing benchstat gate.
- Inspection API: a live observer sink for an instance's runtime activity. An
  `Inspector` (or the `InspectorFunc` closure adapter) receives
  `InspectionEvent`s tagged by `InspectKind`: an event received, a transition taken (carrying the live `Trace`),
  a snapshot update, an actor spawned/stopped, and a message sent/delivered between
  actors. Registered with **`WithInspector`** at `Cast` for the kernel-owned
  event/transition/snapshot stream, and **`ActorSystem.WithActorInspector`** for the
  host-owned actor-lifecycle and inter-actor message stream. It is off by default:
  a nil inspector is never called, so an un-inspected instance pays nothing and
  `Fire` stays pure (the notification is an in-memory observer call gated on a
  registered inspector, never IO).
- **`WaitFor(ctx, inst, predicate, ...opts)`**: a host-side helper that drives an
  instance until a predicate over its `Snapshot` holds, or the context/`timeout`
  budget elapses. It
  checks the predicate immediately, then advances a host driver one step at a time.
  **`WithWaitScheduler`** ticks a `Scheduler` over a `FakeClock` so `after`-driven
  machines progress deterministically, or **`WithWaitStepFunc`** supplies a bespoke
  driver. Time is measured on the instance's clock (a `FakeClock` in tests), so the
  whole wait is deterministic with no real sleeping. Returns the matching snapshot,
  or the typed **`*WaitTimeoutError`** on budget exhaustion. Helpers
  **`WaitInState`** and **`WaitDone`** cover the common predicates.
- Path enumeration in `state/analysis`:
  **`ShortestPaths(m)`** returns the shortest event sequence from the initial state
  to every reachable state (the multi-target generalization of the kernel's
  `PlanPath`), and **`SimplePaths(m)`** enumerates every acyclic (simple) path to
  each state, terminating even on machines with cycles by refusing to re-enter a
  state already on the current path. Both walk the same flattened IR graph the
  reachability checks use and are guard-agnostic (a static pass cannot evaluate host
  guards, and a guard only ever prunes an edge at run time), so they report the full
  structural scenario set a conformance harness draws coverage from. Paths expose
  `Events()`, `States(initial)`, and ordered `Step`s.
- Deep persistence / snapshots: capture a running `Instance`'s full runtime state
  and restore it to resume from exactly that point. The IR's
  `ToJSON` / `LoadFromJSON` persist the machine DEFINITION; a snapshot persists the
  INSTANCE runtime state, a different thing.
  - **`Instance.Snapshot()`** returns a serializable `Snapshot[S, E, C]` capturing
    the active configuration (all active leaves + spine, parallel regions, nested),
    the recorded per-compound history (shallow and deep), the bound context `C`, the
    ordered `Fire` traces, the lifecycle `Status` (`StatusRunning` / `StatusDone`,
    derived from whether the whole configuration is final; `StatusError` plus an
    error/output is host-set), and a `Pending` inventory of the timer / service /
    actor IDs armed for the configuration. It is a pure read: it never fires,
    mutates, or consults a clock, so `Fire` stays pure.
  - **`Machine.Restore(snap, ...)`** rebuilds an `Instance` resuming at the
    snapshot's configuration, context, and history WITHOUT re-running entry actions
    (resume, not re-enter). It validates the snapshot's machine
    name and every configuration leaf, returning the typed `*SnapshotError` on a
    mismatch, unknown leaf, or empty configuration. Wire a clock with
    `WithRestoreClock`.
  - **`Instance.ResumeEffects()`** emits the re-arm effects a host absorbs after
    restore to re-establish pending children: a `ScheduleAfter` per pending `after`
    timer, a `StartService` per invoked service, and a `SpawnActor` per
    child-machine actor active in the restored configuration, routed through the
    same `Scheduler` / `ServiceRunner` / `ActorSystem` the host drives for `Fire`.
    It is the restore twin of `StartEffects` extended with delayed-timer re-arming;
    entry actions are never re-run.
  - **Context serialization.** A snapshot round-trips through JSON when `C` is
    JSON-marshalable (the default requirement, via the snapshot's own
    `MarshalJSON` / `UnmarshalJSON`). For a context that is not directly
    JSON-marshalable, supply a `ContextCodec[C]` through `WithContextCodec` and
    serialize with `MarshalSnapshot` / `UnmarshalSnapshot`.
  - **Recursive actor-tree persistence.** `ActorSystem.SnapshotActors()` captures
    every live child actor recursively (each actor's own spawned children beneath
    it) keyed by id, and `RestoreActors(ctx, snaps)` re-spawns them from the palette
    under their original ids and resumes each child in place via the `Snapshotter`
    interface (which the standard `actorAdapter` satisfies). Deferred depth: an
    actor whose `ActorInstance` does not implement `Snapshotter` is re-spawned fresh
    rather than resumed (flagged on the snapshot's `Resumed` field), and a snapshot
    is taken at a quiescent point so an undrained mailbox backlog is not persisted.
  - The `state` package stays stdlib-only; snapshot capture and restore perform no
    IO and keep `Fire` pure.

- Invoked services (`invoke`): state-scoped service invocation with `onDone` /
  `onError` routing, host-driven so `Fire` stays pure.
  - **Start/stop effects.** Entering a state that declares an `invoke` emits a
    `StartService{ID, Src, Input, OnDone, OnError, State}` effect; exiting it
    before the service completes emits a `StopService{ID}` effect
    (auto-stop-on-exit). The kernel never runs a service, never starts a goroutine,
    and performs no IO. It emits these as data alongside the transition's other
    effects, and a host runtime runs the service and feeds the result back through
    `Fire`. Invoke IDs are stable per `(machine, owning state, invoke index)` or
    set explicitly; derive one with `InvokeID`.
  - **Declarative invoke + service registry.** A state declares
    `Invocation{ID, Src, Input, OnDone, OnError}`; service implementations bind by
    name through `Registry.Service` / `Builder.Service`, parallel to guards and
    actions. An unbound service ref fails `Quench` with the typed `*ErrUnboundRef`
    (`Kind: "service"`), consistent with unbound guards/actions. Authored via the
    DSL `Invoke(src, ...InvokeOption)` whose outcomes are options —
    `WithInvokeOnDone` / `WithInvokeOnError` — alongside `WithInput`,
    `WithServiceParams`, and `WithInvokeID`, so completion routing is additive
    (matching `Spawn`) rather than positional.
  - **Host-driver harness.** A reusable, exported `ServiceRunner` driver consumes
    the start/stop effects, runs the bound `ServiceFn`, and re-fires each service's
    `onDone` (carrying the result) or `onError` (carrying the error) through the
    instance; `SettleDone` / `SettleError` settle a service by ID for a
    deterministic test driver with no real IO, while `Run` resolves and executes a
    bound service for production. `LastResult` / `LastError` let an onDone/onError
    action read the routed payload, and `StartEffects` arms the services of the
    initial state entered at `Cast`.
  - **Trace & IR.** Service start/stop record microsteps; the `invoke` block (id,
    src ref + params, input, onDone/onError) round-trips losslessly through JSON.
- Actor model: child-machine actors, an actor system, mailboxes, delivery, and
  lifecycle, host-driven so `Fire` stays pure and the
  kernel stays stdlib-only.
  - **Spawn/stop effects.** Entering a state that invokes a child `Machine`
    (`InvokeActor`) emits a `SpawnActor{ID, Src, Input, OnDone, OnError, State,
    SystemID}` effect; exiting it before the child reaches its final state emits a
    `StopActor{ID}` effect (auto-stop-on-exit). A built-in `spawn` action emits a
    `SpawnActor` from a transition for dynamic, runtime-created actors, and a
    `stopActor` built-in emits a `StopActor`. The kernel never runs an actor, owns a
    mailbox, or routes a message. It emits these as data, and a host `ActorSystem`
    runs the child machine and routes its done/error back through the parent's
    `Fire`. Actor IDs are stable per `(machine, owning state, actor index)` or set
    explicitly; derive one with `ActorID`. The spawn/stop built-ins need no host
    registration, mirroring the `Cancel` built-in.
  - **Declarative actor invoke + runtime refs.** An `Invocation` gains a `Kind`
    (`ActorKindService` default vs `ActorKindMachine`) and a `SystemID`; the
    `InvokeActor(src, ...InvokeOption)` DSL (with `WithInvokeOnDone`,
    `WithInvokeOnError`, `WithInput`, `WithInvokeID`, `WithSystemID`) declares a
    child-machine actor whose `src` binds at the
    `ActorSystem` actor palette, not the service registry. Dynamic `Spawn(src, id,
    ...)` takes `WithSpawnInput`, `WithSpawnSystemID`, `WithSpawnOnDone`,
    `WithSpawnOnError`. An `ActorRef` is an opaque runtime handle a machine stores
    in its context to address an actor (id, optional systemId, src, and a `Node`
    locator that is empty for a local actor and names the owning host for a remote
    one); refs are runtime, never IR.
  - **Host-driver harness.** A reusable, exported `ActorSystem` driver consumes the
    spawn/stop effects, runs each child machine as an actor with its own mailbox via
    `NewActor`, and re-fires the parent's `onDone` (carrying the child's `output`) or
    `onError` when the child completes or fails. `Register` binds child behaviors;
    `Absorb` spawns/stops from effects; `Deliver` / `DeliverByID` route an event into
    an actor's mailbox and `Step` drains it; `Ref` / `RefBySystemID` resolve refs;
    `Stop` / `SettleError` tear down or fail an actor; stopping a parent stops its
    children recursively. `LastOutput` / `LastError` let an `onDone` / `onError`
    action read the routed payload. The driver is synchronous and deterministic, so
    actor machines are fully testable without real concurrency. `InFinal` reports a
    child's completion. The message-send action sugar (`sendTo` / `sendParent` /
    `respond` / `forwardTo`) builds on this delivery mechanism and arrives next.
  - **Trace & IR.** Actor spawn/stop record microsteps; an `InvokeActor` block
    (kind, src ref + params, input, systemId, onDone/onError) round-trips losslessly
    through JSON, and a dynamic `Spawn` built-in's params survive too; actor refs are
    runtime and intentionally absent from the IR.
- Actor communication actions: the action-level send/stop sugar on top of the
  actor runtime. Built-in actions that emit data effects the
  `ActorSystem` routes, so `Fire` stays pure.
  - **Send/stop built-in actions.** `SendTo(targetID, event, ...)` emits a
    `SendTo{TargetID, SystemID, Event}` effect the system delivers to the addressed
    actor; `SendParent(event)` emits a `SendParent{Event}` a child routes to its
    parent; `Respond(event)` emits a `RespondToSender{Event}` routed back to the
    sender of the event the actor is currently handling (a no-op when there is no
    identifiable sender); `ForwardTo(targetID, ...)` emits a
    `ForwardEvent{TargetID, SystemID}` that forwards the current event verbatim. A
    single `StopActor(id)` verb stops a spawned or invoked-child actor from a
    transition (emitting `StopActor{ID}` via the one `crucible.stopActor` built-in).
    Address a target by registry id or, with `WithSendToSystemID`, by its
    system-scoped id. Like the
    spawn/stop/cancel built-ins, these need no host registration and are exempt from
    the unbound-ref lint.
  - **Sender-tracked routing in the `ActorSystem`.** Mailbox messages carry the
    origin actor; the system records it as the delivered event's sender, so a
    `RespondToSender` resolves the reply target, and parent/child routing resolves
    `SendParent` to the parent instance and `SendTo` / `ForwardTo` to the addressed
    actor. `Deliver` tags host-injected events with no origin; `AbsorbFor` lets a
    host's own forwardTo forward the event it just fired. The kernel emits the
    effects as data; it never delivers a message or owns the routing.
  - **Trace & IR.** Send/forward/stop actions record microsteps and round-trip
    losslessly through JSON (structural targets and the literal event serialize;
    refs stay runtime).
- Delayed-transition (`after`) scheduling: the runtime contract that makes the
  declarative `after` representation drivable, while keeping `Fire` pure.
  - **Schedule/cancel effects.** Entering a state that declares an `after`
    transition emits a `ScheduleAfter{ID, Delay, Event, State}` effect; exiting it
    before the delay elapses emits a `CancelScheduled{ID}` effect
    (auto-cancel-on-exit). The kernel never reads a clock and never sleeps. It
    emits these as data alongside the transition's other effects, and a host
    runtime owns the real timer and feeds the delayed event back through `Fire`.
    Schedule IDs are stable per `(machine, source state, delayed edge)`; derive
    one with `ScheduleID`.
  - **DSL.** `Transition(from).After(delay).On(event).GoTo(target)` declares a
    timed edge; `Cancel(id)` attaches the kernel Cancel built-in so a machine can
    explicitly drop a pending delayed event. The Cancel built-in needs no host
    registration, mirroring the `stateIn` guard built-in.
  - **Host-driver harness.** A reusable, exported `Scheduler` driver consumes the
    schedule/cancel effects and re-fires delayed events; `WithClock` injects the
    time seam (used only by the driver, never by `Fire`), with `SystemClock()` for
    production and a deterministic `FakeClock` for tests, so `after` machines are
    fully testable without real waiting.
  - **Trace & IR.** Schedule, cancel, and delayed fires record microsteps; the
    `after` delay + target round-trip losslessly through JSON, and visualization
    annotates a delayed edge with its delay.
- Guard combinators and the `stateIn` built-in.
  - **Combinators.** `And(...)`, `Or(...)`, and `Not(...)` compose guards into a
    serializable boolean expression tree whose leaves are named-ref guards
    (`Guard(name, params...)`) or the `stateIn` built-in, nested to any depth
    (e.g. `And(Or(g1, g2), Not(g3))`). Evaluation short-circuits exactly like a
    plain multi-guard transition: `And` stops at the first false, `Or` at the
    first true. A failing composite reports the failing leaf(s) when cheap, else
    the composite, preserving the typed `ErrGuardFailed`; a leaf panic still
    surfaces as `ErrGuardPanic`.
  - **`stateIn(state)`.** A first-class, config-aware built-in guard, true when
    the instance's active configuration includes the named state (its active
    leaves and their ancestor spine), so it is correct for atomic, compound, and
    parallel configurations. It needs no registration; the kernel evaluates it
    directly against the live configuration at Fire time.
  - **IR.** A transition carries an optional `GuardExpr *GuardNode[S]` alongside
    the plain `Guards` slice; the two are AND-composed (both must pass). The
    expression tree serializes and round-trips losslessly through JSON, leaf
    refs bind through `Provide` against the host registry exactly like plain
    guards, and a malformed tree or an unbound leaf fails at `Quench` with the
    same typed errors. The common single-named-guard case stays the plain
    `Guards` slice. Authored via the DSL `WhenExpr(expr)`. The `evolution` differ
    classifies composite-guard leaves (including `stateIn` targets) as guard
    requirements, and the `analysis` and visualization passes treat a transition
    with a `GuardExpr` as guarded.
- Transition semantics: wildcard, forbidden, `reenter`, and
  `raise`.
  - **Wildcard catch-all.** `Transition.Wildcard` (DSL `OnAny()`) matches any event
    no specific `On`-keyed transition of the state handles. It is the lowest-priority
    candidate (tried only after every specific match fails), and the event still
    bubbles to ancestors when no wildcard fires.
  - **Forbidden transitions.** `Transition.Forbidden` (DSL `Forbid(event)` /
    `ForbidAny()`) blocks an event at a state: the event is consumed and ignored and,
    unlike an unhandled event, does NOT bubble to ancestors.
  - **`reenter` / internal-by-default.** A self- or ancestor-targeted transition is
    now internal by default (its effects run with no exit/re-entry of the source).
    `Transition.Reenter` (DSL `Reenter()`) forces the external form,
    running the target's exit then entry. Existing transitions are unaffected:
    ordinary transitions to a distinct target keep their full cascade.
  - **`raise`.** `Transition.Raise` (DSL `Raise(events...)`) enqueues internal events
    processed within the same `Fire` macrostep. `Fire` now drives a run-to-completion
    loop that drains raised events (FIFO) and auto-fires enabled eventless ("always")
    transitions until the configuration is stable, recording each as a Trace
    microstep. The internal queue is macrostep-local, so `Fire` stays pure. An
    unhandled raised event is ignored; a non-settling raise/eventless cycle fails fast
    with the typed `ErrMicrostepOverflow`.
  - DSL also gains `Always()` to author eventless transitions directly (previously
    IR-only). The wildcard target, forbidden marker, reenter flag, and raised-event
    list serialize in the IR and round-trip losslessly through JSON; `raise` is
    carried structurally as part of the transition.
- Arbitrarily nested superstates in the builder DSL. A `SuperState` block may now
  contain another `SuperState` block (and so on, to any depth), so a deep
  hierarchy can be authored entirely through the chained DSL rather than only via
  the IR/`Provide` path. The entry cascade descends through every level to the
  deepest initial leaf, the exit cascade unwinds innermost-first across all
  levels, child-first event resolution bubbles up through every ancestor, and a
  nested compound's `done` event propagates upward as each level completes.
  Deep history authored via the DSL now restores the full nested leaf
  configuration, and the IR round-trips losslessly at arbitrary depth. The
  remaining superstate lints (a compound with substates needs an `Initial`,
  unclosed blocks, etc.) are unchanged.
- History pseudo-states (shallow and deep). A history pseudo-state belongs to a
  compound state and remembers that compound's last active configuration;
  transitioning to it re-enters the remembered configuration instead of the
  compound's initial child. Shallow restores the last active direct child; deep
  restores the full nested leaf configuration. With no recorded history the
  resolver falls back to the history state's declared default target, else the
  compound's initial. Declared via `Builder.History(name, HistoryShallow|
  HistoryDeep)` with optional `Builder.DefaultTo(target)`. The recorded
  per-compound configuration is per-instance runtime state threaded through
  `Fire` (which stays pure); the pseudo-states themselves serialize, so machines
  with history round-trip losslessly through the IR. A Quench lint flags a
  history state declared outside a compound state.
- `state/analysis` package: static model-checking over a machine's IR. `Analyze`
  returns a classified `Report` of `Finding`s covering reachability
  (unreachable/dead states), dead transitions, guardless nondeterminism,
  non-final dead ends, and liveness (states that can never reach a final state).
  Reachability reuses the kernel's breadth-first graph walk; checks are exact
  where the IR proves them and heuristic where opaque guards limit static
  certainty. Restrict the pass with the `Only`/`Without` options.
- `state/evolution` package: classifies the difference between two machine
  definitions as additive or breaking per the Evolution Guide, and maps the
  result onto a semantic-version bump (`Diff`, `DiffJSON`, `DiffMachines`,
  `Report.Breaking`, `Report.SemverBump`).

### Changed

- **BREAKING: context is now value-semantic; actions no longer mutate context.**
  The context model is frozen: a context value `C` flows through the step as data,
  guards and actions observe it through a read-only projection, and the *only*
  place context changes is an assign reducer. Actions emit effects; they cannot
  write context. A consumer that previously mutated the context through a pointer
  inside an action must move those writes into an `AssignFn` registered with
  `Registry.Reducer`/`Builder.Reducer` and wired with `Builder.Assign(name)`. This
  is the central change for clean serialization, deterministic replay, and
  cross-stack evaluation.
- **BREAKING: the reserved `ContextDelta` slot on the action result is removed.**
  Under the value-semantics contract, a context change is the value an assign
  reducer returns (`AssignResult.Context`), not a delta carried back from an
  action. Code referencing `ActionResult.ContextDelta` must drop it and move the
  write into an assign.
- **BREAKING: unhandled child-actor failure now escalates to the parent.** A
  child actor that fails with no `onError` route previously had its failure
  swallowed silently. It now escalates to the parent: the failure is recorded on
  the `ActorSystem` (`LastEscalation`), surfaced to a registered inspector, and
  delivered to a `WithEscalationHandler` callback if one is wired. Wire an
  `onError` route, an escalation handler, or read `LastEscalation` rather than
  relying on the old silent behavior.
- **BREAKING: the built-in effect structs serialize with stable lower-camel JSON
  keys, and the `Trace.EffectsEmitted` suffix is the stable effect `Kind`.** The
  built-in effect structs carry JSON field tags so their serialized form is
  lower-camel and stable (`{"id":...,"src":...}` rather than the Go field names), and
  a `Trace.EffectsEmitted` label records an effect's stable `Kind` in place of its
  Go type name (the `name:...` ref prefix is unchanged, so conformance ref-name
  assertions are unaffected). A host that serialized a built-in effect struct
  directly, or that parsed the type-name suffix of an `EffectsEmitted` label, must
  update; type-switching on the effect structs is unaffected (the structs only
  gained methods and tags).
- **BREAKING: the state trust-boundary check `Assay` is renamed `Verify`.** The
  method `Machine.Assay`, its error type `AssayError`, and its option type
  `AssayOption` become `Machine.Verify`, `VerifyError`, and `VerifyOption`. The
  rename trades the foundry metaphor for a plain, discoverable verb; the
  behavior and signatures are otherwise unchanged. Replace `Assay`, `AssayError`,
  and `AssayOption` at the call site.
- **BREAKING: the `Verify` option `WithAggregate` is renamed `Aggregate`.** The
  option that makes `Verify` collect all failing requirements in one pass instead
  of failing fast is now `Aggregate()`. Replace `WithAggregate()` with
  `Aggregate()` at the call site.
- The determinism and ordering contract is now explicit and frozen: emission
  order is exit → transition → entry across the cascade, declaration order within
  a set, fixed parallel-region order, and the run-to-completion interleave for
  raised/eventless transitions. A golden-order regression test locks it so a
  journal or replay built on top stays stable.

### Fixed

- `Cast` returns the typed `*ErrInvalidTransition` consistently for an event that
  matches no transition, including inside parallel regions, so a caller can
  distinguish "no transition" from other failures uniformly.
- On-entry lifecycle effects (`after` / `invoke` / actor `spawn`) are now emitted
  for a state entered *inside* a parallel region. The region-entry path
  (`applyRegionTransition`) previously ran only transition effects, so a region
  substate declaring an `after` timeout, an invoked service, or an invoked actor
  silently never started it. The region path now emits the same
  `ScheduleAfter` / `StartService` / `SpawnActor` effects on entry (and the
  symmetric `CancelScheduled` / `StopService` / `StopActor` effects on exit), like
  the normal entry/exit cascade, for every state entered within a region
  (including nested compounds). `Fire` stays pure: the fix emits effect data, it
  does not run timers/services/actors in the kernel.

### Performance baseline (v1.0.0)

Representative numbers from `go test -run=^$ -bench=. -benchmem ./...` on the
`state` module (Apple silicon, `-14`). These are a baseline for regression
tracking, not a tuning target; `Fire` allocates because every step returns a
fresh trace and effect set as data.

| Benchmark | ns/op | B/op | allocs/op | What it measures |
|-----------|------:|-----:|----------:|------------------|
| `Fire` | ~1,005 | 1,185 | 34 | flat hot-path step (event → next state, effects, trace) |
| `FireHierarchical/hierarchical` | ~2,483 | 2,819 | 81 | compound entry/exit cascade |
| `FireHierarchical/nested` | ~1,752 | 2,611 | 50 | deep-nested cascade |
| `Assign_ContextCopyPerStep` | ~1,312 | 2,335 | 29 | per-step context copy cost under value-semantic context |
| `GuardExpr/flat` | ~1,041 | 1,273 | 36 | single Core guard expression |
| `Cascade` | ~1,221 | 1,385 | 43 | entry/exit effect cascade |
| `SnapshotRestore` | ~28,550 | 15,108 | 121 | snapshot capture + restore |
| `E2E_ConnectionLifecycle` | ~46,187 | 59,856 | 657 | end-to-end exemplar over the wired host runtime |

## [0.1.0]

Initial release of the pure state-machine kernel.

### Added

- Kernel core: `Forge`/`Temper`/`Quench`/`Cast`/`Fire`/`Verify` foundry API with
  pure-function step semantics. Firing an event returns `(newState, effects,
  trace)` with no IO.
- Serializable definition IR with lossless JSON round-trip; guards, actions, and
  effects are named references bound to a host-provided registry.
- Hierarchical (compound) and parallel (orthogonal-region) states.
- Path planning (`PlanPath`) over the machine graph, honoring guards.
- Mermaid and DOT export for state machines.
- Trace-first observability, functional options throughout, and injected
  clock/ID seams for determinism.
- Reusable conformance harness with golden scenarios.

[1.0.0]: https://github.com/stablekernel/crucible/compare/state/v0.2.0...state/v1.0.0
[0.2.0]: https://github.com/stablekernel/crucible/releases/tag/state/v0.2.0
[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/state/v0.1.0
