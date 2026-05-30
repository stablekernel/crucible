# Changelog

All notable changes to `crucible/state` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
A machine definition is treated as a schema: see the
[Evolution Guide](https://github.com/stablekernel/crucible/discussions/6) for what
counts as an additive (minor) versus breaking (major) change. Use the
`state/evolution` package to classify a machine change and decide the bump.

## [Unreleased]

### Added

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
  plus micro-benchmarks for the previously-uncovered hot paths — guard-combinator
  and `stateIn` evaluation, hierarchical and deep-nested `Fire`, history
  record/restore, actor spawn + dispatch + message delivery, the `after`
  schedule + fire cycle, snapshot + restore, invoke start + settle, and
  `analysis.ShortestPaths`/`SimplePaths` over a branchy machine. All report
  allocations and join the existing benchstat gate.

### Fixed

- On-entry lifecycle effects (`after` / `invoke` / actor `spawn`) are now emitted
  for a state entered *inside* a parallel region. The region-entry path
  (`applyRegionTransition`) previously ran only transition effects, so a region
  substate declaring an `after` timeout, an invoked service, or an invoked actor
  silently never started it. The region path now emits the same
  `ScheduleAfter` / `StartService` / `SpawnActor` effects on entry — and the
  symmetric `CancelScheduled` / `StopService` / `StopActor` effects on exit — as
  the normal entry/exit cascade, for every state entered within a region
  (including nested compounds). `Fire` stays pure: the fix emits effect data, it
  does not run timers/services/actors in the kernel.

### Added

- Inspection API: a live observer sink for an instance's runtime activity. An
  `Inspector` (or the `InspectorFunc` closure adapter) receives
  `InspectionEvent`s tagged by `InspectKind` — an event received, a transition taken (carrying the live `Trace`),
  a snapshot update, an actor spawned/stopped, and a message sent/delivered between
  actors. Registered with **`WithInspector`** at `Cast` for the kernel-owned
  event/transition/snapshot stream, and **`ActorSystem.WithActorInspector`** for the
  host-owned actor-lifecycle and inter-actor message stream. It is off by default —
  a nil inspector is never called, so an un-inspected instance pays nothing and
  `Fire` stays pure (the notification is an in-memory observer call gated on a
  registered inspector, never IO).
- **`WaitFor(ctx, inst, predicate, ...opts)`**: a host-side helper that drives an
  instance until a predicate over its `Snapshot` holds, or the context/`timeout`
  budget elapses. It
  checks the predicate immediately, then advances a host driver one step at a time —
  **`WithWaitScheduler`** ticks a `Scheduler` over a `FakeClock` so `after`-driven
  machines progress deterministically, or **`WithWaitStepFunc`** supplies a bespoke
  driver. Time is measured on the instance's clock (a `FakeClock` in tests), so the
  whole wait is deterministic with no real sleeping. Returns the matching snapshot,
  or the typed **`*WaitTimeoutError`** on budget exhaustion. Helpers
  **`WaitInState`** and **`WaitDone`** cover the common predicates.
- Path enumeration in `state/analysis`:
  **`ShortestPaths(m)`** returns the shortest event sequence from the initial state
  to every reachable state — the multi-target generalization of the kernel's
  `PlanPath` — and **`SimplePaths(m)`** enumerates every acyclic (simple) path to
  each state, terminating even on machines with cycles by refusing to re-enter a
  state already on the current path. Both walk the same flattened IR graph the
  reachability checks use and are guard-agnostic (a static pass cannot evaluate host
  guards, and a guard only ever prunes an edge at run time), so they report the full
  structural scenario set a conformance harness draws coverage from. Paths expose
  `Events()`, `States(initial)`, and ordered `Step`s.
- Deep persistence / snapshots: capture a running `Instance`'s full runtime state
  and restore it to resume from exactly that point. The IR's
  `ToJSON` / `LoadFromJSON` persist the machine DEFINITION; a snapshot persists the
  INSTANCE runtime state — a different thing.
  - **`Instance.Snapshot()`** returns a serializable `Snapshot[S, E, C]` capturing
    the active configuration (all active leaves + spine, parallel regions, nested),
    the recorded per-compound history (shallow and deep), the bound context `C`, the
    ordered `Fire` traces, the lifecycle `Status` (`StatusRunning` / `StatusDone`,
    derived from whether the whole configuration is final; `StatusError` plus an
    error/output is host-set), and a `Pending` inventory of the timer / service /
    actor IDs armed for the configuration. It is a pure read — it never fires,
    mutates, or consults a clock — so `Fire` stays pure.
  - **`Machine.Restore(snap, ...)`** rebuilds an `Instance` resuming at the
    snapshot's configuration, context, and history WITHOUT re-running entry actions
    (resume, not re-enter). It validates the snapshot's machine
    name and every configuration leaf, returning the typed `*SnapshotError` on a
    mismatch, unknown leaf, or empty configuration. Wire a clock with
    `WithRestoreClock`.
  - **`Instance.ResumeEffects()`** emits the re-arm effects a host absorbs after
    restore to re-establish pending children: a `ScheduleAfter` per pending `after`
    timer, a `StartService` per invoked service, and a `SpawnActor` per
    child-machine actor active in the restored configuration — routed through the
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
    and performs no IO — it emits these as data alongside the transition's other
    effects, and a host runtime runs the service and feeds the result back through
    `Fire`. Invoke IDs are stable per `(machine, owning state, invoke index)` or
    set explicitly; derive one with `InvokeID`.
  - **Declarative invoke + service registry.** A state declares
    `Invocation{ID, Src, Input, OnDone, OnError}`; service implementations bind by
    name through `Registry.Service` / `Builder.Service`, parallel to guards and
    actions. An unbound service ref fails `Quench` with the typed `*ErrUnboundRef`
    (`Kind: "service"`), consistent with unbound guards/actions. Authored via the
    DSL `Invoke(src, onDone, onError, ...InvokeOption)` with `WithInput`,
    `WithServiceParams`, and `WithInvokeID`.
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
    mailbox, or routes a message — it emits these as data, and a host `ActorSystem`
    runs the child machine and routes its done/error back through the parent's
    `Fire`. Actor IDs are stable per `(machine, owning state, actor index)` or set
    explicitly; derive one with `ActorID`. The spawn/stop built-ins need no host
    registration, mirroring the `Cancel` built-in.
  - **Declarative actor invoke + runtime refs.** An `Invocation` gains a `Kind`
    (`ActorKindService` default vs `ActorKindMachine`) and a `SystemID`; the
    `InvokeActor(src, onDone, onError, ...)` DSL (with `WithInput`, `WithInvokeID`,
    `WithSystemID`) declares a child-machine actor whose `src` binds at the
    `ActorSystem` actor palette, not the service registry. Dynamic `Spawn(src, id,
    ...)` takes `WithSpawnInput`, `WithSpawnSystemID`, `WithSpawnOnDone`,
    `WithSpawnOnError`. An `ActorRef` (id + optional systemId) is a runtime handle a
    machine stores in its context to address an actor; refs are runtime, never IR.
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
  actor runtime — built-in actions that emit data effects the
  `ActorSystem` routes, so `Fire` stays pure.
  - **Send/stop built-in actions.** `SendTo(targetID, event, ...)` emits a
    `SendTo{TargetID, SystemID, Event}` effect the system delivers to the addressed
    actor; `SendParent(event)` emits a `SendParent{Event}` a child routes to its
    parent; `Respond(event)` emits a `RespondToSender{Event}` routed back to the
    sender of the event the actor is currently handling (a no-op when there is no
    identifiable sender); `ForwardTo(targetID, ...)` emits a
    `ForwardEvent{TargetID, SystemID}` that forwards the current event verbatim; and
    `StopChild(id)` emits a `StopActor{ID}` to stop a spawned actor. Address a target
    by registry id or, with `WithSendToSystemID`, by its system-scoped id. Like the
    spawn/stop/cancel built-ins, these need no host registration and are exempt from
    the unbound-ref lint.
  - **Sender-tracked routing in the `ActorSystem`.** Mailbox messages carry the
    origin actor; the system records it as the delivered event's sender, so a
    `RespondToSender` resolves the reply target, and parent/child routing resolves
    `SendParent` to the parent instance and `SendTo` / `ForwardTo` to the addressed
    actor. `Deliver` tags host-injected events with no origin; `AbsorbFor` lets a
    host's own forwardTo forward the event it just fired. The kernel emits the
    effects as data — it never delivers a message or owns the routing.
  - **Trace & IR.** Send/forward/stop actions record microsteps and round-trip
    losslessly through JSON (structural targets and the literal event serialize;
    refs stay runtime).
- Delayed-transition (`after`) scheduling: the runtime contract that makes the
  declarative `after` representation drivable, while keeping `Fire` pure.
  - **Schedule/cancel effects.** Entering a state that declares an `after`
    transition emits a `ScheduleAfter{ID, Delay, Event, State}` effect; exiting it
    before the delay elapses emits a `CancelScheduled{ID}` effect
    (auto-cancel-on-exit). The kernel never reads a clock and never sleeps — it
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
    the instance's active configuration includes the named state — its active
    leaves and their ancestor spine — so it is correct for atomic, compound, and
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
    candidate — tried only after every specific match fails — and the event still
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

## [0.1.0]

Initial release of the pure state-machine kernel.

### Added

- Kernel core: `Forge`/`Temper`/`Quench`/`Cast`/`Fire`/`Assay` foundry API with
  pure-function step semantics — firing an event returns `(newState, effects,
  trace)` with no IO.
- Serializable definition IR with lossless JSON round-trip; guards, actions, and
  effects are named references bound to a host-provided registry.
- Hierarchical (compound) and parallel (orthogonal-region) states.
- Path planning (`PlanPath`) over the machine graph, honoring guards.
- Mermaid and DOT export for state machines.
- Trace-first observability, functional options throughout, and injected
  clock/ID seams for determinism.
- Reusable conformance harness with golden scenarios.

[Unreleased]: https://github.com/stablekernel/crucible/compare/state/v0.2.0...HEAD
[0.2.0]: https://github.com/stablekernel/crucible/releases/tag/state/v0.2.0
[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/state/v0.1.0
