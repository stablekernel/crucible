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

- Delayed-transition (`after`) scheduling: the runtime contract that makes the
  declarative `after` representation drivable, while keeping `Fire` pure.
  - **Schedule/cancel effects.** Entering a state that declares an `after`
    transition emits a `ScheduleAfter{ID, Delay, Event, State}` effect; exiting it
    before the delay elapses emits a `CancelScheduled{ID}` effect (xstate v5
    auto-cancel-on-exit). The kernel never reads a clock and never sleeps — it
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
- Guard combinators and the `stateIn` built-in, for xstate v5 guard parity.
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
- Transition-semantics parity with xstate v5: wildcard, forbidden, `reenter`, and
  `raise`.
  - **Wildcard catch-all.** `Transition.Wildcard` (DSL `OnAny()`) matches any event
    no specific `On`-keyed transition of the state handles. It is the lowest-priority
    candidate — tried only after every specific match fails — and the event still
    bubbles to ancestors when no wildcard fires.
  - **Forbidden transitions.** `Transition.Forbidden` (DSL `Forbid(event)` /
    `ForbidAny()`) blocks an event at a state: the event is consumed and ignored and,
    unlike an unhandled event, does NOT bubble to ancestors.
  - **`reenter` / internal-by-default.** A self- or ancestor-targeted transition is
    now internal by default (its effects run with no exit/re-entry of the source),
    matching v5. `Transition.Reenter` (DSL `Reenter()`) forces the external form,
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

[Unreleased]: https://github.com/stablekernel/crucible/compare/state/v0.1.0...HEAD
[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/state/v0.1.0
