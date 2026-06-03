# crucible/state

A pure, embeddable **statechart engine** for Go, the kernel of the
[Crucible](../README.md) suite.

> **Status:** experimental, pre-1.0. The engine is feature-complete and
> extensively tested; the API may still change before v1.

Import path: `github.com/stablekernel/crucible/state`

## What it is

`state` is a **portable, domain-agnostic statechart engine**, generic over
state, event, and context types (`Machine[S, E, C]`). It knows nothing about any
particular application domain, so the same machine definition runs unchanged
from a unit test, a synchronous request handler, and an asynchronous event
consumer.

It is the extreme end of the suite's thin-seams philosophy: the engine is
**stdlib-only**, importing only the Go standard library and performing no
injected IO. A tiny dependency graph is a tiny attack surface, and the boundary
is enforced mechanically by an import-graph test.

### The pure kernel, host-driven drivers

The heart of the engine is a pure function. Firing an event returns
`(newState, effects, trace)` and performs no IO of its own:

```go
res := inst.Fire(ctx, Submit)
// res.NewState: the resulting configuration
// res.Effects:  abstract data the host dispatches
// res.Trace:    a structured record of what happened
```

Everything stateful (timers, invoked services, actors, mailboxes) lives in
**host-driven drivers** that the engine feeds with effect *data*. Entering a
state that arms a timer emits a `ScheduleAfter` effect; a host `Scheduler` owns
the real clock and re-fires the delayed event back through `Fire`. The same
pattern carries invoked services (`ServiceRunner`) and actors (`ActorSystem`).
Because the kernel only ever emits data and never starts a goroutine, reads a
clock, or touches the network, it stays portable and statically analyzable, and
every driver is deterministically testable with a `FakeClock`.

## Features

A complete statechart feature surface:

- **State kinds**: atomic, compound (hierarchical), **parallel** (orthogonal
  regions), and final states, nesting to arbitrary depth.
- **History**: shallow and deep history pseudo-states that re-enter a compound
  state's last active configuration rather than its initial child.
- **Guards**: named guard refs plus the **`And` / `Or` / `Not`** combinators and
  the config-aware **`stateIn`** built-in, composable into a serializable boolean
  expression tree.
- **Actions & effects**: named action refs with serializable params; actions
  return abstract effects the host dispatches.
- **Run-to-completion**: **eventless (`Always`) transitions**, **`Raise`** for
  internal events, and a macrostep loop that drains both to a stable
  configuration, with overflow protection.
- **Transition forms**: **wildcard** catch-alls (`OnAny`), **forbidden**
  transitions (`Forbid`), and **`Reenter`** to force the external (exit/entry)
  form of an otherwise-internal self-transition.
- **Delayed transitions**: `Transition(from).After(delay).On(event).GoTo(...)`,
  scheduled and auto-cancelled on exit by a host `Scheduler`.
- **Invoked services**: state-scoped `Invoke(src, onDone, onError)` with
  result/error routing, auto-stopped on exit, driven by a host `ServiceRunner`.
- **Actor model**: child-machine actors, a host `ActorSystem`, mailboxes, and
  dynamic `Spawn`, with **message passing** (`SendTo`, `SendParent`, `Respond`,
  `ForwardTo`, and `StopChild`) and sender-tracked routing.
- **Snapshots**: `Instance.Snapshot()` captures the full runtime state
  (configuration, history, context, traces, pending timers/services/actors);
  `Machine.Restore` resumes from it without re-running entry actions, and
  `ResumeEffects` re-arms pending children. Actor trees persist recursively.
- **Inspection & waiting**: an `Inspector` observer sink for the live
  event/transition/snapshot/actor stream, and `WaitFor(ctx, inst, predicate)`
  (plus `WaitInState` / `WaitDone`) that drives an instance until a predicate
  over its snapshot holds.
- **Opt-in tracing**: a Fire is lite by default (settled result and outcome,
  no per-step diagnostic allocation); `WithFullTrace`, `WithInspector`, or the
  history options populate the rich per-step Trace. Trace history is opt-in and
  bounded — `WithHistory(n)` keeps the last `n` traces in a ring buffer, with
  `WithUnboundedHistory` available when every trace is wanted.
- **Path enumeration**: `PlanPath` finds the shortest sequence to a target;
  `state/analysis` adds `ShortestPaths` and `SimplePaths` over the whole graph.

## What sets it apart

These are Crucible's own strengths, stated plainly:

- **Static analysis / model-checking**: `state/analysis` runs over a machine's
  IR to report reachability (unreachable/dead states), dead transitions,
  guardless nondeterminism, non-final dead ends, and liveness. Checks are exact
  where the IR proves them and heuristic where opaque guards limit certainty.
- **Serializable IR**: the canonical machine is pure data, lossless to and from
  JSON. Behavior is not embedded as closures; guards, actions, effects, and
  services are named references with serializable params, bound to a host
  registry at freeze time. A machine authored in Go and one loaded from JSON are
  the same machine.
- **Conformance harness**: a reusable harness drives golden scenarios against
  any machine, so a definition can be held to a fixed behavioral contract.
- **Machine-evolution diffing**: `state/evolution` classifies the difference
  between two definitions as additive or breaking and maps the result onto a
  SemVer bump, treating a machine definition as a schema.
- **Visualization**: Mermaid and DOT export, with delayed edges annotated by
  their delay.
- **Compile-time type safety**: the engine is generic over `S`, `E`, and `C`;
  states, events, and context are checked by the Go type system, not stringly
  typed.
- **Pluggable telemetry**: a `WithLogger(*slog.Logger)` seam (no-op by default)
  is the only logging hook; the engine never logs unless asked and never imports
  a third-party logger. Determinism is preserved by injecting time and ID seams.

## Foundry vocabulary

The lifecycle API uses a small "foundry" verb vocabulary. The noun stays plain
(the type is a `Machine`); only the verbs are themed:

| Verb     | Role                                                                   |
| -------- | ---------------------------------------------------------------------- |
| `Forge`  | Open the builder DSL.                                                   |
| `Temper` | Optional, non-failing dev-time diagnostics pass (lint / analysis).     |
| `Quench` | Freeze the definition into an immutable `Machine`; binds refs.         |
| `Cast`   | Pour a running instance from the machine.                              |
| `Fire`   | Send an event to an instance and advance it.                           |
| `Assay`  | Check that an externally-built entity is legally in a given state.     |

Operations that favor discoverability over metaphor stay plain: `PlanPath`,
`Requirements`, `Trace`, and the `To*` / `LoadFromJSON` serializers.

The public API follows the suite's functional-options convention: required
inputs stay positional; everything optional is a variadic option, so a
zero-option call reads clean and new capability arrives additively.

## Usage

A small document-approval machine, forged, frozen, and fired:

```go
package main

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

func main() {
	m := state.Forge[DocState, DocEvent, *Document]("document").
		Guard("hasReviewer", func(ctx state.GuardCtx[*Document]) bool {
			return ctx.Entity.ReviewerID != nil
		}).
		Action("emit", emit).
		State(Draft).
		State(Submitted).
		State(Approved).
		Initial(Draft).
		CurrentStateFn(func(d *Document) DocState { return d.Status }).
		Transition(Draft).On(Submit).GoTo(Submitted).
		Do("emit", state.P{"event": "submitted"}).
		Transition(Submitted).On(Approve).GoTo(Approved).
		When("hasReviewer").
		Quench(state.Strict())

	doc := &Document{Status: Draft}
	res := m.Cast(doc).Fire(context.Background(), Submit)

	fmt.Println("state:", res.NewState)   // Submitted
	fmt.Println("effects:", res.Effects)  // [{submitted}]
}
```

`Cast` returns a running `Instance`; `Fire` advances it and returns the new
state, the emitted effects, and the trace. The same machine can be serialized
with `m.ToJSON()`, reloaded with `state.LoadFromJSON`, analyzed with
`analysis.Analyze`, or rendered to Mermaid/DOT, all from the one definition.

## Subpackages

| Package                 | What it is                                                          |
| ----------------------- | ------------------------------------------------------------------ |
| `state/analysis`        | Static model-checking and path enumeration over a machine's IR.    |
| `state/evolution`       | Diffs two machine definitions and classifies the SemVer bump.      |
| `state/conformance`     | Reusable harness for driving golden scenarios against a machine.   |
| `state/verify`          | Property and temporal verification: reachability, liveness, invariants, bounded simulation, coverage, and covering-suite generation. |
| `state/verify/symbolic` | Bounded symbolic guard reasoning: satisfiability, disjointness, and competing-transition (nondeterminism) detection over the Core guard tree. |

## Stability

Stability label: **experimental** (pre-1.0; the API may change). Each module is
independently versioned per-module SemVer.

## Design & docs

Design rationale, concepts, and guides live on the
[documentation site](https://stablekernel.github.io/crucible/). See the
[state machine introduction](https://stablekernel.github.io/crucible/start/introduction/)
and [machine & instance concepts](https://stablekernel.github.io/crucible/concepts/machine-and-instance/).
For questions or proposals, open a GitHub issue.

## License

Apache-2.0. See the repository [LICENSE](../LICENSE) and [NOTICE](../NOTICE).
