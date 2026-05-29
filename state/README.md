# crucible/state

The pure, abstract state machine kernel of the [Crucible](../README.md) suite.

> **Status: scaffolding.** This module is a buildable, empty placeholder. Phase 1
> (Kernel Core + serializable IR + host registry) is pending per the roadmap. No
> types or logic have landed yet.

Import path: `github.com/stablekernel/crucible/state`

## What it is

`state` is a **portable, domain-agnostic state machine kernel**, generic over
state, event, and context types. It knows nothing about any particular
application domain, so the same machine definition runs unchanged from a unit
test, a synchronous request handler, and an asynchronous event consumer.

It is the extreme end of the suite's thin-seams philosophy: **stdlib-only**, no
injected IO, no forced dependencies. A tiny dependency graph is a tiny attack
surface.

### Highlights

- **Pure-function step semantics** — firing an event returns
  `(newState, effects, trace)` with no IO; the caller dispatches the effects.
- **Serializable definition IR** — the canonical machine is pure data, lossless
  to and from JSON. Guards, actions, and effects are named references with
  serializable params, bound to a host-provided registry.
- **Two front-ends, one IR** — a Go DSL and a future visual editor emit the same
  IR; a machine authored in code and one loaded from JSON are the same machine.
- **Foundry vocabulary** — `Forge`, `Temper`, `Quench`, `Cast`, `Fire`, `Assay`.
- **Functional options everywhere**, Trace-first observability, injected
  clock/ID seams for determinism.

## Stability

Stability label: **experimental** (pre-v1, scaffolding).

## Roadmap

Phase 1 (Kernel Core + IR + registry) → Phase 2 (HSM) → Phase 3 (Path Planning)
→ Phase 4 (Visualization) → Phase 5 (Conformance).

Design rationale and the full roadmap live on the GitHub Discussions board under
the **State Machine** category. See the
[Overview & Roadmap](https://github.com/stablekernel/crucible/discussions/1) and
[Kernel Core](https://github.com/stablekernel/crucible/discussions/2)
discussions.

## License

Apache-2.0. See the repository [LICENSE](../LICENSE) and [NOTICE](../NOTICE).
