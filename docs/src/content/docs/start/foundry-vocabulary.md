---
title: Foundry vocabulary
description: The lifecycle verbs — Forge, Temper, Quench, Cast, Fire, Assay — and what each one does.
sidebar:
  order: 3
---

`crucible/state` names its lifecycle after a foundry: you **Forge** a definition,
**Quench** it solid, **Cast** instances, and **Fire** events at them. The
metaphor is load-bearing — each verb marks a real boundary between mutable
authoring, an immutable definition, and a running instance.

## Lifecycle verbs

| Verb | Signature (shape) | What it does | When to call it |
| --- | --- | --- | --- |
| **Forge** | `Forge[S,E,C]("name") *Builder` | Opens a mutable builder. Declare states, transitions, and register named behavior (`.Guard`, `.Action`, `.Reducer`, `.Service`, `.Actor`). | Once, at the start of defining a machine. |
| **Temper** | `b.Temper() []Diagnostic` | Optional, non-failing lint/diagnostics pass over the builder. Returns findings; never panics, never freezes. | Before `Quench`, when you want to surface reachability or wiring warnings. |
| **Quench** | `b.Quench() *Machine` | Freezes the builder into an immutable `*Machine`. **Panics on misconfiguration** (unknown states, dangling refs). | Once, when the definition is complete. The `*Machine` is safe to share across goroutines. |
| **Cast** | `m.Cast(entity, opts...) *Instance` | Creates a running `*Instance` seeded with your context entity (by value). | Per entity you want to track. Cheap; cast freely. |
| **Fire** | `inst.Fire(ctx, event, opts...) FireResult` | Advances the instance. Returns `FireResult{NewState, Effects, Trace, Err}`. Performs **no IO** — effects are data. | Every time an event arrives. |
| **Assay** | `m.Assay(state, entity, opts...) error` | Verifies that an externally-built entity is *legally* in a given state, running the relevant guards. `FailFast` by default; `Aggregate()` collects all violations. | When an entity is reconstructed from storage or another system and you need to trust its state. |

## Plain verbs

Not everything earns a metaphor. These read literally:

| Verb | Signature (shape) | What it does |
| --- | --- | --- |
| `PlanPath` | `m.PlanPath(from, to, entity, opts...) ([]E, error)` | Computes a sequence of events that would drive an entity from one state to another. |
| `Trace` | (field on `FireResult`) | The ordered record of what happened during a `Fire` — transitions, guards, regions. |
| `ToJSON` / `LoadFromJSON` | `m.ToJSON()` / `LoadFromJSON[S,E,C](b)` | Round-trip the canonical IR losslessly to and from JSON. |
| `ToMermaid` / `ToDOT` | `m.ToMermaid()` / `m.ToDOT()` | Render the machine as a diagram for docs or inspection. |

A typical session uses **Forge → (Temper) → Quench** once, then **Cast** and
**Fire** many times — with **Assay** at the edges where entities cross trust
boundaries.
