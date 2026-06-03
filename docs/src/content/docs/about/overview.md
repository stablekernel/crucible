---
title: What Crucible is
description: A high-level overview of the Crucible suite and the philosophy behind it — facilitate how you build event-driven services, not prescribe what you use.
sidebar:
  order: 1
---

Crucible is a multi-module Go suite for building event-driven services:
statecharts, durable execution, egress, and the seams between them. It is built
on one conviction — **facilitate how you build, don't prescribe what you use.**

## What it's for

Event-driven systems accumulate cross-cutting machinery: logging, tracing,
metrics, IDs, clocks, serialization, transport. Most frameworks make you adopt
their version of each, and that choice spreads through your codebase until the
framework owns your architecture.

Crucible takes the opposite stance. Every cross-cutting concern is a small,
consumer-providable interface with a do-nothing default. You bring your logger,
your tracer, your clock. Crucible never makes you adopt its choices, and never
leaks a third-party type into a public signature. The result is a toolkit you
can drop into an existing codebase one piece at a time, with no framework
lock-in and no dependency you didn't ask for.

## The principles

- **Thin seams.** Each integration point is a minimal interface, not a base
  class or a plugin system. Implement the seam you need and ignore the rest.
- **No-op defaults.** Nothing is required to get started. Unconfigured
  telemetry, logging, and ID generation do nothing, quietly. You opt in to
  behavior, never out of it.
- **No forced dependencies.** No third-party type appears in a public
  signature. The `state` engine is the extreme end of this: stdlib-only, with no
  injected IO at all. The IO modules carry heavier seams via injection but
  follow the same rule.
- **Value semantics.** Context flows by value and transitions return the next
  state rather than mutating in place, so a machine is data you can snapshot,
  diff, serialize, and replay.
- **Composable, not coupled.** Each module is independently versioned
  (per-module SemVer) and usable on its own. The suite composes through optional
  bridges, never through a shared core you are forced to depend on.

## The suite

Two pillars are documented here in depth:

- **[state](/crucible/start/introduction/)** — the statechart engine. Author
  hierarchical, parallel, guarded machines, cast them to running instances,
  serialize them losslessly to JSON, and analyze and verify them before they
  ship. Pure stdlib.
- **[sink](/crucible/sink/overview/)** — the egress seam. Fan one event out to
  many destinations (databases, queues, webhooks, metrics) through a typed
  manifold, with buffering, telemetry, and graceful drain.

Further modules — durable execution, clustering, transport, WebAssembly, and
telemetry adapters — follow the same seam-and-default discipline and are
versioned independently. The full module list lives in the
[repository](https://github.com/stablekernel/crucible#modules).

## Where to start

- New to statecharts? Begin with the
  [state machine introduction](/crucible/start/introduction/).
- Routing events outward? See the [sink overview](/crucible/sink/overview/).
- Want the whole thing as one runnable program? Read the
  [food-delivery example](/crucible/examples/overview/).
