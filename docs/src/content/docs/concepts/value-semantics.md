---
title: Value semantics
description: Why the context flows by value, how AssignFn returns the next context, and the pointer escape hatch.
sidebar:
  order: 3
---

The context type `C` flows through a Crucible machine **by value**. Guards and
actions receive a copy of the context; they cannot mutate the instance. The only
place the context changes is an **assign reducer**, and a reducer does not mutate
in place. It returns the *next* context:

```go
type AssignFn[C any] func(in AssignCtx[C]) C
```

```go
reg.Assign("recordPayment", func(a state.AssignCtx[Order]) Order {
    a.Entity.Paid = true            // a.Entity is a copy
    a.Entity.PaidAt = clockNow(a)   // mutate the copy freely
    return a.Entity                 // the returned value becomes the next context
})
```

The reducer reads the prior context (`a.Entity`, by value), folds in event data
(`a.Event`) and static params (`a.Params`), and yields the new value. The kernel
makes that return value the instance's context at the end of the commit. No
shared mutable state is ever touched.

## Why value semantics

Treating context as an immutable value, replaced and never edited, is what gives
`Fire` its guarantees:

- **Snapshots.** The instance's state is fully captured by its current
  `(state, context)` pair; copy it and you have a snapshot.
- **Deterministic replay.** Re-running the same events over the same starting
  value reproduces the same results, every time.
- **Durable execution.** A snapshot can be persisted, reloaded, and resumed
  because nothing lives in hidden pointers or background goroutines.
- **Verification.** `Assay` can check an entity's legality against a state
  because the entity *is* the value the guards see.

## The pointer escape hatch

A pointer `C` (for example `*Order`) compiles and runs, but it forfeits all of
the above. With a pointer, a reducer can mutate shared state out from under
snapshots and replay, and the determinism guarantees no longer hold. Reach for a
pointer only as a deliberate escape hatch (e.g. an entity too large to copy on a
hot path) and only when you are not relying on snapshotting, replay, or durable
execution.

If you are integrating with a pointer-heavy or mutation-heavy codebase, the
preferred pattern is **value-projection at the edge**: keep a small value type as
your context, and project to and from your mutable domain objects only at the
boundary. A dedicated guide on this pattern lives under
[Integrating](/crucible/integrating/overview/).
