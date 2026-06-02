---
title: Symbolic guard analysis
description: Reason over the guard tree itself — provably-disjoint transitions, dead branches, contradictory conditions.
sidebar:
  order: 4
---

Plain [static analysis](/crucible/analysis/static-analysis/) treats every guard as opaque, so it won't report an overlap between two *guarded* transitions on the same event. `state/verify/symbolic` closes that gap by reasoning over the Core guard tree as data — comparison and boolean structure over context fields — instead of running it.

Four questions over a guard node and the machine's `ContextSchema`:

```go
g := state.And(
    f("status").Eq(state.Str[string]("paid")),
    f("total").Gt(state.Float[string](10)),
)

symbolic.Satisfiable(g, schema)   // is there any context that makes g true?
symbolic.Contradiction(g, schema) // is g unsatisfiable — a dead branch?
symbolic.Disjoint(a, b, schema)   // can a and b never both be true at once?
```

`Disjoint` is the load-bearing one. Two competing transitions on the same event are safe — provably deterministic — exactly when their guards are disjoint. `Contradiction` finds guards that can never fire: a dead branch you can delete.

To sweep a whole machine, `Overlaps` walks every same-event transition pair and reports the ones that are *not* provably disjoint:

```go
overlaps, err := symbolic.Overlaps(m)
for _, o := range overlaps {
    fmt.Printf("ambiguous on %v from %v\n", o.Event, o.Source)
}
```

The analysis is **conservative**. It reasons precisely over the Core guard forms — `Eq`, `Ne`, `Lt`/`Le`/`Gt`/`Ge`, `In`, `And`/`Or`/`Not` over typed fields. Anything it can't see through — an opaque host `Guard("ext")`, a Rich predicate, a CEL expression, a `StateIn` check — it treats as "unknown" and assumes satisfiable. So a clean `Overlaps` report over Core guards is a real guarantee of determinism; a flagged pair over opaque guards is a "can't prove it, look closer," never a false alarm of safety.

<!-- IMAGE-SLOT: disjoint-guards — two molten transition arcs leaving one node, a sky-squid holding up a glowing proof token that the two guard-conditions can never overlap; a third grayed arc marked "unknown" — 3:2 -->
![Provably-disjoint guards](../../../assets/placeholders/hero.svg)
