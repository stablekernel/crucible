---
title: Analysis & verification
description: Why Crucible can reason about a machine statically, and the toolbox that does it.
sidebar:
  order: 1
---

<!-- IMAGE-SLOT: analysis-toolbox — a foundry inspection bench laying out five glowing instruments around a translucent statechart ingot, sky-squid examining it under a loupe; conveys "the machine is data you can measure" — 16:9 -->
![The analysis toolbox](../../../assets/analysis-toolbox.png)

Static reasoning is not a bolt-on to Crucible — it is a consequence of the design. The canonical machine is a serializable definition IR: pure data, lossless to and from JSON, with value semantics throughout. Because the machine *is* data — not a tangle of closures — you can read it, walk its graph, and prove things about it before a single event is ever fired.

That is the whole pitch of this section. The same definition that runs in a unit test, an HTTP handler, and an async consumer is also the artifact you analyze, verify, and diff. One source of truth, many uses.

The toolbox, from cheapest to most thorough:

- **`state/analysis`** — fast structural checks over the IR: unreachable states, dead transitions, nondeterminism, dead ends. Surfaced non-failingly at dev time through the builder's `Temper` pass, before you `Quench`.
- **`state/verify`** — property checks with witnesses: reachability, safety (`ReachAvoiding`), liveness (`AlwaysEventually`), configuration invariants, bounded simulation, and structural coverage.
- **`state/verify/symbolic`** — reasons over the guard tree itself: are two competing guards provably disjoint? Is a guard a contradiction — a dead branch?
- **`state/conformance`** — drive golden scenarios against a machine so its behavior is locked over time.
- **`state/evolution`** — diff two machine definitions and classify the SemVer impact: breaking versus additive.

Every check here is pure and deterministic. Nothing fires an event or touches IO. Start with [static analysis](/crucible/analysis/static-analysis/) for the cheapest wins, then reach for [verification](/crucible/analysis/verification/) when you need a proof you can read.
