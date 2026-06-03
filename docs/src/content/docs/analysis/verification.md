---
title: Verification
description: Prove reachability, safety, liveness, invariants, and coverage over a machine, each finding backed by a witness path.
sidebar:
  order: 3
---

<!-- IMAGE-SLOT: witness-path. A glowing statechart with one luminous traced route lit end-to-end while the rest dims, a foundry ledger annotating the event sequence beside it. 16:9 -->
![A property checked with a witness path](../../../assets/witness-path.png)

`state/verify` goes past structure to *properties*. You ask a question; it answers with a verdict and a **witness**: the event sequence that proves (or disproves) the claim, computed without ever firing an event.

One entry point, composed with options:

```go
result := verify.Verify(m,
    verify.Reachable("shipped"),
    verify.ReachAvoiding("shipped", "canceled"),
    verify.AlwaysEventually("delivered"),
    verify.CheckInvariant(verify.MutualExclusion("held", "paid")),
    verify.Coverage([]string{"pay", "ship"}),
)
```

The options map to the properties you care about:

- **`Reachable(states...)`**: can each named state be entered? Read with `result.For(name)`; the `Finding`'s `Witness.Events()` is the proof path. With no option, `Verify` checks every declared state and `result.Unreachable()` lists the orphans.
- **`ReachAvoiding(target, avoid...)`** is *safety*: reach `target` along a run that never touches the avoided states. Read with `result.ConditionalReach(target)`.
- **`AlwaysEventually(target)`** is *liveness*: from every reachable configuration, can `target` still be reached? On violation, `result.Liveness(target)` names the stuck configuration.
- **`CheckInvariant(MutualExclusion / Implies / NeverActive, ...)`** covers configuration invariants. `result.Invariant(label)` returns the counterexample configuration when one is reachable.
- **`Coverage(scenarios...)`**: which states and transitions a scenario set exercises against the reachable universe. `result.Coverage()` reports the gaps, a ready-made CI gate.
- **`SimulateBounded(label, depth, oracle)`** explores every trace to a depth bound and returns the shortest one a caller-supplied `Oracle` rejects.

A liveness failure, with its counterexample:

```go
result := verify.Verify(m, verify.AlwaysEventually("delivered"))
f, _ := result.Liveness("delivered")
fmt.Printf("always delivered: %t; stuck at %q via %v\n",
    f.Reachable, f.Witness.Target, f.Witness.Events())
// always delivered: false; stuck at "lost" via [misroute]
```

A witness is the difference between "this might be wrong" and "here is exactly how it goes wrong." Pair coverage with `verify.CoveringSuite(m)` to seed conformance tests from structure alone.
