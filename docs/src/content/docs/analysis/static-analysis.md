---
title: Static analysis
description: Cheap structural checks over the machine IR — dead states, nondeterminism, dead ends — surfaced before you Quench.
sidebar:
  order: 2
---

`state/analysis` reads the machine's transition graph and reports structural defects without firing anything. It is the cheapest check in the toolbox: pure graph reasoning over the IR.

`analysis.Analyze` returns a `Report` of `Finding`s. Each finding carries a `Kind`, a `Severity`, and the state (or transition) it concerns.

```go
report := analysis.Analyze(m)
for _, f := range report.Findings {
    fmt.Printf("%s [%s] %s\n", f.Severity, f.Kind, f.State)
}
// error   [unreachable_state] lost
// error   [dead_transition]   lost
// warning [dead_end]          stuck
// warning [cannot_reach_final] stuck
```

The kinds split into what the IR *proves* and what it *suggests*:

- **`unreachable_state`** / **`dead_transition`** — *exact*. Reachability ignores guards (a guard can only ever remove an edge at run time, never add one), so a statically unreachable state is unreachable in every run. Severity `error`.
- **`nondeterministic`** — *exact* for the guardless case: two or more guardless transitions on the same event, or competing guardless "always" transitions. Guarded overlaps are deferred to [symbolic analysis](/analysis/symbolic-guards/).
- **`dead_end`** / **`cannot_reach_final`** — *heuristic* `warning`s: a non-final state with no exit, or a state from which no final state is reachable. A guard that is always false at run time could make these real, but the IR can't decide that.

Scope a pass with `analysis.Only(kinds...)` or `analysis.Without(kinds...)`.

These same findings power the builder's **`Temper`** pass — an optional, non-failing diagnostics step you chain before `Quench`:

```go
for _, d := range builder.Temper() {
    log.Printf("%s: %s", d.Severity, d.Message)
}
machine := builder.Quench() // panics on errors Temper merely reported
```

`Temper` hands you the findings as data; `Quench` is the always-call finalizer that turns the same defects into a loud panic. Lint early, freeze with confidence.

<!-- IMAGE-SLOT: temper-pass — a foundry worker running a glowing statechart casting under a diagnostic scanner that lights orphaned nodes amber and severed edges red, before the quench tank — 16:9 -->
![The Temper diagnostics pass](../../../assets/placeholders/hero.svg)
