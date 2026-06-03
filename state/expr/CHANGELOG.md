# Changelog

All notable changes to `crucible/state/expr` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
This is the opt-in rich-expression tier; the kernel (the `state` module) stays
dependency-free, and `expr` depends on `state`, never the reverse.

## [0.1.0]

First release of the rich guard tier. It compiles guard logic written in CEL
(the Common Expression Language) against a machine's `ContextSchema`, type-checks
it at authoring time, and registers a CEL-backed guard binding the kernel
evaluates synchronously inside the pure `Fire` step.

### Added

- CEL-backed Rich guard tier. `Guard(...)` compiles guard source against a
  `state.ContextSchema`, returning a kernel-facing guard binding registered under
  a name plus a tooling-facing rich IR node. To the kernel the result is an
  ordinary named-ref guard leaf (tagged Kind `"rich"`): the kernel resolves it by
  name and evaluates it like any Go-func guard, never importing or seeing CEL.
  `WithCatalog` and `WithCostLimit` options control sidecar placement and the CEL
  evaluation cost budget.
- Determinism-stripped environment. The CEL environment is built from the
  standard library with no extension libraries and no host-declared functions, and
  the standard library carries no ambient or nondeterministic builtin (no `now`,
  no `random`), so a compiled guard is a pure function of its context, safe to
  evaluate inside `Fire` and to replay.
- Authoring-time type checking. A guard is parsed and *checked* against the
  `ContextSchema` before it is usable, so a type error (unknown field, mismatched
  comparison) surfaces at compile time rather than at `Fire` time.
- Core-to-CEL equivalence. `Lower` translates a Core `state.GuardNode` tree into an
  equivalent CEL program, and `EvalLowered` evaluates it; a conformance test
  asserts the Core in-kernel evaluator and the lowered CEL program agree, so the
  two tiers cannot drift on the shared fragment (boolean / compare / membership /
  `stateIn`).
- IR sidecar (`Catalog`). The type-checked CEL AST for each rich guard is stored
  in a name-keyed `Catalog` (`RichEntry` per name) that travels in the IR's
  machine-level `Meta`, so the evaluated guard and the persisted AST come from one
  compile and round-trip together. `Catalog.Meta()` writes the sidecar and
  `LoadCatalog` reads it back; `EvalCheckedAST` re-evaluates a stored AST directly.
  The same serialized CEL AST is the cross-stack contract a non-Go evaluator
  consumes.

[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/state/expr/v0.1.0
