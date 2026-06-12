// Package expr is crucible's opt-in rich expression tier. It compiles guard
// source written in CEL (the Common Expression Language) against a machine's
// ContextSchema, type-checks it at authoring time, and registers a CEL-backed
// guard binding the kernel evaluates synchronously inside the pure Fire step.
//
// The tier is deliberately a separate Go module so the kernel (the state module)
// stays dependency-free: state never imports CEL, expr depends on state. A guard
// authored here is, to the kernel, an ordinary named-ref guard leaf — the kernel
// resolves it by name and calls it like any Go-func guard, never seeing CEL.
//
// Two artifacts come out of one compile. The kernel-facing artifact is a guard
// binding registered under a name (so Fire can evaluate it). The tooling-facing
// artifact is a rich IR node — a named-ref leaf tagged Kind "rich" — plus the
// type-checked AST stored in a name-keyed sidecar (a Catalog) that travels in the
// IR's machine-level Meta. Both are produced from the same compiled program, so
// the evaluated guard and the stored AST can never drift.
//
// Determinism is a property of the environment: the env is built from the CEL
// standard library with no extension libraries and no host-declared functions, and
// the standard library contains no ambient or nondeterministic builtin (no now, no
// random) — so a compiled guard is a pure function of its context. This guarantee is
// enforced two ways: source that references an ambient or random builtin fails to
// compile (TestDeterminism_NondeterministicBuiltinsUnavailable), and the real env's
// function registry is introspected and asserted free of nondeterministic
// declarations (TestCELEnvDeterminism), so a future cel-go bump that silently adds a
// nondeterministic builtin fails the build rather than corrupting replay.
//
// # Guard eval-error asymmetry
//
// A guard can fail to evaluate (a runtime CEL error: a missing variable, a division
// by zero, an exceeded cost limit). This package's job is narrow and uniform: an
// eval failure is always returned as a typed, wrapped error from EvalGuard — the
// package never decides what that error means for the transition. The DISPOSITION of
// that error is the kernel's policy, not this package's, and it is deliberately
// asymmetric:
//
//   - Eventless (Always) guards fail CLOSED. An eval error on an eventless transition
//     is treated as a false verdict: the transition is simply not taken, and stepping
//     continues. An always-on edge that cannot be evaluated must not silently fire, so
//     the safe default is "did not match."
//   - Event-driven guards fail LOUD. An eval error on an event-driven transition
//     surfaces to the caller as a Fire error rather than being swallowed, because the
//     caller explicitly asked to fire a named event and is owed a definite answer —
//     either the transition fired, it was rejected, or evaluation failed.
//
// The kernel owns this disposition; the expr package only guarantees that an eval
// failure is reported consistently (a wrapped error, never a panic and never a silent
// true) so the kernel's policy has a reliable signal to act on.
//
// # Stability
//
// The expression language and its semantics — the CEL dialect accepted by Guard, the
// schema-to-env type mapping, the bool-result requirement, and the determinism
// guarantee (StdLib-only, no nondeterministic builtins) — are part of crucible's
// v1.0 stable contract. This package is load-bearing for the replay and guard
// surface: a recorded run must replay to the same verdicts, and a guard authored
// against a schema must compile and evaluate identically across versions, so the
// accepted language and the determinism property will not change incompatibly within
// v1. This is a stability commitment, not an advisory: callers may depend on the
// expression semantics and the no-nondeterminism guarantee as a fixed contract.
package expr
