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
// random) — so a compiled guard is a pure function of its context.
package expr
