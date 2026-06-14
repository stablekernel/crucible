---
title: Eject codegen
description: Turn a machine's IR into typed Go stubs and a Provide wiring function — proven against the registry before a line of behavior is written.
sidebar:
  order: 2
---

The `gen` module turns a machine's IR into typed Go source: one stub per
referenced behavior, plus a `Provide` function that wires them into a registry.
It is the scaffolding step between designing a machine's *shape* and implementing
its *behavior*. The [`crucible eject`](../cli/#eject) command is the CLI front-end
over this module.

## What it generates

`gen.Eject` walks a [serialized IR](../../serialization/overview/) and emits a
single gofmt'd Go source file containing:

- a generated `Context` type synthesized from the IR's context schema — a struct
  (one field per schema field, with Go-typed fields and `json` tags) when the
  schema declares fields, or a `map[string]any` alias when the schema is absent
  or empty;
- one panic-bodied stub per referenced guard, action, assign, and service, each
  typed to the exact engine signature with the generated `Context` substituted
  for the machine's context type parameter; and
- a `Provide` function that registers every stub against a `state.Registry` by
  its original IR name.

Each stub panics with a TODO until it is implemented, but the file *compiles* and
its `Provide` type-checks against the real registry. The wiring is proven before
any behavior is written: the IR says which behaviors a machine needs, and the
generated file is the typed skeleton a host fills in.

Output is deterministic. Behavior names are walked across the full state
hierarchy (states, children, regions, transitions, invocations), deduplicated,
and sorted, so ejecting the same IR twice yields byte-identical source. A name
shared across behavior kinds gets a unique, kind-suffixed Go identifier while its
registration string stays the original name.

## From the command line

The common path is the CLI, which reads an IR file (or stdin) and writes the
generated source:

```sh
crucible eject order.json -package order -o behaviors.go
```

`-package` sets the generated package clause (default `machine`); `-o` writes to
a file (default stdout). Implement each stub, then call `Provide` to register the
behaviors and assemble the machine.

## From Go

`gen.Eject` is the same codegen as a library call. It takes the loaded IR and an
additive tail of options, and returns the formatted source bytes:

```go
src, err := gen.Eject[string, string, OrderContext](ir,
    gen.WithPackageName("order"),
    gen.WithContextTypeName("Context"),
)
if err != nil {
    return err
}
return os.WriteFile("behaviors.go", src, 0o644)
```

The type parameters mirror the machine's `state.IR[S, E, C]`, so a typed
machine's loaded IR passes through without reflection or a wrapper. `WithPackageName`
sets the package clause (default `machine`) and `WithContextTypeName` sets the
generated context type name (default `Context`); both are part of the additive
option tail, so new knobs arrive without breaking the `Eject` signature.
