---
title: The crucible CLI
description: A headless command-line tool over a machine's serialized IR — lint, render, diff, validate, and eject, without running any behavior.
sidebar:
  order: 1
---

`crucible` is a headless command-line tool over a machine's intermediate
representation (IR). It lints, renders, diffs, validates, and ejects a serialized
IR JSON document without running a single transition. It is the tooling face of
the [serialization split](../../serialization/overview/): structure is data, so a
tool can reason about a machine's shape from the IR alone.

Every command reads an IR JSON file path, or `-` to read from stdin. So a
machine written in Go becomes a CI gate: emit its IR with `ToJSON`, write it to a
file, and hand that file to the CLI.

```go
m, err := state.ForgeFor[OrderContext]("order").
    // ... states and transitions ...
    Quench()
if err != nil {
    return err
}

ir, err := m.ToJSON()
if err != nil {
    return err
}
return os.WriteFile("order.json", ir, 0o644)
```

## Install

The CLI lives in the `cmd/crucible` module, versioned independently of `state`.
Build it from source with the Go toolchain:

```sh
go install github.com/stablekernel/crucible/cmd/crucible@latest
```

Or build a local binary from a checkout:

```sh
go build -o crucible ./cmd/crucible
```

## Behavior-free operation

A serialized IR carries behavior references by name only; the kernel binds those
names to real implementations when it assembles a machine. The commands that need
an assembled machine (`lint`, `render`, `validate`) do not have the host's real
guards, actions, reducers, and services. They register a deterministic no-op stub
for every referenced name first, so the machine assembles from its structure
alone. The stubs never run — no instance is cast and no event is fired — so the
structural view is exactly what the IR describes.

## Commands

### lint

```sh
crucible lint order.json
```

Runs every [static analysis](../../analysis/overview/) check and prints the
findings. Exits non-zero when the analysis reports any finding, so it gates CI.

### render

```sh
crucible render order.json -format mermaid
crucible render order.json -format dot | dot -Tsvg -o order.svg
```

Renders the machine as a Mermaid `stateDiagram-v2` (the default) or as Graphviz
DOT. `-format` selects between `mermaid` and `dot`; the flag may appear after the
IR path. Output is text — pipe the DOT through Graphviz for an SVG.

### diff

```sh
crucible diff order-v1.json order-v2.json
```

Classifies the changes between two serialized IRs, prints the recommended semver
bump (`major`, `minor`, or `patch`), and lists the breaking and additive changes
separately. This is the [evolution](../../analysis/evolution/) check on the
command line: treat a machine definition as a schema and let the diff decide the
bump.

### validate

```sh
crucible validate order.json
```

Confirms the IR loads and assembles cleanly. A malformed JSON document or a
structural defect exits non-zero with a message on stderr; a clean machine prints
`ok: <name>` and exits zero. It is the well-formedness gate.

### eject

```sh
crucible eject order.json -package order -o behaviors.go
```

Generates typed Go behavior stubs for every referenced guard, action, reducer,
and service, plus a `Provide` function that registers them against a
`state.Registry`. `-package` sets the generated package name (default `machine`),
and `-o` writes to a file (default stdout). See [Eject](../eject/) for what the
generated file contains and how to fill it in.

### version

```sh
crucible version
crucible -version
```

Prints the CLI version.

## Exit codes

- `0` success
- `1` runtime or load error, and lint findings
- `2` usage error

A non-zero `lint` or `diff` makes the CLI a drop-in CI gate: a failing analysis
or an unexpected breaking change fails the build.
