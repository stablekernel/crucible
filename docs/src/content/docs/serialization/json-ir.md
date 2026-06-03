---
title: JSON IR
description: Round-trip a machine to and from JSON losslessly, and rebind its named behavior.
sidebar:
  order: 2
---

The IR serializes to JSON and back without losing a thing. Two methods bracket
the round-trip, and a third rebinds behavior to the rehydrated definition.

## Emit

`ToJSON` serializes a machine's IR. It stamps the current schema version onto
the document so every emitted definition is self-describing.

```go
b, err := m.ToJSON()
if err != nil {
    return err
}
// b is canonical JSON: stable key ordering, deterministic for golden diffs.
```

## Load

`LoadFromJSON` rehydrates the IR. It is generic over the same state, event, and
context types the machine was forged with, and it returns an `*IR`: pure data,
not yet a runnable machine.

```go
ir, err := state.LoadFromJSON[Stage, Signal, Order](b)
if err != nil {
    return err // e.g. a document declaring a newer schema major is refused
}
```

A higher schema *major* is rejected rather than guessed at; a higher *minor* and
a pre-versioned document both load, and unknown keys are preserved verbatim so a
load-then-save cycle never drops fields a newer producer emitted.

## Rebind and quench

The IR carries behavior as named `Ref`s, never as code. `Provide` binds every
ref against a host registry and hands back a builder ready to `Quench`:

```go
m := ir.Provide(reg).Quench()
```

If a ref does not resolve, it surfaces at `Quench` as the same typed error the
fluent DSL raises for an unregistered binding: a JSON-authored machine and a
code-authored one fail identically.

## The Ref model

Every named hook in the IR is a `Ref`:

```go
type Ref struct {
    Name   string         `json:"name"`
    Params map[string]any `json:"params,omitempty"`
    Meta   map[string]any `json:"meta,omitempty"`
}
```

`Name` keys the registry. `Params` carry serializable configuration. `Meta` is a
reserved, round-tripped extension namespace the kernel never inspects. Reach for
the JSON IR whenever you need **persistence**, **interchange**, or **tooling**:
anywhere the definition must outlive or travel beyond the process that forged it.
See [the IR and the split](/crucible/concepts/ir-and-the-split/) for the model.
