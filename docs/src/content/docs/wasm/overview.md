---
title: What is crucible/wasm
description: Run state behaviors as WebAssembly; polyglot guards authored in any language and evaluated by the host over a small JSON ABI through a pure-Go, CGo-free runtime.
sidebar:
  order: 1
---

<!-- IMAGE-SLOT: wasm-overview-polyglot (a sky-squid smith feeding several differently-shaped ingots, each a language, into one mold that casts a single verdict; ember/copper on steel) 16:9 -->

`crucible/wasm` runs [`state`](/crucible/start/introduction/) **behaviors as
WebAssembly**: polyglot guards authored in any language that compiles to WASM and
evaluated by the host over a serializable JSON ABI.

A guard is normally a Go func or a CEL expression. wasm adds a third option, a
guard implemented as a WebAssembly module, so behavior logic can be written in any
WASM-targeting language and dropped into a machine by name. The host invokes the
module over a small JSON ABI through **wazero, a pure-Go, CGo-free runtime**, so
adopting it adds no C toolchain and no cross-compilation burden.

It lives apart from the kernel so the wazero dependency never enters the
stdlib-only core: a deployment that uses only Go or CEL guards never compiles WASM
in. The ABI is **core WebAssembly**, not the Component Model, which would require a
CGo runtime.

## The shape of it

Compile a module once and reuse it across calls, then bind it as a guard by name:

```go
mod, err := wasm.Compile(ctx, moduleBytes) // instantiate once; reuse across calls
defer mod.Close(ctx)

reg := state.NewRegistry[Order]()
guard := wasm.Guard[string](reg, "approved", mod) // a WASM-backed state.GuardBinding

def := state.ForgeFor[Order]("order").
	Guard("approved", func(state.GuardCtx[Order]) bool { return false }). // stub; Provide overwrites
	State("pending").
	Transition("pending").On("submit").GoTo("submitted").WhenExpr(guard).
	State("submitted").
	Initial("pending").
	Quench()
// ... ToJSON -> LoadFromJSON -> Provide(reg) -> Quench: the guard now evaluates in WASM.
```

The guard composes like any other: combine it with `And`/`Or`/`Not`, or reference
it by name from a JSON-authored machine. A broken module is **fail-safe**: an
evaluation error reports `false`, so the guarded transition is blocked rather than
taken on a bad verdict.

## The JSON ABI

A guest module exports two functions over its linear memory:

| Export | Signature | Purpose |
| --- | --- | --- |
| `alloc` | `(size u32) u32` | reserve `size` bytes, return the pointer the host writes the request into |
| `eval`  | `(ptr u32, size u32) u64` | read the JSON request at `[ptr, ptr+size)`, evaluate, write the JSON response, return packed `(outPtr<<32 \| outLen)` |

For a guard the request is `{"context": <ctx-json>}` and the response is
`{"ok": <bool>}`. Because the payloads are JSON, the same module works for any host
language. A `Module` serializes concurrent `Eval` calls behind a mutex (one linear
memory per instance).

A guest can be written in any WASM-targeting language; the test suite compiles a
tiny Go `//go:wasmexport` guest with the standard toolchain (`GOOS=wasip1
GOARCH=wasm`, `-buildmode=c-shared`), with no TinyGo and no committed binary.

## When to reach for it

Each `Eval` marshals the request, crosses into the guest, and reads the response
back, so the cost is dominated by the JSON round-trip across the linear-memory
boundary. A WASM guard is therefore heavier than an in-process Go or CEL guard and
is best reserved for genuinely polyglot logic: a rule you want to author once and
share with a non-Go service, or behavior shipped by a team that does not write Go.
The runtime is feature-complete for guards today; services are the next behavior to
land on the same ABI.
