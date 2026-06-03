# crucible/wasm

Run [Crucible](../README.md) [`state`](../state) **behaviors as WebAssembly** —
polyglot guards (and, ahead, services) authored in any language and evaluated by
the host over a serializable JSON ABI.

> **Status:** experimental, pre-1.0. The runtime is feature-complete for guards and
> tested against a real compiled guest; the API may still change before v1.

Import path: `github.com/stablekernel/crucible/wasm`

## What it is

A guard is normally a Go func or a CEL expression. `wasm` adds a third option: a
guard implemented as a **WebAssembly module**, so behavior logic can be written in
any language that compiles to WASM and dropped into a machine by name. The host
invokes the module over a small JSON ABI through **wazero — a pure-Go, CGo-free
runtime** — so adopting it adds no C toolchain and no cross-compilation burden.

It lives apart from the kernel so the wazero dependency never enters the stdlib-only
core: a deployment that uses only Go or CEL guards never compiles WASM in. The ABI
is **core WebAssembly**, not the Component Model (which would require a CGo runtime).

## Quick start

```go
mod, err := wasm.Compile(ctx, moduleBytes) // instantiate once; reuse across calls
if err != nil {
	return err
}
defer mod.Close(ctx)

reg := state.NewRegistry[Order]()
guard := wasm.Guard[string](reg, "approved", mod) // a WASM-backed state.GuardBinding

def := state.Forge[string, string, Order]("order").
	Guard("approved", func(state.GuardCtx[Order]) bool { return false }). // stub; Provide overwrites
	State("pending").
	Transition("pending").On("submit").GoTo("submitted").WhenExpr(guard).
	State("submitted").
	Initial("pending").
	Quench()
// ... ToJSON → LoadFromJSON → Provide(reg) → Quench: the guard now evaluates in WASM.
```

The guard composes like any other: combine it with `And`/`Or`/`Not`, or reference it
by name from a JSON-authored machine. A broken module is **fail-safe** — an
evaluation error reports `false`, so the guarded transition is blocked rather than
taken on a bad verdict.

## The JSON ABI

A guest module exports two functions over its linear memory:

| Export | Signature | Purpose |
| --- | --- | --- |
| `alloc` | `(size u32) u32` | reserve `size` bytes, return the pointer the host writes the request into |
| `eval`  | `(ptr u32, size u32) u64` | read the JSON request at `[ptr, ptr+size)`, evaluate, write the JSON response, return packed `(outPtr<<32 \| outLen)` |

For a guard the request is `{"context": <ctx-json>}` and the response `{"ok": <bool>}`.
Because the payloads are JSON, the same module works for any host language. A `Module`
serializes concurrent `Eval` calls behind a mutex (one linear memory per instance).

A guest can be written in any WASM-targeting language; the test suite compiles a tiny
Go `//go:wasmexport` guest with the standard toolchain (`GOOS=wasip1 GOARCH=wasm`,
`-buildmode=c-shared`) — no TinyGo and no committed binary.

## Performance

Indicative per-call overhead (Apple Silicon dev machine, `go test -bench`); reproduce
with `go test -bench=. -benchmem -run=^$ ./...`. Each `Eval` marshals the request,
crosses into the guest, and reads the response back; the cost is dominated by the
JSON round-trip across the linear-memory boundary, so a WASM guard is heavier than an
in-process Go or CEL guard and is best reserved for genuinely polyglot logic.

## Stability

Stability label: **experimental** (pre-1.0; the API may change). Each module is
independently versioned per-module SemVer.

## Design & docs

Design rationale and guides live on the
[documentation site](https://stablekernel.github.io/crucible/). For questions or
proposals, open a GitHub issue.

## License

Apache-2.0. See the repository [LICENSE](../LICENSE) and [NOTICE](../NOTICE).
