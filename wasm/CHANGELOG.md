# Changelog

All notable changes to `crucible/wasm` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0]

The first release of the WebAssembly behavior runtime for the `crucible/state`
kernel. It runs polyglot guards as WASM modules over a JSON ABI through the pure-Go
wazero runtime, isolated to this module so the kernel and the rest of the suite stay
free of the dependency and of any CGo.

### Added

- **`Module`.** A compiled and instantiated WebAssembly behavior module exposing the
  Crucible JSON ABI (`alloc` / `eval`). `Compile` instantiates a wasip1 module through
  wazero (providing WASI preview 1) and resolves the ABI exports; `Eval` marshals a
  JSON request into guest memory, calls the guest, and reads the JSON response back,
  serializing concurrent calls behind a mutex; `Close` releases the runtime.
- **`Guard`.** Registers a WASM-backed `state.GuardBinding` under a name and returns
  the rich IR node referencing it, so a guard authored in WebAssembly gates a
  transition exactly like a Go or CEL guard, composes with `And`/`Or`/`Not`, and
  resolves by name from a JSON-authored machine. A module evaluation error is
  fail-safe: the guard reports false, blocking the transition.
- **Core-WASM JSON ABI.** `alloc(size)`/`eval(ptr,size)` over the guest's linear
  memory, with JSON request/response payloads, so a guest may be authored in any
  language that compiles to WebAssembly. Not the Component Model (which would require
  a CGo runtime); core WASM keeps the host pure-Go.

[0.1.0]: https://github.com/stablekernel/crucible/releases/tag/wasm/v0.1.0
