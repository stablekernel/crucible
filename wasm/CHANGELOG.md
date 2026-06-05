# Changelog

All notable changes to `crucible/wasm` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this module adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`CompileOption` / `WithRuntimeConfig`.** `Compile` takes a variadic
  `...CompileOption` so configuration arrives additively without breaking the
  signature. `WithRuntimeConfig` overrides the wazero RuntimeConfig the module is
  built with.

### Fixed

- **Runaway-guest timeout.** The runtime is built with
  `WithCloseOnContextDone(true)`, so a guest that never returns is interrupted when
  the `Eval` context is canceled or hits its deadline, returning an error instead of
  blocking the host. Pass a per-call timeout context for untrusted guests.
- **Defensive ABI result indexing.** `Eval` guards the `alloc` and `eval` result
  slices before indexing, so a misbehaving guest that returns no result fails the
  call with an error rather than panicking the host.

### Documentation

- **ABI / allocator note.** The README no longer implies a general-purpose
  allocator: `alloc` returns a writable region of at least `size` bytes, which the
  reference guest backs with a fixed buffer. Added a timeout/cancellation section.

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
