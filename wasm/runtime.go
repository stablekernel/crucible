// Package wasm runs Crucible behaviors authored as WebAssembly modules, invoked
// over a serializable JSON ABI through the pure-Go wazero runtime (no CGo). It is
// the polyglot binding path: a guard (and later a service) can be implemented in
// any language that compiles to WebAssembly, and the host calls it with a JSON
// request and reads a JSON response.
//
// The module lives apart from the kernel so the wazero dependency never enters the
// stdlib-only core. The ABI is core WebAssembly plus two exported functions —
// alloc and eval — over the guest's linear memory; it is not the WebAssembly
// Component Model (which would require a CGo runtime), trading that for a pure-Go
// host.
package wasm

import (
	"context"
	"fmt"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// Module is a compiled and instantiated WebAssembly behavior module exposing the
// Crucible JSON ABI. A guest exports:
//
//	alloc(size uint32) uint32              — reserve size bytes, return the pointer
//	eval(ptr uint32, size uint32) uint64   — evaluate the JSON request at [ptr,ptr+size),
//	                                          return packed (outPtr<<32 | outLen)
//
// One Module owns one linear memory, so Eval serializes concurrent calls behind a
// mutex. Close releases the runtime.
type Module struct {
	runtime wazero.Runtime
	mod     api.Module
	alloc   api.Function
	eval    api.Function

	mu sync.Mutex
}

// CompileOption configures Compile. New capabilities arrive as additional options,
// so the signature never breaks. No option changes the default behavior; each is
// additive.
type CompileOption func(*compileConfig)

// compileConfig holds resolved CompileOption state for one Compile.
type compileConfig struct {
	runtimeConfig wazero.RuntimeConfig
}

// WithRuntimeConfig overrides the wazero RuntimeConfig the module is built with,
// for hosts that need to tune compilation (interpreter vs compiler), memory
// limits, or other wazero knobs. The default already closes a running guest when
// the call context is done, so a caller that supplies its own config should retain
// WithCloseOnContextDone(true) to keep timeout/cancellation working.
func WithRuntimeConfig(cfg wazero.RuntimeConfig) CompileOption {
	return func(c *compileConfig) {
		if cfg != nil {
			c.runtimeConfig = cfg
		}
	}
}

// Compile instantiates a behavior module from its WebAssembly bytes and resolves
// its ABI exports. The bytes are a wasip1 module (a Go //go:wasmexport guest, or any
// language's equivalent); WASI preview 1 is provided for the Go runtime's needs.
//
// The runtime is built with WithCloseOnContextDone(true), so a guest that runs away
// (an infinite loop, a pathological input) is interrupted when the context passed to
// Eval / Compile is canceled or hits its deadline: the call returns an error rather
// than blocking the host indefinitely. Pass a per-call timeout context to bound a
// guest's execution.
func Compile(ctx context.Context, wasmBytes []byte, opts ...CompileOption) (*Module, error) {
	cfg := compileConfig{
		// Interrupt a running guest when the call context is done, so a runaway guest
		// cannot block the host forever; a caller bounds execution with a timeout or
		// cancelable context.
		runtimeConfig: wazero.NewRuntimeConfig().WithCloseOnContextDone(true),
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	rt := wazero.NewRuntimeWithConfig(ctx, cfg.runtimeConfig)
	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	// Instantiate as a reactor: run the module's _initialize (if present) but not
	// _start, so the guest's exported functions stay callable rather than the Go
	// runtime exiting when main returns.
	mod, err := rt.InstantiateWithConfig(ctx, wasmBytes,
		wazero.NewModuleConfig().WithStartFunctions("_initialize"))
	if err != nil {
		_ = rt.Close(ctx)
		return nil, fmt.Errorf("wasm: instantiate: %w", err)
	}

	alloc := mod.ExportedFunction("alloc")
	eval := mod.ExportedFunction("eval")
	if alloc == nil || eval == nil {
		_ = rt.Close(ctx)
		return nil, fmt.Errorf("wasm: module is missing the alloc/eval ABI exports")
	}
	return &Module{runtime: rt, mod: mod, alloc: alloc, eval: eval}, nil
}

// Eval sends the JSON request to the guest and returns its JSON response. It
// allocates input space in the guest, writes the request there, calls eval, and
// reads the response back from the returned pointer. Calls are serialized.
func (m *Module) Eval(ctx context.Context, request []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	allocRes, err := m.alloc.Call(ctx, uint64(len(request)))
	if err != nil {
		return nil, fmt.Errorf("wasm: alloc: %w", err)
	}
	// A conforming alloc returns one result (the pointer). A misbehaving guest may
	// return none; index defensively so a malformed guest fails the call rather than
	// panicking the host.
	if len(allocRes) == 0 {
		return nil, fmt.Errorf("wasm: alloc returned no result")
	}
	inPtr := uint32(allocRes[0])
	if !m.mod.Memory().Write(inPtr, request) {
		return nil, fmt.Errorf("wasm: writing %d-byte request at %d is out of range", len(request), inPtr)
	}

	evalRes, err := m.eval.Call(ctx, uint64(inPtr), uint64(len(request)))
	if err != nil {
		return nil, fmt.Errorf("wasm: eval: %w", err)
	}
	// A conforming eval returns one packed result. A misbehaving guest may return
	// none; guard so a malformed guest fails the call rather than panicking the host.
	if len(evalRes) == 0 {
		return nil, fmt.Errorf("wasm: eval returned no result")
	}
	packed := evalRes[0]
	outPtr, outLen := uint32(packed>>32), uint32(packed)
	out, ok := m.mod.Memory().Read(outPtr, outLen)
	if !ok {
		return nil, fmt.Errorf("wasm: reading %d-byte response at %d is out of range", outLen, outPtr)
	}
	// Memory().Read returns a view into linear memory; copy it out before the next
	// call reuses the buffer.
	resp := make([]byte, len(out))
	copy(resp, out)
	return resp, nil
}

// Close releases the underlying runtime and its instances.
func (m *Module) Close(ctx context.Context) error {
	return m.runtime.Close(ctx)
}
