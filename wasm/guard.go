package wasm

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// guardRequest is the JSON envelope sent to a guard module's eval: the read-only
// context the guard evaluates against.
type guardRequest struct {
	Context any `json:"context"`
}

// guardResponse is the JSON envelope a guard module returns: the predicate verdict.
type guardResponse struct {
	OK bool `json:"ok"`
}

// wasmGuard is a state.GuardBinding whose verdict is computed by a WebAssembly
// module over the JSON ABI. EvalGuard is synchronous, so the kernel calls it inside
// the pure Fire step exactly like a Go-func guard; a transport-level failure or a
// malformed response surfaces as an error, which the kernel treats as a
// non-transitioning false.
type wasmGuard[C any] struct {
	mod *Module
}

func (g wasmGuard[C]) EvalGuard(ctx context.Context, req state.GuardRequest[C]) (state.GuardResult, error) {
	reqJSON, err := json.Marshal(guardRequest{Context: req.Context.Raw()})
	if err != nil {
		return state.GuardResult{}, fmt.Errorf("wasm guard %q: marshal request: %w", req.Name, err)
	}
	respJSON, err := g.mod.Eval(ctx, reqJSON)
	if err != nil {
		return state.GuardResult{}, fmt.Errorf("wasm guard %q: %w", req.Name, err)
	}
	var resp guardResponse
	if err := json.Unmarshal(respJSON, &resp); err != nil {
		return state.GuardResult{}, fmt.Errorf("wasm guard %q: decode response: %w", req.Name, err)
	}
	return state.GuardResult{OK: resp.OK}, nil
}

// Guard registers a WebAssembly-backed guard under name in reg and returns the rich
// IR node that references it. The module is already compiled (Compile); Guard only
// binds it, so a JSON-authored machine that Provides reg resolves the guard to the
// WASM evaluator. The returned node composes like any named-ref guard: drop it into
// a transition with WhenExpr, or combine it with And/Or/Not. It is tagged rich so
// analysis knows its truth lives in the module rather than the kernel's tree.
func Guard[S comparable, C any](reg *state.Registry[C], name string, mod *Module) state.GuardNode[S] {
	reg.BindGuard(name, wasmGuard[C]{mod: mod})
	node := state.Guard[S](name)
	node.Kind = state.GuardKindRich
	return node
}
