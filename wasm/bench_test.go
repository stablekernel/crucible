package wasm_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/wasm"
)

// BenchmarkModule_Eval measures the per-call cost of a WASM guard evaluation: the
// JSON request marshaled into guest memory, the guest call, and the JSON response
// read back. The compiled module is reused across iterations, so the benchmark
// isolates the ABI round-trip, not instantiation.
func BenchmarkModule_Eval(b *testing.B) {
	ctx := context.Background()
	mod, err := wasm.Compile(ctx, guardWASM)
	if err != nil {
		b.Fatalf("compile: %v", err)
	}
	b.Cleanup(func() { _ = mod.Close(ctx) })

	req := []byte(`{"context":{"status":"paid","total":50}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := mod.Eval(ctx, req); err != nil {
			b.Fatalf("eval: %v", err)
		}
	}
}
