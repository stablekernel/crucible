package durable_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// twoStateMachine returns a minimal two-state machine (idle → done) used
// across benchmarks to isolate durable overhead from machine complexity.
func twoStateMachine() *state.Machine[string, string, struct{}] {
	return state.Forge[string, string, struct{}]("bench").
		State("idle").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("done").
		Quench()
}

// BenchmarkHandle_Fire_DurableVsBare measures the per-step overhead the durable
// Runner adds over a bare kernel Instance.Fire. Both benchmarks drive the same
// two-state machine one step; the difference is the MemStore Append the durable
// path incurs.
//
// Run with: go test -bench=. -benchmem -run=^$ ./...
func BenchmarkHandle_Fire_DurableVsBare(b *testing.B) {
	ctx := context.Background()
	m := twoStateMachine()

	b.Run("bare", func(b *testing.B) {
		// Re-cast each iteration so the machine always sits in the idle state
		// that still accepts the event and the kernel never rejects it (it is a final
		// machine; post-final fires are no-ops, not panics, but the cost is not
		// representative).
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			inst := m.Cast(struct{}{}, state.WithInitialState[string]("idle"))
			inst.Fire(ctx, "go")
		}
	})

	b.Run("durable/memstore", func(b *testing.B) {
		// Each iteration starts a fresh instance so the event is always valid. A
		// MemStore is allocated once per sub-benchmark; Start persists the baseline
		// checkpoint and Fire appends a single Record — the minimal durable path.
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			store := durable.NewMemStore()
			id := durable.InstanceID("bench")
			runner := durable.NewRunner(m, store)
			h, err := runner.Start(ctx, id, struct{}{}, state.WithInitialState[string]("idle"))
			if err != nil {
				b.Fatalf("iteration %d: Start: %v", i, err)
			}
			if _, err := h.Fire(ctx, "go"); err != nil {
				b.Fatalf("iteration %d: Fire: %v", i, err)
			}
		}
	})
}

// BenchmarkMemStore_Append isolates the raw per-step MemStore.Append cost —
// the serialization and map bookkeeping the durable path incurs — independent
// of machine and Runner overhead.
func BenchmarkMemStore_Append(b *testing.B) {
	ctx := context.Background()
	store := durable.NewMemStore()
	const id = durable.InstanceID("append-bench")

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		_, err := store.Append(ctx, id, durable.Record{Step: i})
		if err != nil {
			b.Fatalf("step %d: %v", i, err)
		}
	}
}
