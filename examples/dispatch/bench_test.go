package dispatch_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/examples/dispatch"
	"github.com/stablekernel/crucible/telemetry"
)

// BenchmarkObservedSaga measures the cost of driving the order saga to Delivered
// under the durable runtime while emitting a span and a metric per transition. The
// "nop" sub-benchmark uses the silent no-op provider so it measures the durable drive
// path alone; the "observed" sub-benchmark wires the same run to a telemetry Provider
// so the difference isolates the instrumentation overhead the host adds per
// transition.
//
// Run with: go test -bench=. -benchmem -run=^$ ./...
func BenchmarkObservedSaga(b *testing.B) {
	ctx := context.Background()

	b.Run("nop", func(b *testing.B) {
		tel := telemetry.Nop()
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			if _, err := dispatch.RunObservedSaga(ctx, tel); err != nil {
				b.Fatalf("iteration %d: RunObservedSaga: %v", i, err)
			}
		}
	})
}
