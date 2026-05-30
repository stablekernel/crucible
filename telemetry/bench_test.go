package telemetry_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stablekernel/crucible/telemetry"
)

// recordingMeter is a minimal real (non-no-op) Counter implementation used by
// the allocation benchmarks. Unlike the no-op meter — which discards its
// arguments and so would let the compiler elide the work being measured — this
// impl actually consumes each attribute by reading its slog.Value through the
// typed accessors. That keeps the attributes live across the call, so the
// benchmark measures the true cost of constructing and passing them. The sink
// fields are written so escape analysis cannot prove the values dead.
type recordingCounter struct {
	sum     int64
	lastS   string
	lastF   float64
	lastB   bool
	lastID  int64
	lastAny any
}

func (c *recordingCounter) Add(_ context.Context, n int64, attrs ...telemetry.Attr) {
	c.sum += n
	for _, a := range attrs {
		switch a.Value.Kind() {
		case slog.KindString:
			c.lastS = a.Value.String()
		case slog.KindInt64:
			c.lastID = a.Value.Int64()
		case slog.KindFloat64:
			c.lastF = a.Value.Float64()
		case slog.KindBool:
			c.lastB = a.Value.Bool()
		default:
			// Any/other kinds: retain the boxed value. Storing it in a field that
			// outlives the call forces the interface box to the heap, so the
			// boxing cost the scalar path avoids becomes visible in the benchmark.
			c.lastAny = a.Value.Any()
		}
	}
}

// sink is an escaping global the benchmark writes through so escape analysis
// cannot prove the recorded values dead and elide the work being measured.
var sink *recordingCounter

// BenchmarkScalarAttrs proves that constructing and passing scalar attributes
// (String, Int64, Float64, Bool) through a recording path is zero-allocation:
// slog.Value stores scalars inline, so no interface box is created.
func BenchmarkScalarAttrs(b *testing.B) {
	c := &recordingCounter{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Add(ctx, 1,
			telemetry.String("outlet", "dynamo"),
			telemetry.Int64("size", 100),
			telemetry.Float64("latency_ms", 3.2),
			telemetry.Bool("retried", true),
		)
	}
	sink = c
}

// BenchmarkAnyAttr documents the contrast: Any is the opt-in boxing escape
// hatch, so passing an arbitrary value through the same path allocates. Reported
// here purely so the zero-alloc scalar result above is read against a baseline
// that is known to allocate.
func BenchmarkAnyAttr(b *testing.B) {
	c := &recordingCounter{}
	ctx := context.Background()
	// A multi-word struct: boxing it into an interface must copy the value to the
	// heap (it does not fit in an interface's single data word), so the boxing
	// cost the scalar path avoids is unavoidable here. The counter retains the
	// boxed value, defeating any escape-analysis elision.
	type payload struct {
		ID   int64
		Name string
		Tags [4]int64
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Vary the value per iteration so the boxed interface cannot be hoisted
		// out of the loop; each Any genuinely boxes.
		v := payload{ID: int64(i), Name: "order", Tags: [4]int64{1, 2, 3, 4}}
		c.Add(ctx, 1, telemetry.Any("payload", v))
	}
	sink = c
}
