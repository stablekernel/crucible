// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"sync"
	"testing"
	"time"
)

// stepClock returns a clock that advances by step on each call, for
// deterministic, sleep-free latency measurement.
func stepClock(step time.Duration) func() time.Time {
	var mu sync.Mutex
	t := time.Unix(0, 0)
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t = t.Add(step)
		return t
	}
}

type batchBucket struct {
	mu      sync.Mutex
	batches [][]any
	singles []any
}

func (b *batchBucket) Sink(_ context.Context, p any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.singles = append(b.singles, p)
	return nil
}

func (b *batchBucket) SinkBatch(_ context.Context, ps []any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.batches = append(b.batches, append([]any(nil), ps...))
	return nil
}

func TestReservoirSizeTriggeredFlush(t *testing.T) {
	t.Parallel()

	bucket := NewBucket()
	r := Reservoir(bucket, WithBatchSize(3), WithBatchInterval(0))
	for i := 0; i < 3; i++ {
		if err := r.Sink(context.Background(), i); err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
	}
	if got := len(bucket.All()); got != 3 {
		t.Fatalf("after reaching batch size, inner has %d, want 3", got)
	}
}

func TestReservoirManualFlushDrains(t *testing.T) {
	t.Parallel()

	bucket := NewBucket()
	r := Reservoir(bucket, WithBatchSize(100), WithBatchInterval(0)).(*reservoir)
	_ = r.Sink(context.Background(), "a")
	_ = r.Sink(context.Background(), "b")
	if got := len(bucket.All()); got != 0 {
		t.Fatalf("inner flushed early: %d", got)
	}
	if err := r.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if got := len(bucket.All()); got != 2 {
		t.Fatalf("after Flush inner has %d, want 2", got)
	}
}

func TestReservoirTickFlushesBuffer(t *testing.T) {
	t.Parallel()

	bucket := NewBucket()
	r := Reservoir(bucket, WithBatchSize(100), WithBatchInterval(0)).(*reservoir)
	_ = r.Sink(context.Background(), 1)
	r.tick(context.Background()) // deterministic interval-flush, no sleep
	if got := len(bucket.All()); got != 1 {
		t.Fatalf("after tick inner has %d, want 1", got)
	}
}

func TestReservoirUsesBatchOutlet(t *testing.T) {
	t.Parallel()

	bb := &batchBucket{}
	r := Reservoir(bb, WithBatchSize(2), WithBatchInterval(0))
	_ = r.Sink(context.Background(), "a")
	_ = r.Sink(context.Background(), "b")
	if len(bb.batches) != 1 || len(bb.batches[0]) != 2 {
		t.Fatalf("SinkBatch got batches=%v, want one batch of 2", bb.batches)
	}
	if len(bb.singles) != 0 {
		t.Fatalf("BatchOutlet path used per-item Sink: %v", bb.singles)
	}
}

func TestReservoirDropsOverCap(t *testing.T) {
	t.Parallel()

	fm := newFakeMeter()
	bucket := NewBucket()
	r := Reservoir(bucket, WithBatchSize(0), WithBatchInterval(0), WithMaxBuffered(2), WithReservoirMeter(fm))
	for i := 0; i < 5; i++ {
		_ = r.Sink(context.Background(), i)
	}
	if got := fm.counterValue("sink.dropped"); got != 3 {
		t.Fatalf("sink.dropped = %d, want 3", got)
	}
}

func TestReservoirRecordsBatchSizeAndLatency(t *testing.T) {
	t.Parallel()

	fm := newFakeMeter()
	bucket := NewBucket()
	r := Reservoir(bucket, WithBatchSize(0), WithBatchInterval(0),
		WithReservoirMeter(fm), WithReservoirClock(stepClock(7*time.Millisecond))).(*reservoir)
	_ = r.Sink(context.Background(), 1)
	_ = r.Sink(context.Background(), 2)
	if err := r.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if !fm.histogramObserved("sink.batch_size") {
		t.Error("sink.batch_size not recorded")
	}
	if !fm.histogramObserved("sink.flush_latency_ms") {
		t.Error("sink.flush_latency_ms not recorded")
	}
}

func TestReservoirShutdownStopsLoopNoLeak(t *testing.T) {
	t.Parallel()

	bucket := NewBucket()
	r := Reservoir(bucket, WithBatchSize(100), WithBatchInterval(time.Millisecond)).(*reservoir)
	_ = r.Sink(context.Background(), "x")
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	// Shutdown drains, so the payload reaches inner; a second Shutdown is a no-op.
	if got := len(bucket.All()); got != 1 {
		t.Fatalf("after Shutdown inner has %d, want 1 (drained)", got)
	}
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}
