// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"testing"
	"time"
)

func TestPollerCollectOnceDeliversAll(t *testing.T) {
	t.Parallel()

	bucket := NewBucket()
	p := NewPoller(bucket, func(_ context.Context, emit func(any)) {
		emit("a")
		emit("b")
	})
	p.collectOnce(context.Background())

	if got := bucket.All(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("collectOnce delivered %v, want [a b]", got)
	}
}

func TestPollerCollectOnceEmptyNoOp(t *testing.T) {
	t.Parallel()

	bucket := NewBucket()
	p := NewPoller(bucket, func(context.Context, func(any)) {})
	p.collectOnce(context.Background())
	if got := len(bucket.All()); got != 0 {
		t.Fatalf("empty collect delivered %d, want 0", got)
	}
}

func TestPollerCollectOnceUsesBatchOutlet(t *testing.T) {
	t.Parallel()

	bb := &batchBucket{}
	p := NewPoller(bb, func(_ context.Context, emit func(any)) { emit(1); emit(2) })
	p.collectOnce(context.Background())
	if len(bb.batches) != 1 || len(bb.batches[0]) != 2 {
		t.Fatalf("batches = %v, want one batch of 2", bb.batches)
	}
}

func TestPollerStartStopNoLeak(t *testing.T) {
	t.Parallel()

	bucket := NewBucket()
	p := NewPoller(bucket, func(_ context.Context, emit func(any)) { emit("x") },
		WithPollInterval(time.Millisecond))
	p.Start(context.Background()).Start(context.Background()) // idempotent
	p.Stop()
	p.Stop() // idempotent, must not block or panic
}

func TestPollerStopBeforeStartIsSafe(t *testing.T) {
	t.Parallel()
	NewPoller(NewBucket(), func(context.Context, func(any)) {}).Stop()
}
