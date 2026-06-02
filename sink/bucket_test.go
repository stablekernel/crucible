// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

func TestBucketRecordsInOrder(t *testing.T) {
	t.Parallel()

	b := sink.NewBucket()
	for _, p := range []any{1, "two", 3.0} {
		if err := b.Sink(context.Background(), p); err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
	}
	got := b.All()
	if len(got) != 3 || got[0] != 1 || got[1] != "two" || got[2] != 3.0 {
		t.Fatalf("All() = %v, want [1 two 3]", got)
	}
}

func TestRecordsOfFiltersByType(t *testing.T) {
	t.Parallel()

	b := sink.NewBucket()
	_ = b.Sink(context.Background(), payloadA{N: 1})
	_ = b.Sink(context.Background(), payloadB{S: "x"})
	_ = b.Sink(context.Background(), payloadA{N: 2})

	as := sink.RecordsOf[payloadA](b)
	if len(as) != 2 || as[0].N != 1 || as[1].N != 2 {
		t.Fatalf("RecordsOf[payloadA] = %v, want [{1} {2}]", as)
	}
}

func TestBucketReset(t *testing.T) {
	t.Parallel()

	b := sink.NewBucket()
	_ = b.Sink(context.Background(), 1)
	b.Reset()
	if got := b.All(); len(got) != 0 {
		t.Fatalf("All() after Reset = %v, want empty", got)
	}
}

func TestBucketConcurrentSink(t *testing.T) {
	t.Parallel()

	b := sink.NewBucket()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = b.Sink(context.Background(), 1) }()
	}
	wg.Wait()
	if got := b.All(); len(got) != 100 {
		t.Fatalf("recorded %d, want 100", len(got))
	}
}
