// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

type fuzzPayload struct{ N int }

// FuzzRegistryDispatch checks the type-keyed dispatch round-trip: a transformer
// registered for a concrete payload type is found by Lookup and applied to
// yield the mapped value, while an unrelated type always misses. No registry
// state leaks across types.
func FuzzRegistryDispatch(f *testing.F) {
	f.Add(0)
	f.Add(42)
	f.Add(-1)

	f.Fuzz(func(t *testing.T, n int) {
		r := sink.NewRegistry[int]()
		sink.Register(r, func(_ context.Context, p fuzzPayload) int { return p.N })

		fn, ok := r.Lookup(fuzzPayload{N: n})
		if !ok {
			t.Fatalf("Lookup(fuzzPayload) missed for n=%d", n)
		}
		if got := fn(context.Background(), fuzzPayload{N: n}); got != n {
			t.Fatalf("dispatch round-trip = %d, want %d", got, n)
		}
		if _, ok := r.Lookup("a string is a different type"); ok {
			t.Fatal("Lookup hit for an unregistered type")
		}
	})
}

// FuzzReservoirConservation checks the batching invariant: across any count of
// payloads and any batch size, a final Flush delivers exactly the payloads sunk
// to the inner outlet, none lost and none duplicated.
func FuzzReservoirConservation(f *testing.F) {
	f.Add(uint8(0), uint8(1))
	f.Add(uint8(5), uint8(3))
	f.Add(uint8(100), uint8(10))
	f.Add(uint8(7), uint8(0)) // size 0 disables size-triggered flush

	f.Fuzz(func(t *testing.T, count, size uint8) {
		bucket := sink.NewBucket()
		// Interval 0 disables the background loop, so the test is deterministic
		// and sleep-free: flushes happen only at the batch size or on Flush.
		r := sink.Reservoir(bucket, sink.WithBatchSize(int(size)), sink.WithBatchInterval(0))

		for i := 0; i < int(count); i++ {
			if err := r.Sink(context.Background(), i); err != nil {
				t.Fatalf("Sink(%d) error = %v", i, err)
			}
		}
		if fl, ok := r.(sink.Flusher); ok {
			if err := fl.Flush(context.Background()); err != nil {
				t.Fatalf("Flush error = %v", err)
			}
		}

		got := bucket.All()
		if len(got) != int(count) {
			t.Fatalf("inner received %d payloads, want %d (no loss or duplication)", len(got), count)
		}
		for i, v := range got {
			if v != i {
				t.Fatalf("payload at %d = %v, want %d (order preserved, no duplication)", i, v, i)
			}
		}
	})
}
