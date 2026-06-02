// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

type (
	payloadA struct{ N int }
	payloadB struct{ S string }
)

func TestRegistryLookupHitAndTransform(t *testing.T) {
	t.Parallel()

	r := sink.NewRegistry[string]()
	sink.Register(r, func(_ context.Context, p payloadA) string { return "A" })

	fn, ok := r.Lookup(payloadA{N: 1})
	if !ok {
		t.Fatalf("Lookup(payloadA) ok = false, want true")
	}
	if got := fn(context.Background(), payloadA{N: 1}); got != "A" {
		t.Errorf("transform = %q, want %q", got, "A")
	}
}

func TestRegistryLookupMiss(t *testing.T) {
	t.Parallel()

	r := sink.NewRegistry[string]()
	sink.Register(r, func(_ context.Context, p payloadA) string { return "A" })

	if _, ok := r.Lookup(payloadB{S: "x"}); ok {
		t.Fatalf("Lookup(payloadB) ok = true, want false")
	}
}

func TestRegistriesDoNotShareState(t *testing.T) {
	t.Parallel()

	r1 := sink.NewRegistry[string]()
	r2 := sink.NewRegistry[string]()
	sink.Register(r1, func(_ context.Context, p payloadA) string { return "r1" })

	if _, ok := r2.Lookup(payloadA{}); ok {
		t.Fatalf("r2 sees r1's registration; registries share state")
	}
}

func TestRegistryConcurrentRegisterAndLookup(t *testing.T) {
	t.Parallel()

	r := sink.NewRegistry[int]()
	sink.Register(r, func(_ context.Context, p payloadA) int { return p.N })

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); sink.Register(r, func(_ context.Context, p payloadB) int { return len(p.S) }) }()
		go func() { defer wg.Done(); r.Lookup(payloadA{N: 1}) }()
	}
	wg.Wait()
}
