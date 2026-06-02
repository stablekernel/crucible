// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"sync"
)

// Bucket is an in-memory Outlet for tests: it records every payload it receives
// and never errors. Pair it with All or RecordsOf to assert on what a Manifold
// fanned out. It is safe for concurrent use.
type Bucket struct {
	mu      sync.Mutex
	records []any
}

// NewBucket returns an empty Bucket.
func NewBucket() *Bucket { return &Bucket{} }

// Sink records payload and returns nil.
func (b *Bucket) Sink(_ context.Context, payload any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.records = append(b.records, payload)
	return nil
}

// All returns a copy of the recorded payloads in arrival order.
func (b *Bucket) All() []any {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]any(nil), b.records...)
}

// Reset clears the recorded payloads.
func (b *Bucket) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.records = nil
}

// RecordsOf returns the recorded payloads whose concrete type is T, in arrival
// order. It is a free function because Go does not permit type parameters on
// methods.
func RecordsOf[T any](b *Bucket) []T {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]T, 0, len(b.records))
	for _, r := range b.records {
		if v, ok := r.(T); ok {
			out = append(out, v)
		}
	}
	return out
}

var _ Outlet = (*Bucket)(nil)
