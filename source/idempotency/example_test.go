// SPDX-License-Identifier: Apache-2.0

package idempotency_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/idempotency"
)

// setStore is a tiny in-memory idempotency.Store backed by a set.
type setStore struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (s *setStore) Seen(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[key] {
		return true, nil
	}
	s.seen[key] = true
	return false, nil
}

func ExampleMiddleware() {
	store := &setStore{seen: map[string]bool{}}

	processed := 0
	base := func(_ context.Context, m source.Message) source.Result {
		processed++
		fmt.Printf("processed %s\n", m.Key())
		return source.Ack()
	}
	h := idempotency.Middleware(idempotency.WithStore(store))(base)

	// The same message delivered twice runs the handler only once; the duplicate
	// is acked and dropped.
	msg := stubMsg{key: []byte("order-1")}
	r1 := h(context.Background(), msg)
	r2 := h(context.Background(), msg)

	fmt.Printf("first: %s/%s\n", r1.Action, r1.Class)
	fmt.Printf("second: %s/%s\n", r2.Action, r2.Class)
	fmt.Printf("handler runs: %d\n", processed)
	// Output:
	// processed order-1
	// first: ack/unclassified
	// second: ack/drop
	// handler runs: 1
}
