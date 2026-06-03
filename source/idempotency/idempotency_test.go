// SPDX-License-Identifier: Apache-2.0

package idempotency_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/idempotency"
)

// stubMsg is a minimal [source.Message] for middleware tests.
type stubMsg struct {
	key     []byte
	headers source.Headers
}

func (m stubMsg) Key() []byte             { return m.key }
func (m stubMsg) Value() []byte           { return nil }
func (m stubMsg) Headers() source.Headers { return m.headers }
func (m stubMsg) Subject() string         { return "s" }
func (m stubMsg) PartitionKey() string    { return "" }
func (m stubMsg) Cursor() source.Cursor   { return stubCursor{} }
func (m stubMsg) As(any) bool             { return false }

type stubCursor struct{}

func (stubCursor) String() string { return "stub" }

// memStore is a concurrency-safe in-memory [idempotency.Store].
type memStore struct {
	mu   sync.Mutex
	seen map[string]bool
	err  error
}

func newMemStore() *memStore { return &memStore{seen: map[string]bool{}} }

func (s *memStore) Seen(_ context.Context, key string) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[key] {
		return true, nil
	}
	s.seen[key] = true
	return false, nil
}

// memDeduper satisfies source.Deduper for the WithDeduper path.
type memDeduper struct{ s *memStore }

func (d memDeduper) Seen(ctx context.Context, key string) (bool, error) {
	return d.s.Seen(ctx, key)
}

func TestMiddleware_SkipsDuplicate(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	var calls atomic.Int64
	h := idempotency.Middleware(idempotency.WithStore(store))(
		func(_ context.Context, _ source.Message) source.Result {
			calls.Add(1)
			return source.Ack()
		})

	msg := stubMsg{key: []byte("k1")}
	first := h(context.Background(), msg)
	second := h(context.Background(), msg)

	if first.Action != source.ActionAck || first.Class != source.Unclassified {
		t.Errorf("first = %+v, want plain ack", first)
	}
	if second.Action != source.ActionAck || second.Class != source.Drop {
		t.Errorf("second = %+v, want ack/drop (skip)", second)
	}
	if calls.Load() != 1 {
		t.Errorf("handler calls = %d, want 1", calls.Load())
	}
}

func TestMiddleware_DistinctKeysBothRun(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	var calls atomic.Int64
	h := idempotency.Middleware(idempotency.WithStore(store))(
		func(_ context.Context, _ source.Message) source.Result {
			calls.Add(1)
			return source.Ack()
		})
	h(context.Background(), stubMsg{key: []byte("a")})
	h(context.Background(), stubMsg{key: []byte("b")})
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

func TestMiddleware_NoStoreIsPassthrough(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	h := idempotency.Middleware()(func(_ context.Context, _ source.Message) source.Result {
		calls.Add(1)
		return source.Ack()
	})
	msg := stubMsg{key: []byte("k")}
	h(context.Background(), msg)
	h(context.Background(), msg)
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2 (no dedup)", calls.Load())
	}
}

func TestMiddleware_FailsOpen(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		store *memStore
		msg   stubMsg
	}{
		{"store error", &memStore{seen: map[string]bool{}, err: errors.New("down")}, stubMsg{key: []byte("k")}},
		{"no derivable key", newMemStore(), stubMsg{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			h := idempotency.Middleware(idempotency.WithStore(tt.store))(
				func(_ context.Context, _ source.Message) source.Result {
					calls.Add(1)
					return source.Ack()
				})
			h(context.Background(), tt.msg)
			if calls.Load() != 1 {
				t.Fatalf("calls = %d, want 1 (fail open)", calls.Load())
			}
		})
	}
}

func TestDefaultKeyFunc(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		msg     stubMsg
		wantKey string
		wantOK  bool
	}{
		{"from key", stubMsg{key: []byte("k1")}, "k1", true},
		{
			"from header",
			stubMsg{headers: source.Headers{{Key: idempotency.DefaultKeyHeader, Value: "h1"}}},
			"h1", true,
		},
		{"none", stubMsg{}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotKey, gotOK := idempotency.DefaultKeyFunc(tt.msg)
			if gotKey != tt.wantKey || gotOK != tt.wantOK {
				t.Errorf("= (%q,%v), want (%q,%v)", gotKey, gotOK, tt.wantKey, tt.wantOK)
			}
		})
	}
}

func TestMiddleware_CustomKeyFunc(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	keyFunc := func(m source.Message) (string, bool) {
		v, ok := m.Headers().Get("trace-id")
		return v, ok
	}
	var calls atomic.Int64
	h := idempotency.Middleware(
		idempotency.WithStore(store),
		idempotency.WithKeyFunc(keyFunc),
	)(func(_ context.Context, _ source.Message) source.Result {
		calls.Add(1)
		return source.Ack()
	})
	msg := stubMsg{headers: source.Headers{{Key: "trace-id", Value: "t1"}}}
	h(context.Background(), msg)
	h(context.Background(), msg)
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1 (deduped by trace-id)", calls.Load())
	}
}

func TestMiddleware_WithDeduper(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	var calls atomic.Int64
	h := idempotency.Middleware(idempotency.WithDeduper(memDeduper{store}))(
		func(_ context.Context, _ source.Message) source.Result {
			calls.Add(1)
			return source.Ack()
		})
	msg := stubMsg{key: []byte("k")}
	h(context.Background(), msg)
	h(context.Background(), msg)
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestOptions_IgnoreNil(t *testing.T) {
	t.Parallel()
	// Nil store/keyfunc/deduper are ignored; with no store it is a pass-through.
	var calls atomic.Int64
	h := idempotency.Middleware(
		idempotency.WithStore(nil),
		idempotency.WithDeduper(nil),
		idempotency.WithKeyFunc(nil),
	)(func(_ context.Context, _ source.Message) source.Result {
		calls.Add(1)
		return source.Ack()
	})
	h(context.Background(), stubMsg{key: []byte("k")})
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestMiddleware_ConcurrentSameKey(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	var calls atomic.Int64
	h := idempotency.Middleware(idempotency.WithStore(store))(
		func(_ context.Context, _ source.Message) source.Result {
			calls.Add(1)
			return source.Ack()
		})
	msg := stubMsg{key: []byte("k")}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h(context.Background(), msg)
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1 (atomic Seen dedups concurrent deliveries)", calls.Load())
	}
}
