// SPDX-License-Identifier: Apache-2.0

package dlq_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/dlq"
	"github.com/stablekernel/crucible/source/retry"
)

// drain consumes a subscription to completion, invoking h on each message and
// settling with its result, and returns how many messages were delivered.
func drain(t *testing.T, sub source.Subscription, h source.Handler) int {
	t.Helper()
	ctx := context.Background()
	n := 0
	for {
		m, err := sub.Next(ctx)
		if errors.Is(err, source.ErrDrained) {
			return n
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		n++
		if err := sub.Settle(ctx, m, h(ctx, m)); err != nil {
			t.Fatalf("Settle: %v", err)
		}
	}
}

func TestMemDeadLetter_ReplayThroughHandler(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()

	// Park two failures via the middleware.
	park := dlq.Middleware(store)(func(_ context.Context, _ source.Message) source.Result {
		return source.Term(errBoom)
	})
	park(retry.WithAttempt(context.Background(), 3), stubMsg{key: []byte("a"), value: []byte("1"), subject: "orders"})
	park(retry.WithAttempt(context.Background(), 3), stubMsg{key: []byte("b"), value: []byte("2"), subject: "orders"})
	if store.Len() != 2 {
		t.Fatalf("parked = %d, want 2", store.Len())
	}

	// Replay: the parking store IS an Inlet. Drain it through a now-healthy
	// handler that succeeds, and assert attempt counts reset to 1.
	sub, err := store.Subscribe(context.Background(), source.SubscribeConfig{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	var (
		mu    sync.Mutex
		seen  []string
		attrs []int
	)
	replayHandler := func(ctx context.Context, m source.Message) source.Result {
		n, _ := retry.Attempt(ctx) // fresh context: replay is attempt 1
		mu.Lock()
		seen = append(seen, string(m.Value()))
		attrs = append(attrs, n)
		mu.Unlock()
		return source.Ack()
	}

	delivered := drain(t, sub, replayHandler)
	if delivered != 2 {
		t.Fatalf("delivered = %d, want 2", delivered)
	}
	if len(seen) != 2 || seen[0] != "1" || seen[1] != "2" {
		t.Errorf("replayed payloads = %v, want [1 2]", seen)
	}
	for _, a := range attrs {
		if a != 1 {
			t.Errorf("replay attempt = %d, want 1 (reset)", a)
		}
	}
	// Successful replay drains the store.
	if store.Len() != 0 {
		t.Errorf("store after successful replay = %d, want 0", store.Len())
	}
}

func TestMemDeadLetter_ReplayRequeuesStillFailing(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()
	_ = store.Park(context.Background(), dlq.DeadLetterRecord{Value: []byte("x"), Subject: "s"})

	sub, _ := store.Subscribe(context.Background(), source.SubscribeConfig{})
	// A replay that still fails (Term) must re-park, not lose the message.
	drain(t, sub, func(_ context.Context, _ source.Message) source.Result {
		return source.Term(errBoom)
	})
	if store.Len() != 1 {
		t.Fatalf("store after failing replay = %d, want 1 (re-parked)", store.Len())
	}
}

func TestMemDeadLetter_ReplayMessageAsRecord(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()
	_ = store.Park(context.Background(), dlq.DeadLetterRecord{
		Value:    []byte("x"),
		Subject:  "s",
		Reason:   "poison",
		Attempts: 9,
	})
	sub, _ := store.Subscribe(context.Background(), source.SubscribeConfig{})
	m, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if string(m.Value()) != "x" || m.Subject() != "s" {
		t.Errorf("value/subject = %q/%q", m.Value(), m.Subject())
	}
	if m.Key() != nil || m.PartitionKey() != "" {
		t.Errorf("key/pk = %q/%q", m.Key(), m.PartitionKey())
	}
	if m.Headers() != nil {
		t.Errorf("headers = %v, want nil", m.Headers())
	}
	var rec dlq.DeadLetterRecord
	if !m.As(&rec) {
		t.Fatal("As(*DeadLetterRecord) = false, want true")
	}
	if rec.Attempts != 9 || rec.Reason != "poison" {
		t.Errorf("record via As = %+v", rec)
	}
	if !m.As(&rec) {
		t.Fatal("As should still report true")
	}
	var wrong int
	if m.As(&wrong) {
		t.Error("As to wrong type = true, want false")
	}
	if m.Cursor().String() == "" {
		t.Error("cursor string empty")
	}
}

func TestMemDeadLetter_EmptyDrains(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()
	sub, _ := store.Subscribe(context.Background(), source.SubscribeConfig{})
	if _, err := sub.Next(context.Background()); !errors.Is(err, source.ErrDrained) {
		t.Fatalf("Next on empty = %v, want ErrDrained", err)
	}
}

func TestMemDeadLetter_NextHonorsContext(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()
	_ = store.Park(context.Background(), dlq.DeadLetterRecord{Subject: "s"})
	sub, _ := store.Subscribe(context.Background(), source.SubscribeConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sub.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next(canceled) = %v, want context.Canceled", err)
	}
}

func TestMemDeadLetter_CloseDrains(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()
	_ = store.Park(context.Background(), dlq.DeadLetterRecord{Subject: "s"})
	sub, _ := store.Subscribe(context.Background(), source.SubscribeConfig{})
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, source.ErrDrained) {
		t.Fatalf("Next after Close = %v, want ErrDrained", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("inlet Close: %v", err)
	}
}

func TestMemDeadLetter_ConcurrentPark(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = store.Park(context.Background(), dlq.DeadLetterRecord{Subject: "s"})
		}()
	}
	wg.Wait()
	if store.Len() != 50 {
		t.Fatalf("parked = %d, want 50", store.Len())
	}
}

func TestMemDeadLetter_SettleUnknownMessage(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()
	_ = store.Park(context.Background(), dlq.DeadLetterRecord{Subject: "s"})
	sub, _ := store.Subscribe(context.Background(), source.SubscribeConfig{})
	// Settling a message the subscription didn't issue is a no-op, not a panic.
	if err := sub.Settle(context.Background(), stubMsg{}, source.Term(errBoom)); err != nil {
		t.Fatalf("Settle(foreign) = %v, want nil", err)
	}
}
