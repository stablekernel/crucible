// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
)

func TestHopper_SettlesByResult(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		result source.Result
		want   memsource.Counts
	}{
		{"ack", source.Ack(), memsource.Counts{Acked: 1}},
		{"skip drops", source.Skip(), memsource.Counts{Dropped: 1}},
		{"nak", source.Nak(errors.New("transient")), memsource.Counts{Nak: 1}},
		{"term", source.Term(errors.New("bad")), memsource.Counts{Term: 1}},
		{"reject is term", source.Reject(errors.New("wrong state")), memsource.Counts{Term: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := memsource.NewHarness(t, nil, memsource.Msg{Key: "k", Value: []byte("v")})
			h.Run(func(context.Context, source.Message) source.Result { return tt.result })
			h.AssertCounts(tt.want)
		})
	}
}

// TestHopper_InProgressAndManualDispositions exercises the two dispositions the
// Counts tally does not bucket: ActionInProgress (extend deadline) and
// ActionManual (handler settled itself). Both must still flow through the
// subscription's Settle so the backend observes the chosen action, even though
// neither is an ack/nak/term.
func TestHopper_InProgressAndManualDispositions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		result source.Result
		want   source.Action
	}{
		{"in_progress", source.InProgress(), source.ActionInProgress},
		{"manual", source.Manual(), source.ActionManual},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := memsource.NewHarness(t, nil, memsource.Msg{Key: "k", Value: []byte("v")})
			h.Run(func(context.Context, source.Message) source.Result { return tt.result })
			// Neither is counted as ack/nak/term/drop.
			h.AssertCounts(memsource.Counts{})
			// But the disposition still reached the subscription's Settle.
			entries := h.Ledger().Entries()
			if len(entries) != 1 {
				t.Fatalf("settled %d messages, want 1", len(entries))
			}
			if entries[0].Result.Action != tt.want {
				t.Fatalf("settled action = %v, want %v", entries[0].Result.Action, tt.want)
			}
		})
	}
}

func TestHopper_PerKeyInOrder(t *testing.T) {
	t.Parallel()
	const perKey = 50
	const keys = 8

	var msgs []memsource.Msg
	for i := 0; i < perKey; i++ {
		for k := 0; k < keys; k++ {
			msgs = append(msgs, memsource.Msg{
				Key:   fmt.Sprintf("key-%d", k),
				Value: []byte(fmt.Sprintf("%d", i)),
			})
		}
	}

	var mu sync.Mutex
	lastSeen := make(map[string]int) // per key, the last sequence number processed

	h := memsource.NewHarness(t, []source.Option{source.WithConcurrency(keys)}, msgs...)
	h.Run(func(_ context.Context, m source.Message) source.Result {
		key := string(m.Key())
		var seq int
		_, _ = fmt.Sscanf(string(m.Value()), "%d", &seq)
		mu.Lock()
		prev, ok := lastSeen[key]
		if ok && seq != prev+1 {
			mu.Unlock()
			t.Errorf("key %s out of order: saw %d after %d", key, seq, prev)
			return source.Term(errors.New("order violation"))
		}
		lastSeen[key] = seq
		mu.Unlock()
		return source.Ack()
	})

	h.AssertCounts(memsource.Counts{Acked: perKey * keys})
}

func TestHopper_DistinctKeysRunConcurrently(t *testing.T) {
	t.Parallel()
	const keys = 4
	var msgs []memsource.Msg
	for k := 0; k < keys; k++ {
		msgs = append(msgs, memsource.Msg{Key: fmt.Sprintf("k%d", k)})
	}

	var active, maxActive int32
	var ready sync.WaitGroup
	ready.Add(keys)
	release := make(chan struct{})

	h := memsource.NewHarness(t, []source.Option{source.WithConcurrency(keys)}, msgs...)
	go h.Run(func(context.Context, source.Message) source.Result {
		cur := atomic.AddInt32(&active, 1)
		for {
			m := atomic.LoadInt32(&maxActive)
			if cur <= m || atomic.CompareAndSwapInt32(&maxActive, m, cur) {
				break
			}
		}
		ready.Done()
		<-release // hold all lanes open until every key is in-flight
		atomic.AddInt32(&active, -1)
		return source.Ack()
	})

	ready.Wait()
	close(release)

	// Give the run a moment to settle, then assert true parallelism happened.
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&maxActive) < keys {
		select {
		case <-deadline:
			t.Fatalf("max concurrent lanes = %d, want %d", maxActive, keys)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestHopper_ConcurrencyOneIsSerial(t *testing.T) {
	t.Parallel()
	var msgs []memsource.Msg
	for i := 0; i < 20; i++ {
		msgs = append(msgs, memsource.Msg{Key: fmt.Sprintf("k%d", i)}) // all distinct keys
	}

	var active, maxActive int32
	h := memsource.NewHarness(t, []source.Option{source.WithConcurrency(1)}, msgs...)
	h.Run(func(context.Context, source.Message) source.Result {
		cur := atomic.AddInt32(&active, 1)
		if cur > atomic.LoadInt32(&maxActive) {
			atomic.StoreInt32(&maxActive, cur)
		}
		time.Sleep(time.Millisecond)
		atomic.AddInt32(&active, -1)
		return source.Ack()
	})
	if maxActive != 1 {
		t.Fatalf("concurrency 1 ran %d lanes in parallel, want 1", maxActive)
	}
	h.AssertSettled(20)
}

func TestHopper_DecodeFailureTerminates(t *testing.T) {
	t.Parallel()
	reg := source.NewRegistry().SetDefault(source.NewJSONCodec[order]())
	h := memsource.NewHarness(
		t,
		[]source.Option{source.WithRegistry(reg)},
		memsource.Msg{Key: "k", Value: []byte(`{bad json`)},
	)
	handlerRan := false
	h.Run(func(context.Context, source.Message) source.Result {
		handlerRan = true
		return source.Ack()
	})
	if handlerRan {
		t.Error("handler should not run on a decode failure")
	}
	h.AssertCounts(memsource.Counts{Term: 1})

	entry := h.Ledger().Entries()[0]
	var de *source.DecodeError
	if !errors.As(entry.Result.Err, &de) {
		t.Fatalf("term error = %v, want a *DecodeError", entry.Result.Err)
	}
}

func TestHopper_DecodedValueReachesHandler(t *testing.T) {
	t.Parallel()
	reg := source.NewRegistry().SetDefault(source.NewJSONCodec[order]())
	h := memsource.NewHarness(
		t,
		[]source.Option{source.WithRegistry(reg)},
		memsource.Msg{Key: "k", Value: []byte(`{"id":"A-9","qty":3}`)},
	)
	var got order
	h.Run(func(_ context.Context, m source.Message) source.Result {
		v, ok := source.Decoded(m)
		if !ok {
			return source.Term(errors.New("no decoded value"))
		}
		got = v.(order)
		return source.Ack()
	})
	h.AssertCounts(memsource.Counts{Acked: 1})
	if got.ID != "A-9" || got.Qty != 3 {
		t.Fatalf("decoded = %+v, want {A-9 3}", got)
	}
}

func TestHopper_RawMessageWhenNoRegistry(t *testing.T) {
	t.Parallel()
	h := memsource.NewHarness(t, nil, memsource.Msg{Key: "k", Value: []byte("raw")})
	h.Run(func(_ context.Context, m source.Message) source.Result {
		if _, ok := source.Decoded(m); ok {
			return source.Term(errors.New("unexpected decoded value"))
		}
		if string(m.Value()) != "raw" {
			return source.Term(errors.New("wrong value"))
		}
		return source.Ack()
	})
	h.AssertCounts(memsource.Counts{Acked: 1})
}

func TestHopper_MiddlewareRuns(t *testing.T) {
	t.Parallel()
	var seen int32
	mw := func(next source.Handler) source.Handler {
		return func(ctx context.Context, m source.Message) source.Result {
			atomic.AddInt32(&seen, 1)
			return next(ctx, m)
		}
	}
	h := memsource.NewHarness(
		t,
		[]source.Option{source.WithMiddleware(mw)},
		memsource.Msg{Key: "k"}, memsource.Msg{Key: "k"},
	)
	h.Run(func(context.Context, source.Message) source.Result { return source.Ack() })
	if seen != 2 {
		t.Fatalf("middleware saw %d messages, want 2", seen)
	}
}

func TestHopper_GracefulDrainOnContextCancel(t *testing.T) {
	t.Parallel()
	in := memsource.New(memsource.WithMessages(memsource.Msg{Key: "k"}))
	hp := source.New()
	t.Cleanup(func() { _ = hp.Close() })

	sub, err := in.Subscribe(context.Background(), source.SubscribeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	processed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- hp.Run(ctx, sub, func(context.Context, source.Message) source.Result {
			close(processed)
			return source.Ack()
		})
	}()

	<-processed
	cancel() // cancel after the message is processed
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after cancel returned %v, want nil (clean drain)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestHopper_CloseStopsRun(t *testing.T) {
	t.Parallel()
	in := memsource.New() // empty queue, never drains on its own
	hp := source.New()

	sub, err := in.Subscribe(context.Background(), source.SubscribeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- hp.Run(context.Background(), sub, func(context.Context, source.Message) source.Result {
			return source.Ack()
		})
	}()

	time.Sleep(10 * time.Millisecond) // let the fetch loop block on an empty queue
	if err := hp.Close(); err != nil {
		t.Fatalf("Close err = %v", err)
	}
	if err := hp.Close(); err != nil {
		t.Fatalf("second Close err = %v, want idempotent", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after Close returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after Close")
	}
}

func TestHopper_ReceiveSubscribesAndRuns(t *testing.T) {
	t.Parallel()
	in := memsource.New(memsource.WithMessages(memsource.Msg{Key: "k"}))
	hp := source.New()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- hp.Receive(ctx, in, source.SubscribeConfig{Topics: []string{"t"}},
			func(context.Context, source.Message) source.Result { return source.Ack() })
	}()

	// Receive does not auto-close the subscription's stream; cancel to drain.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Receive returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receive did not return")
	}
	if c := in.Ledger().Counts(); c.Acked != 1 {
		t.Fatalf("acked = %d, want 1", c.Acked)
	}
}

// TestHopper_MaxLanesFoldsKeys verifies that WithMaxLanes bounds the lane set:
// with a single lane, many distinct keys are folded onto it and every message is
// still delivered exactly once and acked. The bound caps goroutines without
// dropping or reordering work.
func TestHopper_MaxLanesFoldsKeys(t *testing.T) {
	t.Parallel()
	const keys = 50
	msgs := make([]memsource.Msg, keys)
	for i := range msgs {
		msgs[i] = memsource.Msg{Key: fmt.Sprintf("key-%d", i), Value: []byte("v")}
	}
	h := memsource.NewHarness(t,
		[]source.Option{source.WithConcurrency(8), source.WithMaxLanes(1)},
		msgs...,
	)
	var handled int32
	h.Run(func(context.Context, source.Message) source.Result {
		atomic.AddInt32(&handled, 1)
		return source.Ack()
	})
	if int(handled) != keys {
		t.Fatalf("handled = %d, want %d (every folded key delivered once)", handled, keys)
	}
	h.AssertCounts(memsource.Counts{Acked: keys})
}

func TestHopper_FetchErrorPropagates(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("subscription broke")
	sub := &errSub{err: wantErr}
	hp := source.New()
	t.Cleanup(func() { _ = hp.Close() })

	err := hp.Run(context.Background(), sub, func(context.Context, source.Message) source.Result {
		return source.Ack()
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run err = %v, want %v", err, wantErr)
	}
}

// errSub is a Subscription whose Next always fails with a non-drain error.
type errSub struct{ err error }

func (s *errSub) Next(context.Context) (source.Message, error) { return nil, s.err }
func (s *errSub) Settle(context.Context, source.Message, source.Result) error {
	return nil
}
func (s *errSub) Close() error { return nil }

func TestHopper_SettleFailureIsLoggedNotFatal(t *testing.T) {
	t.Parallel()
	in := memsource.New(memsource.WithMessages(memsource.Msg{Key: "k"}))
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})
	failing := &settleErrSub{Subscription: sub, err: errors.New("settle broke")}
	_ = failing.Close()

	hp := source.New()
	t.Cleanup(func() { _ = hp.Close() })
	// A settle failure must not abort the run; it is logged and metered.
	if err := hp.Run(context.Background(), failing, func(context.Context, source.Message) source.Result {
		return source.Ack()
	}); err != nil {
		t.Fatalf("Run err = %v, want nil despite settle failure", err)
	}
}

// settleErrSub wraps a Subscription and forces Settle to report an error while
// still advancing the inner subscription's drain bookkeeping, so the run can
// still finish.
type settleErrSub struct {
	source.Subscription
	err error
}

func (s *settleErrSub) Settle(ctx context.Context, m source.Message, r source.Result) error {
	_ = s.Subscription.Settle(ctx, m, r)
	return s.err
}

func TestHopper_PartitionKeyOverridesKeyForLane(t *testing.T) {
	t.Parallel()
	// Two messages share a PartitionKey but differ in Key: they must run on the
	// same ordered lane, so with concurrency 2 they are still serialized.
	var msgs []memsource.Msg
	for i := 0; i < 30; i++ {
		msgs = append(msgs, memsource.Msg{
			Key:          fmt.Sprintf("distinct-%d", i),
			PartitionKey: "shared",
			Value:        []byte(fmt.Sprintf("%d", i)),
		})
	}
	last := -1
	var mu sync.Mutex
	h := memsource.NewHarness(t, []source.Option{source.WithConcurrency(2)}, msgs...)
	h.Run(func(_ context.Context, m source.Message) source.Result {
		var seq int
		_, _ = fmt.Sscanf(string(m.Value()), "%d", &seq)
		mu.Lock()
		if seq != last+1 {
			mu.Unlock()
			t.Errorf("shared lane out of order: %d after %d", seq, last)
			return source.Term(errors.New("order"))
		}
		last = seq
		mu.Unlock()
		return source.Ack()
	})
	h.AssertSettled(30)
}

func TestHopper_MaxInFlightBackpressure(t *testing.T) {
	t.Parallel()
	const total = 30
	var msgs []memsource.Msg
	for i := 0; i < total; i++ {
		msgs = append(msgs, memsource.Msg{Key: fmt.Sprintf("k%d", i)})
	}

	var inFlight, maxInFlight int32
	h := memsource.NewHarness(
		t,
		[]source.Option{source.WithConcurrency(8), source.WithMaxInFlight(3)},
		msgs...,
	)
	h.Run(func(context.Context, source.Message) source.Result {
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxInFlight)
			if cur <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, cur) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		return source.Ack()
	})
	if maxInFlight > 3 {
		t.Fatalf("max in-flight = %d, want <= 3 (backpressure)", maxInFlight)
	}
	h.AssertSettled(total)
}
