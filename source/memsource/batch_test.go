// SPDX-License-Identifier: Apache-2.0

package memsource_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
)

// TestHarness_RunBatch_DrivesBatchLane drives the harness batch lane across a
// range of size/count combinations and asserts batch contents, the per-call
// grouping, and a clean drain of every queued message. It exercises the
// RunBatch/RunBatchFor harness entry points from the memsource package itself.
func TestHarness_RunBatch_DrivesBatchLane(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		size      int
		count     int
		wantSizes []int
	}{
		{name: "single full batch", size: 3, count: 3, wantSizes: []int{3}},
		{name: "two full batches", size: 2, count: 4, wantSizes: []int{2, 2}},
		{name: "trailing partial batch", size: 3, count: 5, wantSizes: []int{3, 2}},
		{name: "size exceeds count", size: 10, count: 4, wantSizes: []int{4}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msgs := make([]memsource.Msg, tt.count)
			for i := range msgs {
				// One key keeps every message in a single ordered lane so the
				// batch sizes are deterministic.
				msgs[i] = memsource.Msg{Key: "k", Value: []byte(fmt.Sprintf("%d", i))}
			}

			var (
				mu        sync.Mutex
				gotSizes  []int
				gotValues []string
			)
			h := memsource.NewHarness(t,
				[]source.Option{source.WithBatch(tt.size, 0)},
				msgs...,
			)
			h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
				res := make([]source.Result, len(ms))
				mu.Lock()
				gotSizes = append(gotSizes, len(ms))
				for i, m := range ms {
					gotValues = append(gotValues, string(m.Value()))
					res[i] = source.Ack()
				}
				mu.Unlock()
				return res
			})

			if !equalInts(gotSizes, tt.wantSizes) {
				t.Fatalf("batch sizes = %v, want %v", gotSizes, tt.wantSizes)
			}
			// The single lane preserves arrival order across the whole run.
			wantValues := make([]string, tt.count)
			for i := range wantValues {
				wantValues[i] = fmt.Sprintf("%d", i)
			}
			if !equalStrings(gotValues, wantValues) {
				t.Fatalf("delivered values = %v, want %v", gotValues, wantValues)
			}
			h.AssertCounts(memsource.Counts{Acked: tt.count})
			h.AssertSettled(tt.count)
		})
	}
}

// TestHarness_RunBatch_SettleByResult proves the harness settles each message in
// a batch by its own result, not a single batch-wide outcome: a mixed result
// slice acks, naks, and terms the corresponding positions.
func TestHarness_RunBatch_SettleByResult(t *testing.T) {
	t.Parallel()
	h := memsource.NewHarness(t,
		[]source.Option{source.WithBatch(3, 0)},
		memsource.Msg{Key: "k", Value: []byte("0")},
		memsource.Msg{Key: "k", Value: []byte("1")},
		memsource.Msg{Key: "k", Value: []byte("2")},
	)
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		res := make([]source.Result, len(ms))
		for i := range ms {
			switch i {
			case 0:
				res[i] = source.Ack()
			case 1:
				res[i] = source.Nak(fmt.Errorf("retry %d", i))
			default:
				res[i] = source.Term(fmt.Errorf("poison %d", i))
			}
		}
		return res
	})
	h.AssertCounts(memsource.Counts{Acked: 1, Nak: 1, Term: 1})
}

// TestHarness_RunBatchFor_DrainsOnTimeout confirms RunBatchFor passes an explicit
// timeout through and still drains every queued message under it.
func TestHarness_RunBatchFor_DrainsOnTimeout(t *testing.T) {
	t.Parallel()
	h := memsource.NewHarness(t,
		[]source.Option{source.WithBatch(2, 0)},
		memsource.Msg{Key: "k"},
		memsource.Msg{Key: "k"},
		memsource.Msg{Key: "k"},
	)
	h.RunBatchFor(2*time.Second, func(_ context.Context, ms []source.Message) []source.Result {
		res := make([]source.Result, len(ms))
		for i := range ms {
			res[i] = source.Ack()
		}
		return res
	})
	h.AssertCounts(memsource.Counts{Acked: 3})
}

// TestHarness_RunBatch_BatchedCapability drives the WithBatched subscription so
// the engine takes the whole-batch NextBatch/SettleBatch path. It asserts the
// batched inlet still delivers and settles every queued message in order.
func TestHarness_RunBatch_BatchedCapability(t *testing.T) {
	t.Parallel()
	const count = 12
	msgs := make([]memsource.Msg, count)
	for i := range msgs {
		msgs[i] = memsource.Msg{Key: "k", Value: []byte(fmt.Sprintf("%d", i))}
	}

	var (
		mu        sync.Mutex
		delivered []string
	)
	h := memsource.NewHarnessWith(t,
		[]source.Option{source.WithBatch(8, 0)},
		[]memsource.Option{memsource.WithBatched()},
		msgs...,
	)
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		res := make([]source.Result, len(ms))
		mu.Lock()
		for i, m := range ms {
			delivered = append(delivered, string(m.Value()))
			res[i] = source.Ack()
		}
		mu.Unlock()
		return res
	})

	want := make([]string, count)
	for i := range want {
		want[i] = fmt.Sprintf("%d", i)
	}
	if !equalStrings(delivered, want) {
		t.Fatalf("batched delivery = %v, want %v", delivered, want)
	}
	h.AssertCounts(memsource.Counts{Acked: count})
	h.AssertSettled(count)
}

// TestSubscription_NextBatch_DrainsQueued exercises the batched subscription's
// NextBatch directly: it blocks for the first message then drains whatever else
// is queued without blocking, capped at the limit, and never exceeds the queue.
func TestSubscription_NextBatch_DrainsQueued(t *testing.T) {
	t.Parallel()
	in := memsource.New(memsource.WithBatched())
	in.Queue(
		memsource.Msg{Key: "k", Value: []byte("a")},
		memsource.Msg{Key: "k", Value: []byte("b")},
		memsource.Msg{Key: "k", Value: []byte("c")},
	)
	sub, err := in.Subscribe(context.Background(), source.SubscribeConfig{})
	if err != nil {
		t.Fatalf("Subscribe err = %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	batched, ok := sub.(source.Batched)
	if !ok {
		t.Fatalf("WithBatched subscription does not satisfy source.Batched")
	}

	// limit below 1 is normalized to 1: a single message comes back.
	first, err := batched.NextBatch(context.Background(), 0)
	if err != nil {
		t.Fatalf("NextBatch(0) err = %v", err)
	}
	if len(first) != 1 || string(first[0].Value()) != "a" {
		t.Fatalf("NextBatch(0) = %v, want one message 'a'", values(first))
	}

	// A larger limit drains the remaining two without blocking, capped at queue.
	rest, err := batched.NextBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("NextBatch(10) err = %v", err)
	}
	if got := values(rest); !equalStrings(got, []string{"b", "c"}) {
		t.Fatalf("NextBatch(10) = %v, want [b c]", got)
	}

	// SettleBatch records every message in the slice on the ledger.
	all := append(append([]source.Message{}, first...), rest...)
	if err := batched.SettleBatch(context.Background(), all, source.Ack()); err != nil {
		t.Fatalf("SettleBatch err = %v", err)
	}
	if got := in.Ledger().Len(); got != 3 {
		t.Fatalf("ledger len after SettleBatch = %d, want 3", got)
	}
	if c := in.Ledger().Counts(); c != (memsource.Counts{Acked: 3}) {
		t.Fatalf("counts after SettleBatch = %+v, want Acked:3", c)
	}
}

// TestSubscription_NextBatch_DrainedReportsErr confirms NextBatch surfaces
// ErrDrained once the subscription is closed and its queue is empty, the signal
// the batch run loop uses to exit cleanly.
func TestSubscription_NextBatch_DrainedReportsErr(t *testing.T) {
	t.Parallel()
	in := memsource.New(memsource.WithBatched())
	sub, err := in.Subscribe(context.Background(), source.SubscribeConfig{})
	if err != nil {
		t.Fatalf("Subscribe err = %v", err)
	}
	batched := sub.(source.Batched)
	_ = sub.Close()

	if _, err := batched.NextBatch(context.Background(), 4); err != source.ErrDrained {
		t.Fatalf("NextBatch after close err = %v, want ErrDrained", err)
	}
}

// TestMessage_AccessorsAndAsEscapeHatch asserts the in-memory message exposes
// its Key and Headers, and that the As escape hatch declines a target type it
// does not recognize (the documented narrow contract: it matches only the
// concrete **message).
func TestMessage_AccessorsAndAsEscapeHatch(t *testing.T) {
	t.Parallel()
	h := memsource.NewHarness(t, nil, memsource.Msg{
		Key:     "route-key",
		Value:   []byte("payload"),
		Headers: source.Headers{{Key: "tenant", Value: "acme"}},
	})
	h.Run(func(_ context.Context, m source.Message) source.Result {
		if got := string(m.Key()); got != "route-key" {
			t.Errorf("Key() = %q, want route-key", got)
		}
		if got, ok := m.Headers().Get("tenant"); !ok || got != "acme" {
			t.Errorf("Headers.Get(tenant) = %q,%v, want acme,true", got, ok)
		}
		// As declines a target it does not recognize; the concrete **message
		// target is unexported, so an external caller cannot match it.
		var other *int
		if m.As(&other) {
			t.Error("As matched an unrelated target type")
		}
		return source.Ack()
	})
	h.AssertCounts(memsource.Counts{Acked: 1})
}

// equalInts reports whether two int slices have identical contents in order.
func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// equalStrings reports whether two string slices have identical contents in
// order.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// values extracts the string values of a message slice for assertion messages.
func values(ms []source.Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m.Value())
	}
	return out
}
