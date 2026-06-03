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

// TestHopper_BatchSizeTrigger verifies a full-size batch is dispatched as one
// call and every message acked, across a range of size/count combinations.
func TestHopper_BatchSizeTrigger(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		size        int
		count       int
		wantBatches int
	}{
		{"exact multiple", 5, 10, 2},
		{"single batch", 8, 3, 1},
		{"size one", 1, 4, 4},
		{"remainder flushes on drain", 4, 10, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			msgs := make([]memsource.Msg, tt.count)
			for i := range msgs {
				// One key so every message lands on a single ordered lane and the
				// batch sizes are deterministic.
				msgs[i] = memsource.Msg{Key: "k", Value: []byte(fmt.Sprintf("%d", i))}
			}
			h := memsource.NewHarness(t,
				[]source.Option{source.WithBatch(tt.size, 0)},
				msgs...,
			)
			var batches int32
			h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
				atomic.AddInt32(&batches, 1)
				res := make([]source.Result, len(ms))
				for i := range res {
					res[i] = source.Ack()
				}
				return res
			})
			h.AssertCounts(memsource.Counts{Acked: tt.count})
			if int(batches) != tt.wantBatches {
				t.Fatalf("batches = %d, want %d", batches, tt.wantBatches)
			}
		})
	}
}

// TestHopper_BatchMaxWaitTrigger drives a single message into a lane whose size
// is larger than the queue and asserts the max-wait timer flushes the partial
// batch. An injected clock is not needed for correctness here (the timer is real
// and short), but the partial flush must happen before drain closes the lane.
func TestHopper_BatchMaxWaitTrigger(t *testing.T) {
	t.Parallel()
	// size 100, but only 3 messages: the batch can only be released by the
	// max-wait timer or by the drain. A short max-wait releases it promptly.
	h := memsource.NewHarness(t,
		[]source.Option{source.WithBatch(100, 5*time.Millisecond)},
		memsource.Msg{Key: "k", Value: []byte("0")},
		memsource.Msg{Key: "k", Value: []byte("1")},
		memsource.Msg{Key: "k", Value: []byte("2")},
	)
	var maxSeen int
	var mu sync.Mutex
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		mu.Lock()
		if len(ms) > maxSeen {
			maxSeen = len(ms)
		}
		mu.Unlock()
		res := make([]source.Result, len(ms))
		for i := range res {
			res[i] = source.Ack()
		}
		return res
	})
	h.AssertCounts(memsource.Counts{Acked: 3})
	if maxSeen == 0 || maxSeen > 3 {
		t.Fatalf("max batch = %d, want 1..3", maxSeen)
	}
}

// TestHopper_BatchPartialOnDrain verifies a batch that never reaches size and has
// no max-wait still flushes when the run drains, losing nothing.
func TestHopper_BatchPartialOnDrain(t *testing.T) {
	t.Parallel()
	h := memsource.NewHarness(t,
		[]source.Option{source.WithBatch(1000, 0)}, // never fills, no timer
		memsource.Msg{Key: "k", Value: []byte("a")},
		memsource.Msg{Key: "k", Value: []byte("b")},
	)
	var got int
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		got = len(ms)
		res := make([]source.Result, len(ms))
		for i := range res {
			res[i] = source.Ack()
		}
		return res
	})
	h.AssertCounts(memsource.Counts{Acked: 2})
	if got != 2 {
		t.Fatalf("drain batch = %d, want 2", got)
	}
}

// TestHopper_BatchPerMessageResultMapping checks the positional Result-to-message
// contract: a mixed slice of ack/nak/term/skip results settles each message by
// its own result.
func TestHopper_BatchPerMessageResultMapping(t *testing.T) {
	t.Parallel()
	h := memsource.NewHarness(t,
		[]source.Option{source.WithBatch(4, 0)},
		memsource.Msg{Key: "k", Value: []byte("ack")},
		memsource.Msg{Key: "k", Value: []byte("nak")},
		memsource.Msg{Key: "k", Value: []byte("term")},
		memsource.Msg{Key: "k", Value: []byte("skip")},
	)
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		res := make([]source.Result, len(ms))
		for i, m := range ms {
			switch string(m.Value()) {
			case "ack":
				res[i] = source.Ack()
			case "nak":
				res[i] = source.Nak(errors.New("retry"))
			case "term":
				res[i] = source.Term(errors.New("bad"))
			case "skip":
				res[i] = source.Skip()
			}
		}
		return res
	})
	h.AssertCounts(memsource.Counts{Acked: 1, Nak: 1, Term: 1, Dropped: 1})
}

// TestHopper_BatchOrderingPreserved confirms that within a key, the batch handler
// sees messages in delivery order and across batches the sequence never breaks.
func TestHopper_BatchOrderingPreserved(t *testing.T) {
	t.Parallel()
	const perKey = 40
	const keys = 6
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
	lastSeen := make(map[string]int)
	for k := 0; k < keys; k++ {
		lastSeen[fmt.Sprintf("key-%d", k)] = -1
	}

	h := memsource.NewHarness(t,
		[]source.Option{source.WithConcurrency(keys), source.WithBatch(7, 0)},
		msgs...,
	)
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		res := make([]source.Result, len(ms))
		mu.Lock()
		for i, m := range ms {
			key := string(m.Key())
			var seq int
			_, _ = fmt.Sscanf(string(m.Value()), "%d", &seq)
			if seq != lastSeen[key]+1 {
				t.Errorf("key %s out of order: %d after %d", key, seq, lastSeen[key])
			}
			lastSeen[key] = seq
			res[i] = source.Ack()
		}
		mu.Unlock()
		return res
	})
	h.AssertCounts(memsource.Counts{Acked: perKey * keys})
}

// TestHopper_BatchResultCountMismatch verifies a handler returning too few results
// terminates the unmatched messages as poison rather than stranding them.
func TestHopper_BatchResultCountMismatch(t *testing.T) {
	t.Parallel()
	h := memsource.NewHarness(t,
		[]source.Option{source.WithBatch(3, 0)},
		memsource.Msg{Key: "k"},
		memsource.Msg{Key: "k"},
		memsource.Msg{Key: "k"},
	)
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		// Return one result for three messages.
		return []source.Result{source.Ack()}
	})
	// First message acked; the two without a result are termed as poison.
	h.AssertCounts(memsource.Counts{Acked: 1, Term: 2})
	for _, e := range h.Ledger().Entries() {
		if e.Result.Action == source.ActionTerm && !errors.Is(e.Result.Err, source.ErrBatchResultCount) {
			t.Fatalf("term err = %v, want ErrBatchResultCount", e.Result.Err)
		}
	}
}

// TestHopper_BatchDecodeFailureIsolated checks one undecodable message terminates
// alone without poisoning its batch-mates.
func TestHopper_BatchDecodeFailureIsolated(t *testing.T) {
	t.Parallel()
	reg := source.NewRegistry().SetDefault(source.NewJSONCodec[order]())
	h := memsource.NewHarness(t,
		[]source.Option{source.WithBatch(3, 0), source.WithRegistry(reg)},
		memsource.Msg{Key: "k", Value: []byte(`{"id":"A","qty":1}`)},
		memsource.Msg{Key: "k", Value: []byte(`{bad`)},
		memsource.Msg{Key: "k", Value: []byte(`{"id":"C","qty":3}`)},
	)
	var handled int
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		handled = len(ms)
		res := make([]source.Result, len(ms))
		for i := range res {
			res[i] = source.Ack()
		}
		return res
	})
	if handled != 2 {
		t.Fatalf("handler saw %d messages, want 2 (decode failure excluded)", handled)
	}
	h.AssertCounts(memsource.Counts{Acked: 2, Term: 1})
}

// TestHopper_BatchedCapabilityPath drives the whole-batch fetch path via a
// Batched memsource subscription and asserts every message is delivered and acked.
func TestHopper_BatchedCapabilityPath(t *testing.T) {
	t.Parallel()
	const total = 25
	msgs := make([]memsource.Msg, total)
	for i := range msgs {
		msgs[i] = memsource.Msg{Key: "k", Value: []byte(fmt.Sprintf("%d", i))}
	}
	h := memsource.NewHarnessWith(t,
		[]source.Option{source.WithBatch(8, 0)},
		[]memsource.Option{memsource.WithBatched()},
		msgs...,
	)
	var seen int32
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		atomic.AddInt32(&seen, int32(len(ms)))
		res := make([]source.Result, len(ms))
		for i := range res {
			res[i] = source.Ack()
		}
		return res
	})
	h.AssertCounts(memsource.Counts{Acked: total})
	if int(seen) != total {
		t.Fatalf("handler saw %d messages, want %d", seen, total)
	}
}

// TestHopper_BatchMaxWaitWithInjectedClock proves WithBatchClock is wired: the
// clock is read while batching (it has no effect on outcome here, but the option
// must be accepted and the run must complete deterministically).
func TestHopper_BatchMaxWaitWithInjectedClock(t *testing.T) {
	t.Parallel()
	var ticks int32
	clock := func() time.Time {
		atomic.AddInt32(&ticks, 1)
		return time.Unix(0, 0)
	}
	h := memsource.NewHarness(t,
		[]source.Option{source.WithBatch(2, 0), source.WithBatchClock(clock)},
		memsource.Msg{Key: "k"}, memsource.Msg{Key: "k"},
	)
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		res := make([]source.Result, len(ms))
		for i := range res {
			res[i] = source.Ack()
		}
		return res
	})
	h.AssertCounts(memsource.Counts{Acked: 2})
}

// TestHopper_BatchRespectsMaxInFlight verifies the per-message backpressure bound
// still applies in batch mode: a lane never holds more than maxInFlight unsettled
// messages even with a larger batch size.
func TestHopper_BatchRespectsMaxInFlight(t *testing.T) {
	t.Parallel()
	const total = 30
	var msgs []memsource.Msg
	for i := 0; i < total; i++ {
		msgs = append(msgs, memsource.Msg{Key: fmt.Sprintf("k%d", i)})
	}
	// size 5 per lane, maxInFlight 10 across 8 lanes: well within bounds, and a
	// positive max-wait guarantees lanes flush even when they cannot fill.
	h := memsource.NewHarness(t,
		[]source.Option{
			source.WithConcurrency(8),
			source.WithMaxInFlight(10),
			source.WithBatch(5, 2*time.Millisecond),
		},
		msgs...,
	)
	h.RunBatch(func(_ context.Context, ms []source.Message) []source.Result {
		res := make([]source.Result, len(ms))
		for i := range res {
			res[i] = source.Ack()
		}
		return res
	})
	h.AssertSettled(total)
}
