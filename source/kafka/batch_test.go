// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stablekernel/crucible/source"
)

// manyFetch wraps several records on one partition into a single fetch batch,
// the shape PollRecords returns when franz-go hands back a whole poll at once.
func manyFetch(recs ...*kgo.Record) kgo.Fetches {
	parts := make([]kgo.FetchPartition, 0, 1)
	parts = append(parts, kgo.FetchPartition{
		Partition: recs[0].Partition,
		Records:   recs,
	})
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{Topic: recs[0].Topic, Partitions: parts}},
	}}
}

func TestTakeBatchPopsUpToLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		buffer   int
		limit    int
		wantN    int
		wantRest int
	}{
		{"limit below buffer takes limit", 5, 2, 2, 3},
		{"limit equals buffer takes all", 3, 3, 3, 0},
		{"limit above buffer takes buffer", 2, 9, 2, 0},
		{"limit one takes one", 4, 1, 1, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sub := newSub(&fakePoller{})
			buf := make([]*kgo.Record, tt.buffer)
			for i := range buf {
				buf[i] = rec("orders", 0, int64(i), "k", "v")
			}
			sub.buffer = buf

			got, ok := sub.takeBatch(tt.limit)
			if !ok {
				t.Fatal("takeBatch() = _,false, want true with a populated buffer")
			}
			if len(got) != tt.wantN {
				t.Errorf("took %d records, want %d", len(got), tt.wantN)
			}
			if len(sub.buffer) != tt.wantRest {
				t.Errorf("buffer left = %d, want %d", len(sub.buffer), tt.wantRest)
			}
			if sub.inFlight != tt.wantN {
				t.Errorf("inFlight = %d, want %d", sub.inFlight, tt.wantN)
			}
		})
	}
}

func TestTakeBatchEmptyBufferReportsFalse(t *testing.T) {
	t.Parallel()

	sub := newSub(&fakePoller{})
	if recs, ok := sub.takeBatch(4); ok || recs != nil {
		t.Errorf("takeBatch(empty) = %v,%v, want nil,false", recs, ok)
	}
	if sub.inFlight != 0 {
		t.Errorf("inFlight = %d, want 0 on an empty buffer", sub.inFlight)
	}
}

func TestNextBatchClampsLimitBelowOne(t *testing.T) {
	t.Parallel()

	// A non-positive limit clamps to 1, so a buffered batch hands back exactly
	// one record rather than panicking on a zero/negative slice bound.
	sub := newSub(&fakePoller{})
	sub.buffer = []*kgo.Record{
		rec("orders", 0, 1, "k", "v1"),
		rec("orders", 0, 2, "k", "v2"),
	}

	msgs, err := sub.NextBatch(context.Background(), 0)
	if err != nil {
		t.Fatalf("NextBatch(0) error = %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("NextBatch(0) = %d messages, want 1 (clamped)", len(msgs))
	}
	if string(msgs[0].Value()) != "v1" {
		t.Errorf("first message = %q, want v1", msgs[0].Value())
	}
	if len(sub.buffer) != 1 || sub.inFlight != 1 {
		t.Errorf("buffer=%d inFlight=%d, want 1/1", len(sub.buffer), sub.inFlight)
	}
}

func TestNextBatchReturnsBufferedGroupInOrder(t *testing.T) {
	t.Parallel()

	// A pre-populated buffer is served without polling: NextBatch returns the
	// records as a group, in arrival order, and counts them all in flight.
	sub := newSub(&fakePoller{})
	sub.buffer = []*kgo.Record{
		rec("orders", 1, 10, "A-1", "placed"),
		rec("orders", 1, 11, "A-2", "paid"),
		rec("orders", 1, 12, "A-3", "shipped"),
	}

	msgs, err := sub.NextBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("NextBatch() error = %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("NextBatch() = %d messages, want 3", len(msgs))
	}
	wantKeys := []string{"A-1", "A-2", "A-3"}
	for i, m := range msgs {
		if string(m.Key()) != wantKeys[i] {
			t.Errorf("message[%d] key = %q, want %q", i, m.Key(), wantKeys[i])
		}
	}
	if sub.inFlight != 3 {
		t.Errorf("inFlight = %d, want 3", sub.inFlight)
	}
}

func TestNextBatchPollsWhenBufferEmpty(t *testing.T) {
	t.Parallel()

	// With an empty buffer NextBatch falls through to a poll, buffers the fetched
	// records, then hands back up to limit of them. It also releases a rebalance
	// before polling, mirroring the single-record Next path.
	r0 := rec("orders", 0, 1, "A-1", "placed")
	r1 := rec("orders", 0, 2, "A-2", "paid")
	fp := &fakePoller{fetches: []kgo.Fetches{manyFetch(r0, r1)}}
	sub := newSub(fp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs, err := sub.NextBatch(ctx, 5)
	if err != nil {
		t.Fatalf("NextBatch() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("NextBatch() = %d messages, want 2 from the poll", len(msgs))
	}
	if string(msgs[0].Value()) != "placed" || string(msgs[1].Value()) != "paid" {
		t.Errorf("values = %q/%q, want placed/paid", msgs[0].Value(), msgs[1].Value())
	}
	if fp.allowed < 1 {
		t.Errorf("AllowRebalance called %d times, want at least 1 before the poll", fp.allowed)
	}
	if sub.inFlight != 2 {
		t.Errorf("inFlight = %d, want 2", sub.inFlight)
	}
}

func TestNextBatchPollErrorPropagates(t *testing.T) {
	t.Parallel()

	// A non-context fetch error surfaces wrapped, the same as the single-record
	// poll path, so the engine can decide whether to retry.
	boom := errors.New("partition unavailable")
	fp := &fakePoller{fetches: []kgo.Fetches{{{
		Topics: []kgo.FetchTopic{{
			Topic:      "orders",
			Partitions: []kgo.FetchPartition{{Partition: 0, Err: boom}},
		}},
	}}}}
	sub := newSub(fp)

	if _, err := sub.NextBatch(context.Background(), 4); !errors.Is(err, boom) {
		t.Fatalf("NextBatch() = %v, want wrapped %v", err, boom)
	}
}

func TestNextBatchDrainedAfterClose(t *testing.T) {
	t.Parallel()

	// A closed, fully-settled subscription with an empty buffer drains rather
	// than polling.
	sub := newSub(&fakePoller{})
	if err := sub.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := sub.NextBatch(context.Background(), 4); !errors.Is(err, source.ErrDrained) {
		t.Errorf("NextBatch() after drain = %v, want ErrDrained", err)
	}
}

func TestNextBatchRespectsContextCancel(t *testing.T) {
	t.Parallel()

	// An empty buffer and a quiet broker leave NextBatch blocked on the poll;
	// the deadline must end the wait with the context error.
	sub := newSub(&fakePoller{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if _, err := sub.NextBatch(ctx, 4); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("NextBatch() = %v, want DeadlineExceeded", err)
	}
}

func TestSettleBatchSettlesEveryMessage(t *testing.T) {
	t.Parallel()

	// One Ack result applied to a batch marks every record for commit and clears
	// the in-flight accounting for the whole group.
	fp := &fakePoller{}
	sub := newSub(fp)
	sub.inFlight = 3
	ms := []source.Message{
		newMessage(rec("orders", 0, 1, "k", "v1")),
		newMessage(rec("orders", 0, 2, "k", "v2")),
		newMessage(rec("orders", 0, 3, "k", "v3")),
	}

	if err := sub.SettleBatch(context.Background(), ms, source.Ack()); err != nil {
		t.Fatalf("SettleBatch() error = %v", err)
	}
	if fp.markedCount() != 3 {
		t.Errorf("marked = %d, want 3 (every record acked)", fp.markedCount())
	}
	if sub.inFlight != 0 {
		t.Errorf("inFlight = %d, want 0 after settling the whole batch", sub.inFlight)
	}
}

func TestSettleBatchReturnsFirstErrorButSettlesAll(t *testing.T) {
	t.Parallel()

	// A Term batch with no DLQ topic configured fails on every message; the
	// first error is returned, yet every message is still attempted so the
	// in-flight count is fully cleared.
	fp := &fakePoller{}
	sub := &subscription{client: fp} // no dlqTopic configured
	sub.inFlight = 2
	ms := []source.Message{
		newMessage(rec("orders", 0, 1, "k", "v1")),
		newMessage(rec("orders", 0, 2, "k", "v2")),
	}

	err := sub.SettleBatch(context.Background(), ms, source.Term(errors.New("poison")))
	if !errors.Is(err, ErrNoDLQTopic) {
		t.Fatalf("SettleBatch() = %v, want ErrNoDLQTopic", err)
	}
	if fp.markedCount() != 0 {
		t.Errorf("marked = %d, want 0 (nothing dead-lettered, nothing committed)", fp.markedCount())
	}
	if sub.inFlight != 0 {
		t.Errorf("inFlight = %d, want 0 (every message attempted)", sub.inFlight)
	}
}

func TestSettleBatchEmptyIsNoError(t *testing.T) {
	t.Parallel()

	sub := newSub(&fakePoller{})
	if err := sub.SettleBatch(context.Background(), nil, source.Ack()); err != nil {
		t.Errorf("SettleBatch(nil) = %v, want nil", err)
	}
}
