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

// recsFetch wraps several records into a one-partition fetch batch (the
// multi-record analog of oneFetch).
func recsFetch(recs ...*kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: recs[0].Topic,
			Partitions: []kgo.FetchPartition{{
				Partition: recs[0].Partition,
				Records:   recs,
			}},
		}},
	}}
}

// fetchIx reports how many scripted fetch batches the poller has served, so
// drain tests can prove Close stops polling.
func (f *fakePoller) fetchIxLocked() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetchIx
}

// nextResult carries a Next outcome across a goroutine boundary.
type nextResult struct {
	msg source.Message
	err error
}

// TestNextAfterCloseYieldsOnlyBufferedThenDrained pins the P0-1 drain contract
// against the real subscription: after Close, Next yields only already-fetched
// records, never polls again, and blocks while records are in flight — returning
// ErrDrained only once the last delivered record settles.
func TestNextAfterCloseYieldsOnlyBufferedThenDrained(t *testing.T) {
	t.Parallel()

	r0 := rec("orders", 2, 10, "k", "v0")
	r1 := rec("orders", 2, 11, "k", "v1")
	r2 := rec("orders", 2, 12, "k", "v2")
	fp := &fakePoller{fetches: []kgo.Fetches{recsFetch(r0, r1, r2)}}
	sub := newSub(fp)
	ctx := context.Background()

	m0, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if string(m0.Value()) != "v0" {
		t.Fatalf("first Next value = %q, want v0", m0.Value())
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	m1, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next() after close error = %v", err)
	}
	m2, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next() after close error = %v", err)
	}
	if string(m1.Value()) != "v1" || string(m2.Value()) != "v2" {
		t.Errorf("buffered values = %q/%q, want v1/v2", m1.Value(), m2.Value())
	}
	if got := fp.fetchIxLocked(); got != 1 {
		t.Fatalf("polls served = %d, want 1 (Next must never poll after Close)", got)
	}

	// Buffer empty, one record in flight: Next must block, not poll and not
	// drain early.
	resCh := make(chan nextResult, 1)
	go func() {
		m, err := sub.Next(ctx)
		resCh <- nextResult{msg: m, err: err}
	}()
	select {
	case res := <-resCh:
		t.Fatalf("Next returned before the last settle: msg=%v err=%v", res.msg != nil, res.err)
	case <-time.After(50 * time.Millisecond):
	}

	// Settle every delivered record; only the last settle may release the
	// blocked Next.
	for _, m := range []source.Message{m0, m1, m2} {
		if err := sub.Settle(ctx, m, source.Ack()); err != nil {
			t.Fatalf("Settle(ack) error = %v", err)
		}
	}
	select {
	case res := <-resCh:
		if !errors.Is(res.err, source.ErrDrained) {
			t.Fatalf("blocked Next = %v, want ErrDrained", res.err)
		}
		if res.msg != nil {
			t.Fatalf("drained Next yielded %+v, want no message", res.msg)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Next did not return after the last settle")
	}
	if got := fp.markedCount(); got != 3 {
		t.Errorf("marked = %d, want 3", got)
	}
}

// TestNextBatchAfterCloseYieldsOnlyBufferedThenDrained pins the P0-1 drain
// contract for the batch path: after Close, NextBatch serves only the buffer
// without polling and drains only after every settled delivery.
func TestNextBatchAfterCloseYieldsOnlyBufferedThenDrained(t *testing.T) {
	t.Parallel()

	r0 := rec("orders", 2, 10, "k", "v0")
	r1 := rec("orders", 2, 11, "k", "v1")
	r2 := rec("orders", 2, 12, "k", "v2")
	fp := &fakePoller{fetches: []kgo.Fetches{recsFetch(r0, r1, r2)}}
	sub := newSub(fp)
	ctx := context.Background()

	first, err := sub.NextBatch(ctx, 2)
	if err != nil {
		t.Fatalf("NextBatch(2) error = %v", err)
	}
	if len(first) != 2 || string(first[0].Value()) != "v0" || string(first[1].Value()) != "v1" {
		t.Fatalf("first batch values = %q/%q, want v0/v1", first[0].Value(), first[1].Value())
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	second, err := sub.NextBatch(ctx, 2)
	if err != nil {
		t.Fatalf("NextBatch(2) after close error = %v", err)
	}
	if len(second) != 1 || string(second[0].Value()) != "v2" {
		t.Fatalf("second batch = %d records (%q), want exactly [v2]", len(second), second[0].Value())
	}
	if got := fp.fetchIxLocked(); got != 1 {
		t.Fatalf("polls served = %d, want 1 (NextBatch must never poll after Close)", got)
	}

	// Settle the first batch, then hold a third call while v2 is in flight.
	for _, m := range first {
		if err := sub.Settle(ctx, m, source.Ack()); err != nil {
			t.Fatalf("Settle(batch, ack) error = %v", err)
		}
	}
	resCh := make(chan nextResult, 1)
	go func() {
		ms, err := sub.NextBatch(ctx, 2)
		if err != nil {
			resCh <- nextResult{err: err}
			return
		}
		_ = ms
		resCh <- nextResult{}
	}()
	select {
	case res := <-resCh:
		t.Fatalf("NextBatch returned before the last settle: err=%v", res.err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := sub.Settle(ctx, second[0], source.Ack()); err != nil {
		t.Fatalf("Settle(v2, ack) error = %v", err)
	}
	select {
	case res := <-resCh:
		if !errors.Is(res.err, source.ErrDrained) {
			t.Fatalf("blocked NextBatch = %v, want ErrDrained", res.err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked NextBatch did not return after the last settle")
	}
}

// TestNextAfterCloseDoesNotPollWithEmptyBufferAndNoInFlight pins the simplest
// drain shape: Close on a quiet subscription drains immediately without a
// single poll.
func TestNextAfterCloseDoesNotPollWithEmptyBufferAndNoInFlight(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{} // no scripted fetches
	sub := newSub(fp)

	if err := sub.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	resCh := make(chan nextResult, 1)
	go func() {
		m, err := sub.Next(context.Background())
		resCh <- nextResult{msg: m, err: err}
	}()
	select {
	case res := <-resCh:
		if !errors.Is(res.err, source.ErrDrained) {
			t.Fatalf("Next() after close = %v, want ErrDrained", res.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Next() did not drain after Close on an empty subscription")
	}
	if got := fp.fetchIxLocked(); got != 0 {
		t.Errorf("polls served = %d, want 0 (a closed subscription never polls)", got)
	}
}

// TestPlainNakReseeksPartitionImmediately pins the P0-2 decision: a plain Nak
// (zero Requeue) redelivers in-session via the same pause/re-seek/resume
// machinery as a delayed nak, and never marks the record.
func TestPlainNakReseeksPartitionImmediately(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{}
	sub := newSub(fp)
	r := rec("orders", 5, 88, "k", "v")

	start := time.Now()
	if err := sub.Settle(context.Background(), newMessage(r), source.Nak(errors.New("boom"))); err != nil {
		t.Fatalf("Settle(nak) error = %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("plain Nak took %v, want effectively immediate", elapsed)
	}
	if got := fp.markedCount(); got != 0 {
		t.Errorf("marked = %d, want 0 (a nak never commits)", got)
	}
	if len(fp.paused) != 1 || len(fp.resumed) != 1 {
		t.Fatalf("paused=%d resumed=%d, want 1/1", len(fp.paused), len(fp.resumed))
	}
	if len(fp.setOffsets) != 1 {
		t.Fatalf("setOffsets calls = %d, want 1 (re-seek to the record offset)", len(fp.setOffsets))
	}
	eo, ok := fp.setOffsets[0]["orders"][5]
	if !ok || eo.Offset != 88 || eo.Epoch != -1 {
		t.Errorf("re-seek = %+v, want offset 88 epoch -1 on orders/5", eo)
	}
}

// TestNakAfterDelaysThenReseeks pins the delayed-nak path end to end: the
// requested delay elapses before the re-seek, and the same
// pause/re-seek/resume choreography runs.
func TestNakAfterDelaysThenReseeks(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{}
	sub := newSub(fp)
	r := rec("orders", 5, 88, "k", "v")

	start := time.Now()
	if err := sub.Settle(context.Background(), newMessage(r), source.NakAfter(60*time.Millisecond, errors.New("boom"))); err != nil {
		t.Fatalf("Settle(nak-after) error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 55*time.Millisecond {
		t.Errorf("NakAfter(60ms) returned after %v, want >= ~55ms", elapsed)
	}
	if got := fp.markedCount(); got != 0 {
		t.Errorf("marked = %d, want 0 (a nak never commits)", got)
	}
	if len(fp.paused) != 1 || len(fp.resumed) != 1 {
		t.Fatalf("paused=%d resumed=%d, want 1/1", len(fp.paused), len(fp.resumed))
	}
	eo, ok := fp.setOffsets[0]["orders"][5]
	if !ok || eo.Offset != 88 || eo.Epoch != -1 {
		t.Errorf("re-seek = %+v, want offset 88 epoch -1 on orders/5", eo)
	}
}

// TestDuplicateSettleIsSafe defines duplicate-settle behavior: settling the
// same message twice is accepted (the backend mark is idempotent at the broker),
// the drain accounting stays honest, and the subscription still drains cleanly.
func TestDuplicateSettleIsSafe(t *testing.T) {
	t.Parallel()

	r0 := rec("orders", 2, 10, "k", "v0")
	r1 := rec("orders", 2, 11, "k", "v1")
	fp := &fakePoller{fetches: []kgo.Fetches{recsFetch(r0, r1)}}
	sub := newSub(fp)
	ctx := context.Background()

	m0, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	m1, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	for i, m := range []source.Message{m0, m1} {
		for j := 0; j < 2; j++ {
			if err := sub.Settle(ctx, m, source.Ack()); err != nil {
				t.Fatalf("duplicate Settle(%d, pass %d) error = %v", i, j, err)
			}
		}
	}
	if got := fp.markedCount(); got != 4 {
		t.Errorf("marked = %d, want 4 (each Settle marks; duplicate marks are broker-idempotent)", got)
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	resCh := make(chan nextResult, 1)
	go func() {
		m, err := sub.Next(ctx)
		resCh <- nextResult{msg: m, err: err}
	}()
	select {
	case res := <-resCh:
		if !errors.Is(res.err, source.ErrDrained) {
			t.Fatalf("Next() after duplicate settles = %v, want ErrDrained", res.err)
		}
	case <-time.After(time.Second):
		t.Fatal("subscription did not drain after duplicate settles (in-flight accounting corrupted)")
	}
}
