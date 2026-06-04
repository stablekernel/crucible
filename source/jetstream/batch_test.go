// SPDX-License-Identifier: Apache-2.0

package jetstream

import (
	"context"
	"errors"
	"testing"

	njs "github.com/nats-io/nats.go/jetstream"

	"github.com/stablekernel/crucible/source"
)

// --- NextBatch: block-for-first then drain-buffered --------------------------

func TestNextBatch_ClampsLimitBelowOne(t *testing.T) {
	t.Parallel()
	// A non-positive limit clamps to 1, so NextBatch blocks for and returns
	// exactly the first message rather than an empty slice.
	js := newFakeJS(
		&fakeMsg{subject: "orders.placed", data: []byte("a"), seq: 1},
		&fakeMsg{subject: "orders.paid", data: []byte("b"), seq: 2},
	)
	sub := newSub(t, js)

	bs, ok := sub.(source.Batched)
	if !ok {
		t.Fatal("subscription does not satisfy source.Batched")
	}
	msgs, err := bs.NextBatch(context.Background(), 0)
	if err != nil {
		t.Fatalf("NextBatch(0) error = %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("NextBatch(0) = %d messages, want 1 (clamped)", len(msgs))
	}
	if string(msgs[0].Value()) != "a" {
		t.Errorf("first message = %q, want a", msgs[0].Value())
	}
}

func TestNextBatch_DrainsBufferedUpToLimit(t *testing.T) {
	t.Parallel()
	// The pull consumer has prefetched several messages; NextBatch blocks for the
	// first then opportunistically drains the rest the iterator already holds,
	// returning them in arrival order.
	js := newFakeJS(
		&fakeMsg{subject: "orders.placed", data: []byte("a"), seq: 1},
		&fakeMsg{subject: "orders.paid", data: []byte("b"), seq: 2},
		&fakeMsg{subject: "orders.shipped", data: []byte("c"), seq: 3},
	)
	sub := newSub(t, js)

	bs := sub.(source.Batched)
	msgs, err := bs.NextBatch(context.Background(), 10)
	if err != nil {
		t.Fatalf("NextBatch() error = %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("NextBatch() = %d messages, want 3 drained", len(msgs))
	}
	want := []string{"a", "b", "c"}
	for i, m := range msgs {
		if string(m.Value()) != want[i] {
			t.Errorf("message[%d] = %q, want %q", i, m.Value(), want[i])
		}
	}
}

func TestNextBatch_StopsAtLimit(t *testing.T) {
	t.Parallel()
	// More messages are buffered than requested: NextBatch returns exactly limit
	// and leaves the remainder for the next call.
	js := newFakeJS(
		&fakeMsg{subject: "s", data: []byte("a"), seq: 1},
		&fakeMsg{subject: "s", data: []byte("b"), seq: 2},
		&fakeMsg{subject: "s", data: []byte("c"), seq: 3},
		&fakeMsg{subject: "s", data: []byte("d"), seq: 4},
	)
	sub := newSub(t, js)
	bs := sub.(source.Batched)

	first, err := bs.NextBatch(context.Background(), 2)
	if err != nil {
		t.Fatalf("first NextBatch() error = %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("first NextBatch() = %d messages, want 2", len(first))
	}
	if string(first[0].Value()) != "a" || string(first[1].Value()) != "b" {
		t.Errorf("first batch = %q/%q, want a/b", first[0].Value(), first[1].Value())
	}

	second, err := bs.NextBatch(context.Background(), 2)
	if err != nil {
		t.Fatalf("second NextBatch() error = %v", err)
	}
	if len(second) != 2 || string(second[0].Value()) != "c" || string(second[1].Value()) != "d" {
		t.Errorf("second batch = %v, want c/d", second)
	}
}

func TestNextBatch_FirstMessageDrainPropagatesError(t *testing.T) {
	t.Parallel()
	// When the first blocking read fails to create the consumer, NextBatch
	// surfaces that error rather than returning a partial batch.
	js := &fakeJS{createErr: errors.New("boom")}
	sub := newSub(t, js)
	bs := sub.(source.Batched)

	if _, err := bs.NextBatch(context.Background(), 4); err == nil || !errContains(err, "create consumer") {
		t.Fatalf("NextBatch() = %v, want create-consumer error", err)
	}
}

func TestNextBatch_DrainBreaksAfterClose(t *testing.T) {
	t.Parallel()
	// A single buffered message is returned; the drain loop then sees the closed
	// subscription and returns the one message it has rather than blocking.
	js := newFakeJS(&fakeMsg{subject: "s", data: []byte("only"), seq: 1})
	sub := newSub(t, js)
	bs := sub.(source.Batched)

	msgs, err := bs.NextBatch(context.Background(), 8)
	if err != nil {
		t.Fatalf("NextBatch() error = %v", err)
	}
	// The fake iterator yields one message then reports closed, so the drain
	// loop breaks and the batch holds exactly that message.
	if len(msgs) != 1 || string(msgs[0].Value()) != "only" {
		t.Fatalf("NextBatch() = %v, want a single message", msgs)
	}
}

func TestNextBatch_DrainedReturnsErrDrained(t *testing.T) {
	t.Parallel()
	// A closed subscription has no first message to block for, so NextBatch maps
	// the drain exactly as Next does.
	sub := newSub(t, newFakeJS(&fakeMsg{subject: "s", data: []byte("a")}))
	if err := sub.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	bs := sub.(source.Batched)
	if _, err := bs.NextBatch(context.Background(), 4); !errors.Is(err, source.ErrDrained) {
		t.Errorf("NextBatch() after close = %v, want ErrDrained", err)
	}
}

// --- SettleBatch: per-message ack vocabulary ---------------------------------

func TestSettleBatch_AcksEveryMessage(t *testing.T) {
	t.Parallel()
	// One Ack result applied to a batch double-acks every message.
	fms := []*fakeMsg{
		{subject: "s", data: []byte("a"), seq: 1},
		{subject: "s", data: []byte("b"), seq: 2},
		{subject: "s", data: []byte("c"), seq: 3},
	}
	msgs := make([]njs.Msg, len(fms))
	for i, fm := range fms {
		msgs[i] = fm
	}
	js := newFakeJS(msgs...)
	sub := newSub(t, js)
	bs := sub.(source.Batched)

	batch, err := bs.NextBatch(context.Background(), 3)
	if err != nil {
		t.Fatalf("NextBatch() error = %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("NextBatch() = %d, want 3", len(batch))
	}
	if err := bs.SettleBatch(context.Background(), batch, source.Ack()); err != nil {
		t.Fatalf("SettleBatch() error = %v", err)
	}
	for i, fm := range fms {
		if !fm.doubleAck {
			t.Errorf("message[%d] doubleAck = false, want true", i)
		}
	}
}

func TestSettleBatch_ReturnsFirstErrorButSettlesAll(t *testing.T) {
	t.Parallel()
	// Every ack fails; SettleBatch returns the first wrapped error yet still
	// attempts every message in the batch.
	boom := errors.New("server unreachable")
	fms := []*fakeMsg{
		{subject: "s", data: []byte("a"), seq: 1, settleErr: boom},
		{subject: "s", data: []byte("b"), seq: 2, settleErr: boom},
	}
	msgs := []njs.Msg{fms[0], fms[1]}
	sub := newSub(t, newFakeJS(msgs...))
	bs := sub.(source.Batched)

	batch, err := bs.NextBatch(context.Background(), 2)
	if err != nil {
		t.Fatalf("NextBatch() error = %v", err)
	}
	err = bs.SettleBatch(context.Background(), batch, source.Ack())
	if !errors.Is(err, boom) {
		t.Fatalf("SettleBatch() = %v, want wrapped %v", err, boom)
	}
	for i, fm := range fms {
		if !fm.acked {
			t.Errorf("message[%d] not attempted, want every message settled", i)
		}
	}
}

func TestSettleBatch_EmptyIsNoError(t *testing.T) {
	t.Parallel()
	sub := newSub(t, newFakeJS())
	bs := sub.(source.Batched)
	if err := bs.SettleBatch(context.Background(), nil, source.Ack()); err != nil {
		t.Errorf("SettleBatch(nil) = %v, want nil", err)
	}
}
