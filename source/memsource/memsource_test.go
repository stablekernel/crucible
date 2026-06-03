// SPDX-License-Identifier: Apache-2.0

package memsource_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
)

func TestInlet_DeliversInQueueOrder(t *testing.T) {
	t.Parallel()
	in := memsource.New(memsource.WithMessages(
		memsource.Msg{Key: "a", Value: []byte("1"), Subject: "orders"},
		memsource.Msg{Key: "a", Value: []byte("2")},
	))
	sub, err := in.Subscribe(context.Background(), source.SubscribeConfig{})
	if err != nil {
		t.Fatal(err)
	}

	m1, err := sub.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(m1.Value()) != "1" || m1.Subject() != "orders" {
		t.Fatalf("first message = %q on %q, want 1 on orders", m1.Value(), m1.Subject())
	}
	if m1.PartitionKey() != "a" {
		t.Fatalf("partition key = %q, want a (derived from Key)", m1.PartitionKey())
	}

	m2, _ := sub.Next(context.Background())
	if string(m2.Value()) != "2" {
		t.Fatalf("second message = %q, want 2", m2.Value())
	}
}

func TestInlet_DefaultSubject(t *testing.T) {
	t.Parallel()
	in := memsource.New(memsource.WithMessages(memsource.Msg{Key: "k"}))
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})
	m, _ := sub.Next(context.Background())
	if m.Subject() != "memsource" {
		t.Fatalf("subject = %q, want memsource", m.Subject())
	}
}

func TestSubscription_DrainsAfterClose(t *testing.T) {
	t.Parallel()
	in := memsource.New(memsource.WithMessages(memsource.Msg{Key: "k"}))
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})

	m, _ := sub.Next(context.Background())
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	// Still in-flight (not settled): Next must not yet report drained.
	if err := sub.Settle(context.Background(), m, source.Ack()); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, source.ErrDrained) {
		t.Fatalf("Next after drain = %v, want ErrDrained", err)
	}
}

func TestSubscription_NextRespectsContext(t *testing.T) {
	t.Parallel()
	in := memsource.New() // empty, never drains
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sub.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next on canceled ctx = %v, want context.Canceled", err)
	}
}

func TestSubscription_QueueWakesBlockedNext(t *testing.T) {
	t.Parallel()
	in := memsource.New()
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})

	got := make(chan string, 1)
	go func() {
		m, err := sub.Next(context.Background())
		if err != nil {
			got <- "err:" + err.Error()
			return
		}
		got <- string(m.Value())
	}()

	time.Sleep(10 * time.Millisecond)
	in.Queue(memsource.Msg{Value: []byte("late")})

	select {
	case v := <-got:
		if v != "late" {
			t.Fatalf("Next got %q, want late", v)
		}
	case <-time.After(time.Second):
		t.Fatal("Queue did not wake a blocked Next")
	}
}

func TestMessage_As(t *testing.T) {
	t.Parallel()
	in := memsource.New(memsource.WithMessages(memsource.Msg{Key: "k"}))
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})
	m, _ := sub.Next(context.Background())

	var notMatching *int
	if m.As(&notMatching) {
		t.Error("As should not match an unrelated target")
	}
}

func TestInlet_WithIDFunc(t *testing.T) {
	t.Parallel()
	ids := []string{"first", "second"}
	i := 0
	in := memsource.New(
		memsource.WithIDFunc(func() string {
			id := ids[i]
			i++
			return id
		}),
	)
	in.Queue(memsource.Msg{Key: "a"}, memsource.Msg{Key: "b"})
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})
	m1, _ := sub.Next(context.Background())
	if m1.Cursor().String() != "first" {
		t.Fatalf("cursor = %q, want first", m1.Cursor().String())
	}
}

func TestLedger_Counts(t *testing.T) {
	t.Parallel()
	l := memsourceLedgerWith(
		t,
		source.Ack(),
		source.Ack(),
		source.Skip(),
		source.Nak(errors.New("x")),
		source.Term(errors.New("y")),
	)
	want := memsource.Counts{Acked: 2, Dropped: 1, Nak: 1, Term: 1}
	if got := l.Counts(); got != want {
		t.Fatalf("counts = %+v, want %+v", got, want)
	}
	if l.Len() != 5 {
		t.Fatalf("len = %d, want 5", l.Len())
	}
}

// memsourceLedgerWith drives a harness that returns the given results in order
// and yields the resulting ledger, a helper for ledger assertions.
func memsourceLedgerWith(t *testing.T, results ...source.Result) *memsource.Ledger {
	t.Helper()
	msgs := make([]memsource.Msg, len(results))
	for i := range results {
		msgs[i] = memsource.Msg{Key: "k"} // same key => deterministic in-order settle
	}
	h := memsource.NewHarness(t, nil, msgs...)
	i := 0
	h.Run(func(context.Context, source.Message) source.Result {
		r := results[i]
		i++
		return r
	})
	return h.Ledger()
}
