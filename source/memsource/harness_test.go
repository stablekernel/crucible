// SPDX-License-Identifier: Apache-2.0

package memsource_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
)

func TestHarness_Accessors(t *testing.T) {
	t.Parallel()
	h := memsource.NewHarness(t, nil, memsource.Msg{Key: "a"}, memsource.Msg{Key: "b"})

	if h.Inlet() == nil {
		t.Error("Inlet() returned nil")
	}
	if h.Hopper() == nil {
		t.Error("Hopper() returned nil")
	}

	h.RunFor(2*time.Second, func(context.Context, source.Message) source.Result {
		return source.Ack()
	})

	h.AssertSettled(2)
	h.AssertCounts(memsource.Counts{Acked: 2})

	if l := h.Ledger(); l.Len() != 2 {
		t.Fatalf("ledger len = %d, want 2", l.Len())
	}
}

func TestLedger_EntriesAndIDs(t *testing.T) {
	t.Parallel()
	h := memsource.NewHarness(t, nil, memsource.Msg{Key: "k"}, memsource.Msg{Key: "k"})
	h.Run(func(context.Context, source.Message) source.Result { return source.Ack() })

	entries := h.Ledger().Entries()
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
	ids := h.Ledger().IDs()
	if len(ids) != 2 {
		t.Fatalf("ids = %v, want 2", ids)
	}
	// IDs are assigned in queue order; an in-order single-key lane settles them
	// ascending.
	if ids[0] != "1" || ids[1] != "2" {
		t.Fatalf("ids = %v, want [1 2]", ids)
	}
}

func TestInlet_WithClockAndClose(t *testing.T) {
	t.Parallel()
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in := memsource.New(memsource.WithClock(func() time.Time { return fixed }))
	hp := source.New()
	t.Cleanup(func() { _ = hp.Close() })

	in.Queue(memsource.Msg{Key: "k"})
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})
	_ = sub.Close()
	if err := hp.Run(context.Background(), sub, func(context.Context, source.Message) source.Result {
		return source.Ack()
	}); err != nil {
		t.Fatalf("Run err = %v", err)
	}

	entries := in.Ledger().Entries()
	if len(entries) != 1 || !entries[0].At.Equal(fixed) {
		t.Fatalf("settle time = %v, want injected %v", entries[0].At, fixed)
	}

	if err := in.Close(); err != nil {
		t.Fatalf("Close err = %v", err)
	}
	if in.Ledger() == nil {
		t.Fatal("Ledger() nil after Close")
	}
}
