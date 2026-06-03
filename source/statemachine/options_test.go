// SPDX-License-Identifier: Apache-2.0

package statemachine_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/statemachine"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/telemetry"
)

func TestDefaultEventID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		m    source.Message
		want string
	}{
		{name: "header wins", m: msg("hdr-id", "cur-1"), want: "hdr-id"},
		{
			name: "falls back to cursor",
			m:    fakeMessage{cursor: fakeCursor("cur-only")},
			want: "cur-only",
		},
		{
			name: "empty when neither present",
			m:    fakeMessage{},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := statemachine.DefaultEventID(tc.m); got != tc.want {
				t.Fatalf("DefaultEventID = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDrive_WithEventID_CustomExtractor(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store)

	// Derive the id from the subject rather than the header, so two messages with
	// different header ids but the same subject dedup against each other.
	id := func(msg source.Message) string { return msg.Subject() }
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded, statemachine.WithEventID(id),
	)

	if res := h(context.Background(), msg("a", "c1")); res.Action != source.ActionAck || res.Class == source.Drop {
		t.Fatalf("first = %v/%v, want plain ack", res.Action, res.Class)
	}
	// Different header id, same subject → same derived id → skipped.
	redo := h(context.Background(), msg("b", "c2"))
	if redo.Class != source.Drop {
		t.Fatalf("redelivery class = %v, want drop (skip via custom id)", redo.Class)
	}
}

func TestDrive_WithTracer_SpansStarted(t *testing.T) {
	t.Parallel()
	m := buildTurnstile()
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	seedFunded(t, m, store)

	var started int64
	tr := &countingTracer{started: &started}
	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](
		m, store, routeFunded,
		statemachine.WithTracer(tr),
		statemachine.WithSpanName("custom.drive"),
	)

	if res := h(context.Background(), msg("e1", "c1")); res.Action != source.ActionAck {
		t.Fatalf("action = %v, want ack", res.Action)
	}
	if atomic.LoadInt64(&started) != 1 {
		t.Fatalf("spans started = %d, want 1", started)
	}
	if tr.lastName != "custom.drive" {
		t.Fatalf("span name = %q, want custom.drive", tr.lastName)
	}
}

func TestDrive_RestoreFailure_IsNak(t *testing.T) {
	t.Parallel()
	// A record whose snapshot names a different machine cannot be restored: a
	// transient/operational failure, surfaced as a nak.
	m := buildTurnstile()
	other := state.Forge[turnstileState, turnstileEvent, *turnstile]("other").
		State(locked).Initial(locked).
		CurrentStateFn(func(*turnstile) turnstileState { return locked }).
		Quench(state.Strict())
	store := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	bad := other.Cast(&turnstile{Funded: true}, state.WithInitialState[turnstileState](locked))
	_ = store.Save(context.Background(), keyOf,
		statemachine.Record[turnstileState, turnstileEvent, *turnstile]{Snapshot: bad.Snapshot(), Version: 1}, 0)

	h := statemachine.Drive[turnstileState, turnstileEvent, *turnstile](m, store, routeFunded)
	res := h(context.Background(), msg("e1", "c1"))
	if res.Action != source.ActionNak {
		t.Fatalf("action = %v, want nak on restore failure (err=%v)", res.Action, res.Err)
	}
}

// countingTracer is a telemetry.Tracer that counts Start calls and records the
// last span name, returning nop spans.
type countingTracer struct {
	started  *int64
	lastName string
}

func (t *countingTracer) Start(ctx context.Context, name string, _ ...telemetry.Attr) (context.Context, telemetry.Span) {
	atomic.AddInt64(t.started, 1)
	t.lastName = name
	_, span := telemetry.NopTracer().Start(ctx, name)
	return ctx, span
}
