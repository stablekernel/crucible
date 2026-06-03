// SPDX-License-Identifier: Apache-2.0

package dlq_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/dlq"
	"github.com/stablekernel/crucible/source/retry"
)

// stubMsg is a minimal [source.Message] for middleware tests.
type stubMsg struct {
	key     []byte
	value   []byte
	headers source.Headers
	subject string
	pk      string
}

func (m stubMsg) Key() []byte             { return m.key }
func (m stubMsg) Value() []byte           { return m.value }
func (m stubMsg) Headers() source.Headers { return m.headers }
func (m stubMsg) Subject() string         { return m.subject }
func (m stubMsg) PartitionKey() string    { return m.pk }
func (m stubMsg) Cursor() source.Cursor   { return stubCursor{} }
func (m stubMsg) As(any) bool             { return false }

type stubCursor struct{}

func (stubCursor) String() string { return "stub" }

var errBoom = errors.New("boom")

// failDeadLetter is a DeadLetter whose Park always errors.
type failDeadLetter struct{}

func (failDeadLetter) Park(context.Context, dlq.DeadLetterRecord) error {
	return errors.New("park failed")
}

func TestMiddleware_Routing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		inner     source.Result
		wantPark  bool
		wantClass source.Classification
	}{
		{"poison parks", source.Term(errBoom), true, source.Poison},
		{"invalid-for-state parks", source.Reject(errBoom), true, source.InvalidForState},
		{"ack does not park", source.Ack(), false, 0},
		{"nak does not park", source.Nak(errBoom), false, 0},
		{"skip does not park", source.Skip(), false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := dlq.NewMemDeadLetter()
			h := dlq.Middleware(store)(func(_ context.Context, _ source.Message) source.Result {
				return tt.inner
			})
			got := h(context.Background(), stubMsg{subject: "orders"})
			if got.Action != tt.inner.Action {
				t.Errorf("action = %v, want %v (passthrough)", got.Action, tt.inner.Action)
			}
			if tt.wantPark {
				if store.Len() != 1 {
					t.Fatalf("parked = %d, want 1", store.Len())
				}
				if rec := store.Records()[0]; rec.Class != tt.wantClass {
					t.Errorf("class = %v, want %v", rec.Class, tt.wantClass)
				}
			} else if store.Len() != 0 {
				t.Errorf("parked = %d, want 0", store.Len())
			}
		})
	}
}

func TestMiddleware_StampsMetadata(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()
	msg := stubMsg{
		key:     []byte("k1"),
		value:   []byte("payload"),
		headers: source.Headers{{Key: "h", Value: "v"}},
		subject: "orders",
		pk:      "orders/3",
	}
	h := dlq.Middleware(store)(func(_ context.Context, _ source.Message) source.Result {
		return source.Term(errBoom)
	})
	ctx := retry.WithAttempt(context.Background(), 4)
	h(ctx, msg)

	rec := store.Records()[0]
	if string(rec.Key) != "k1" || string(rec.Value) != "payload" {
		t.Errorf("key/value not copied: %q/%q", rec.Key, rec.Value)
	}
	if rec.Subject != "orders" || rec.PartitionKey != "orders/3" {
		t.Errorf("subject/pk = %q/%q", rec.Subject, rec.PartitionKey)
	}
	if rec.Attempts != 4 {
		t.Errorf("attempts = %d, want 4", rec.Attempts)
	}
	if rec.Reason != "poison" {
		t.Errorf("reason = %q, want poison", rec.Reason)
	}
	if rec.LastError != errBoom.Error() {
		t.Errorf("last error = %q, want %q", rec.LastError, errBoom.Error())
	}
	if v, _ := rec.Headers.Get("h"); v != "v" {
		t.Errorf("headers not preserved: %v", rec.Headers)
	}
}

func TestMiddleware_RecordDoesNotAliasInput(t *testing.T) {
	t.Parallel()
	store := dlq.NewMemDeadLetter()
	key := []byte("orig")
	h := dlq.Middleware(store)(func(_ context.Context, _ source.Message) source.Result {
		return source.Term(errBoom)
	})
	h(context.Background(), stubMsg{key: key, value: []byte("v"), subject: "s"})
	key[0] = 'X' // mutate the caller's buffer after parking
	if string(store.Records()[0].Key) != "orig" {
		t.Fatalf("record aliases caller buffer: %q", store.Records()[0].Key)
	}
}

func TestMiddleware_ParkRetryable(t *testing.T) {
	t.Parallel()
	// Retry escalates an exhausted retryable failure to Term with class Retryable.
	exhausted := source.Result{Action: source.ActionTerm, Class: source.Retryable, Err: errBoom}

	t.Run("parked by default", func(t *testing.T) {
		t.Parallel()
		store := dlq.NewMemDeadLetter()
		h := dlq.Middleware(store)(func(_ context.Context, _ source.Message) source.Result {
			return exhausted
		})
		h(context.Background(), stubMsg{subject: "s"})
		if store.Len() != 1 {
			t.Fatalf("parked = %d, want 1", store.Len())
		}
	})

	t.Run("skipped when disabled", func(t *testing.T) {
		t.Parallel()
		store := dlq.NewMemDeadLetter()
		h := dlq.Middleware(store, dlq.WithParkRetryable(false))(
			func(_ context.Context, _ source.Message) source.Result { return exhausted })
		got := h(context.Background(), stubMsg{subject: "s"})
		if store.Len() != 0 {
			t.Fatalf("parked = %d, want 0", store.Len())
		}
		if got.Action != source.ActionTerm {
			t.Fatalf("action = %v, want term passthrough", got.Action)
		}
	})
}

func TestMiddleware_ParkFailureNaks(t *testing.T) {
	t.Parallel()
	h := dlq.Middleware(failDeadLetter{})(func(_ context.Context, _ source.Message) source.Result {
		return source.Term(errBoom)
	})
	got := h(context.Background(), stubMsg{subject: "s"})
	if got.Action != source.ActionNak {
		t.Fatalf("action = %v, want nak on park failure", got.Action)
	}
	if !errors.Is(got.Err, errBoom) {
		t.Errorf("joined err lost original: %v", got.Err)
	}
}

func TestMiddleware_NilDeadLetterIsPassthrough(t *testing.T) {
	t.Parallel()
	h := dlq.Middleware(nil)(func(_ context.Context, _ source.Message) source.Result {
		return source.Term(errBoom)
	})
	if got := h(context.Background(), stubMsg{}); got.Action != source.ActionTerm {
		t.Fatalf("action = %v, want term passthrough", got.Action)
	}
}
