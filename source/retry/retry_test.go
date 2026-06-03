// SPDX-License-Identifier: Apache-2.0

package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/retry"
)

// stubMsg is a minimal [source.Message] for middleware tests.
type stubMsg struct {
	key     []byte
	value   []byte
	headers source.Headers
	subject string
}

func (m stubMsg) Key() []byte             { return m.key }
func (m stubMsg) Value() []byte           { return m.value }
func (m stubMsg) Headers() source.Headers { return m.headers }
func (m stubMsg) Subject() string         { return m.subject }
func (m stubMsg) PartitionKey() string    { return "" }
func (m stubMsg) Cursor() source.Cursor   { return stubCursor{} }
func (m stubMsg) As(any) bool             { return false }

type stubCursor struct{}

func (stubCursor) String() string { return "stub" }

var errBoom = errors.New("boom")

func TestMiddleware_Disposition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		attempt    int
		inner      source.Result
		wantAction source.Action
		wantClass  source.Classification
		wantDelay  time.Duration
	}{
		{
			name:       "retryable below cap backs off",
			attempt:    1,
			inner:      source.Nak(errBoom),
			wantAction: source.ActionNak,
			wantClass:  source.Retryable,
			wantDelay:  100 * time.Millisecond,
		},
		{
			name:       "retryable second attempt grows",
			attempt:    2,
			inner:      source.Nak(errBoom),
			wantAction: source.ActionNak,
			wantClass:  source.Retryable,
			wantDelay:  200 * time.Millisecond,
		},
		{
			name:       "retryable at cap terminates",
			attempt:    3,
			inner:      source.Nak(errBoom),
			wantAction: source.ActionTerm,
			wantClass:  source.Retryable,
		},
		{
			name:       "poison passes through to term",
			attempt:    1,
			inner:      source.Term(errBoom),
			wantAction: source.ActionTerm,
			wantClass:  source.Poison,
		},
		{
			name:       "invalid-for-state passes through to term",
			attempt:    1,
			inner:      source.Reject(errBoom),
			wantAction: source.ActionTerm,
			wantClass:  source.InvalidForState,
		},
		{
			name:       "ack untouched",
			attempt:    1,
			inner:      source.Ack(),
			wantAction: source.ActionAck,
		},
		{
			name:       "drop untouched",
			attempt:    1,
			inner:      source.Skip(),
			wantAction: source.ActionAck,
			wantClass:  source.Drop,
		},
	}

	mw := retry.Middleware(
		retry.WithMaxAttempts(3),
		retry.WithBackoff(100*time.Millisecond, 10*time.Second, 2.0, false),
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := mw(func(_ context.Context, _ source.Message) source.Result {
				return tt.inner
			})
			ctx := retry.WithAttempt(context.Background(), tt.attempt)
			got := h(ctx, stubMsg{})

			if got.Action != tt.wantAction {
				t.Errorf("action = %v, want %v", got.Action, tt.wantAction)
			}
			if got.Class != tt.wantClass {
				t.Errorf("class = %v, want %v", got.Class, tt.wantClass)
			}
			if got.Requeue != tt.wantDelay {
				t.Errorf("delay = %v, want %v", got.Requeue, tt.wantDelay)
			}
		})
	}
}

func TestMiddleware_DefaultAttemptIsOne(t *testing.T) {
	t.Parallel()
	mw := retry.Middleware(retry.WithBackoff(time.Second, time.Minute, 2.0, false))
	h := mw(func(_ context.Context, _ source.Message) source.Result {
		return source.Nak(errBoom)
	})
	// No attempt on the context: treated as attempt 1, so delay = base.
	got := h(context.Background(), stubMsg{})
	if got.Requeue != time.Second {
		t.Fatalf("delay = %v, want %v", got.Requeue, time.Second)
	}
}

func TestMiddleware_PreservesError(t *testing.T) {
	t.Parallel()
	mw := retry.Middleware(retry.WithMaxAttempts(1))
	h := mw(func(_ context.Context, _ source.Message) source.Result {
		return source.Nak(errBoom)
	})
	got := h(context.Background(), stubMsg{})
	if got.Action != source.ActionTerm {
		t.Fatalf("action = %v, want term (single attempt exhausts)", got.Action)
	}
	if !errors.Is(got.Err, errBoom) {
		t.Fatalf("err = %v, want errBoom", got.Err)
	}
}

func TestAttempt(t *testing.T) {
	t.Parallel()
	if n, ok := retry.Attempt(context.Background()); n != 1 || ok {
		t.Fatalf("Attempt(bg) = (%d,%v), want (1,false)", n, ok)
	}
	ctx := retry.WithAttempt(context.Background(), 7)
	if n, ok := retry.Attempt(ctx); n != 7 || !ok {
		t.Fatalf("Attempt(7) = (%d,%v), want (7,true)", n, ok)
	}
}

func TestWithBackoff_JitterAndCap(t *testing.T) {
	t.Parallel()
	mw := retry.Middleware(
		retry.WithMaxAttempts(10),
		retry.WithBackoff(time.Second, 4*time.Second, 2.0, true),
		retry.WithJitterSource(func() float64 { return 0.5 }),
	)
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 500 * time.Millisecond}, // 1s * 0.5
		{2, time.Second},            // 2s * 0.5
		{3, 2 * time.Second},        // 4s * 0.5
		{5, 2 * time.Second},        // capped at 4s, * 0.5
	}
	for _, c := range cases {
		h := mw(func(_ context.Context, _ source.Message) source.Result {
			return source.Nak(errBoom)
		})
		got := h(retry.WithAttempt(context.Background(), c.attempt), stubMsg{})
		if got.Requeue != c.want {
			t.Errorf("attempt %d: delay = %v, want %v", c.attempt, got.Requeue, c.want)
		}
	}
}

func TestWithBackoffFunc_Custom(t *testing.T) {
	t.Parallel()
	mw := retry.Middleware(
		retry.WithMaxAttempts(5),
		retry.WithBackoffFunc(retry.BackoffFunc(func(attempt int) time.Duration {
			return time.Duration(attempt) * time.Second
		})),
	)
	h := mw(func(_ context.Context, _ source.Message) source.Result {
		return source.Nak(errBoom)
	})
	got := h(retry.WithAttempt(context.Background(), 3), stubMsg{})
	if got.Requeue != 3*time.Second {
		t.Fatalf("delay = %v, want 3s", got.Requeue)
	}
}

func TestOptions_IgnoreInvalid(t *testing.T) {
	t.Parallel()
	// Invalid options should leave defaults intact; default max attempts is 5.
	mw := retry.Middleware(
		retry.WithMaxAttempts(0),                                // ignored
		retry.WithBackoff(0, time.Minute, 2.0, false),           // ignored (base<=0)
		retry.WithBackoff(-1, time.Minute, 0.5, false),          // ignored (factor<1)
		retry.WithBackoffFunc(nil),                              // ignored
		retry.WithClock(nil),                                    // ignored
		retry.WithJitterSource(nil),                             // ignored
		retry.WithBackoff(time.Second, time.Minute, 2.0, false), // applies
	)
	h := mw(func(_ context.Context, _ source.Message) source.Result {
		return source.Nak(errBoom)
	})
	// Attempt 4 is below the default cap of 5, so it still naks (backs off).
	got := h(retry.WithAttempt(context.Background(), 4), stubMsg{})
	if got.Action != source.ActionNak {
		t.Fatalf("action = %v, want nak (default cap 5 retained)", got.Action)
	}
}

func TestWithClock_Accepted(t *testing.T) {
	t.Parallel()
	clock := func() time.Time { return time.Unix(0, 0) }
	mw := retry.Middleware(retry.WithClock(clock))
	h := mw(func(_ context.Context, _ source.Message) source.Result { return source.Ack() })
	if got := h(context.Background(), stubMsg{}); got.Action != source.ActionAck {
		t.Fatalf("action = %v, want ack", got.Action)
	}
}
