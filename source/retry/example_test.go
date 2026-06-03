// SPDX-License-Identifier: Apache-2.0

package retry_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/retry"
)

func ExampleMiddleware() {
	// A handler that fails transiently.
	base := func(_ context.Context, _ source.Message) source.Result {
		return source.Nak(errors.New("upstream timeout"))
	}

	mw := retry.Middleware(
		retry.WithMaxAttempts(3),
		retry.WithBackoff(100*time.Millisecond, time.Second, 2.0, false),
	)
	h := mw(base)

	// First and second attempts back off; the third exhausts and terminates.
	for attempt := 1; attempt <= 3; attempt++ {
		ctx := retry.WithAttempt(context.Background(), attempt)
		r := h(ctx, stubMsg{})
		fmt.Printf("attempt %d: %s class=%s delay=%v\n", attempt, r.Action, r.Class, r.Requeue)
	}
	// Output:
	// attempt 1: nak class=retryable delay=100ms
	// attempt 2: nak class=retryable delay=200ms
	// attempt 3: term class=retryable delay=0s
}

func ExampleMiddleware_poisonPassesThrough() {
	base := func(_ context.Context, _ source.Message) source.Result {
		return source.Term(errors.New("malformed"))
	}
	h := retry.Middleware()(base)
	r := h(context.Background(), stubMsg{})
	fmt.Printf("%s class=%s\n", r.Action, r.Class)
	// Output: term class=poison
}
