// SPDX-License-Identifier: Apache-2.0

package dlq_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/dlq"
)

func ExampleMiddleware() {
	store := dlq.NewMemDeadLetter()

	// A handler that rejects a malformed message as poison.
	base := func(_ context.Context, _ source.Message) source.Result {
		return source.Term(errors.New("malformed payload"))
	}
	h := dlq.Middleware(store)(base)

	h(context.Background(), stubMsg{value: []byte("bad"), subject: "orders"})

	rec := store.Records()[0]
	fmt.Printf("parked subject=%s reason=%s last=%q\n", rec.Subject, rec.Reason, rec.LastError)
	// Output: parked subject=orders reason=poison last="malformed payload"
}

func ExampleMemDeadLetter_replay() {
	store := dlq.NewMemDeadLetter()

	// Park a failure.
	park := dlq.Middleware(store)(func(_ context.Context, _ source.Message) source.Result {
		return source.Term(errors.New("downstream down"))
	})
	park(context.Background(), stubMsg{value: []byte("order-1"), subject: "orders"})

	// Later, drain the parking store back through a now-healthy handler. The DLQ
	// is itself an Inlet, so replay uses the same consume-and-settle loop.
	sub, _ := store.Subscribe(context.Background(), source.SubscribeConfig{})
	for {
		m, err := sub.Next(context.Background())
		if errors.Is(err, source.ErrDrained) {
			break
		}
		fmt.Printf("replaying %s\n", m.Value())
		_ = sub.Settle(context.Background(), m, source.Ack())
	}
	fmt.Printf("remaining parked: %d\n", store.Len())
	// Output:
	// replaying order-1
	// remaining parked: 0
}
