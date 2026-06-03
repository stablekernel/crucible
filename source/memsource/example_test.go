// SPDX-License-Identifier: Apache-2.0

package memsource_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
)

// ExampleHarness shows the zero-infra test pattern: queue messages, run a
// handler, and assert the settle ledger — no broker, no goroutine bookkeeping.
func ExampleHarness() {
	// A *testing.T would normally come from the test; nil stands in for the doc.
	in := memsource.New(memsource.WithMessages(
		memsource.Msg{Key: "ok", Value: []byte("good")},
		memsource.Msg{Key: "bad", Value: []byte("poison")},
	))

	hp := source.New()
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})
	_ = sub.Close()

	_ = hp.Run(context.Background(), sub, func(_ context.Context, m source.Message) source.Result {
		if string(m.Value()) == "poison" {
			return source.Term(errors.New("undecodable"))
		}
		return source.Ack()
	})

	c := in.Ledger().Counts()
	fmt.Printf("acked=%d term=%d\n", c.Acked, c.Term)
	// Output: acked=1 term=1
}
