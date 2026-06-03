// SPDX-License-Identifier: Apache-2.0

package schema_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/schema"
)

func ExampleMiddleware() {
	// Require a JSON content type before the handler runs.
	v := schema.ContentTypeValidator{Allowed: []string{"application/json"}}

	base := func(_ context.Context, _ source.Message) source.Result {
		fmt.Println("handler ran")
		return source.Ack()
	}
	h := schema.Middleware(v)(base)

	// A valid message reaches the handler.
	valid := stubMsg{headers: source.Headers{{Key: "content-type", Value: "application/json"}}}
	r1 := h(context.Background(), valid)
	fmt.Printf("valid: %s\n", r1.Action)

	// An invalid message is rejected as poison and never reaches the handler.
	invalid := stubMsg{subject: "orders", headers: source.Headers{{Key: "content-type", Value: "text/csv"}}}
	r2 := h(context.Background(), invalid)
	fmt.Printf("invalid: %s/%s poison=%v\n", r2.Action, r2.Class, errors.Is(r2.Err, source.ErrPoison))
	// Output:
	// handler ran
	// valid: ack
	// invalid: term/poison poison=true
}
