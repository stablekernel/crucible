// SPDX-License-Identifier: Apache-2.0

package file_test

import (
	"bytes"
	"context"
	"fmt"

	filesink "github.com/stablekernel/crucible/sink/file"
)

type orderShipped struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
}

// ExampleNew demonstrates writing JSONL records to an in-memory buffer.
func ExampleNew() {
	var buf bytes.Buffer
	outlet := filesink.New(&buf)

	_ = outlet.Sink(context.Background(), orderShipped{OrderID: "ORD-1", SKU: "WIDGET"})

	fmt.Print(buf.String())
	// Output: {"order_id":"ORD-1","sku":"WIDGET"}
}
