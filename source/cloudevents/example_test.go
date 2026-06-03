// SPDX-License-Identifier: Apache-2.0

package cloudevents_test

import (
	"encoding/json"
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/cloudevents"
)

// Example shows wiring the CloudEvents codec into a source.Registry and
// decoding a binary-mode message into a typed payload. The codec is
// instance-scoped: it is registered on a registry the caller owns, never on a
// process-global format table.
func Example() {
	type orderCreated struct {
		ID  string `json:"id"`
		Qty int    `json:"qty"`
	}

	// Register one codec for both content modes: structured under its media
	// type, and as the default so binary-mode messages (which carry a data
	// content type such as application/json) also route to it.
	codec := cloudevents.New()
	registry := source.NewRegistry().
		Register(cloudevents.StructuredContentType, codec).
		SetDefault(codec)

	// A binary-mode message: attributes ride as ce- headers, data is the body.
	body, _ := json.Marshal(orderCreated{ID: "o-42", Qty: 3})
	msg := exampleMessage{
		value: body,
		headers: source.Headers{
			{Key: source.ContentTypeHeader, Value: "application/json"},
			{Key: "ce-specversion", Value: "1.0"},
			{Key: "ce-id", Value: "evt-1"},
			{Key: "ce-source", Value: "/shop/checkout"},
			{Key: "ce-type", Value: "com.example.order.created"},
			{Key: "ce-region", Value: "us-east"}, // extension attribute
		},
	}

	event, data, err := cloudevents.DecodeData[orderCreated](registry, msg)
	if err != nil {
		fmt.Println("decode failed:", err)
		return
	}

	region, _ := cloudevents.Extensions(event).Get(cloudevents.ExtensionHeaderPrefix + "region")
	fmt.Printf("type=%s id=%s order=%s qty=%d region=%s\n",
		event.Type(), event.ID(), data.ID, data.Qty, region)

	// Output:
	// type=com.example.order.created id=evt-1 order=o-42 qty=3 region=us-east
}

// exampleMessage is a minimal source.Message for the example.
type exampleMessage struct {
	value   []byte
	headers source.Headers
}

func (m exampleMessage) Key() []byte             { return nil }
func (m exampleMessage) Value() []byte           { return m.value }
func (m exampleMessage) Headers() source.Headers { return m.headers }
func (m exampleMessage) Subject() string         { return "orders" }
func (m exampleMessage) PartitionKey() string    { return "" }
func (m exampleMessage) Cursor() source.Cursor   { return nil }
func (m exampleMessage) As(any) bool             { return false }
