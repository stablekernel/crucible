// SPDX-License-Identifier: Apache-2.0

package cdc_test

import (
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/cdc"
)

// Example shows wiring the CDC codec into a source.Registry and decoding a
// Debezium JSON change event into a typed row image. The codec is
// instance-scoped: it is registered on a registry the caller owns, never on a
// process-global table.
func Example() {
	type user struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}

	// Register the codec under the Debezium JSON content type, and as the
	// default so a topic that carries no content-type header still routes to it.
	codec := cdc.New()
	registry := source.NewRegistry().
		Register(cdc.DebeziumJSONContentType, codec).
		SetDefault(codec)

	// A Debezium update envelope for the users table: both row images present.
	msg := exampleMessage{
		subject: "shop.public.users",
		value: []byte(`{
			"op":"u",
			"before":{"id":1,"name":"ada"},
			"after":{"id":1,"name":"ada lovelace"},
			"source":{"connector":"postgresql","db":"shop","schema":"public","table":"users"},
			"ts_ms":1700000000000
		}`),
	}

	event, err := cdc.DecodeEvent(registry, msg)
	if err != nil {
		fmt.Println("decode failed:", err)
		return
	}

	after, err := cdc.AfterAs[user](event)
	if err != nil {
		fmt.Println("after image:", err)
		return
	}
	table, _ := cdc.SourceHeaders(event).Get(cdc.TableHeader)

	fmt.Printf("op=%s table=%s id=%d name=%q\n",
		event.Operation, table, after.ID, after.Name)

	// Output:
	// op=u table=users id=1 name="ada lovelace"
}

// exampleMessage is a minimal source.Message for the example.
type exampleMessage struct {
	subject string
	value   []byte
}

func (m exampleMessage) Key() []byte             { return nil }
func (m exampleMessage) Value() []byte           { return m.value }
func (m exampleMessage) Headers() source.Headers { return nil }
func (m exampleMessage) Subject() string         { return m.subject }
func (m exampleMessage) PartitionKey() string    { return "" }
func (m exampleMessage) Cursor() source.Cursor   { return nil }
func (m exampleMessage) As(any) bool             { return false }
