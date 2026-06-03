// SPDX-License-Identifier: Apache-2.0

package source

// Header is a single inbound metadata entry. Headers are typed key/value pairs,
// not a magic-string map: an inlet maps its backend's native headers (Kafka
// record headers, NATS headers) onto this shape, and a handler reads them
// through [Headers.Get] rather than indexing an untyped map.
type Header struct {
	Key   string
	Value string
}

// Headers is the immutable metadata carried alongside a [Message]. It is a value
// type: a copy is independent of the message it came from, so a handler may hold
// or forward headers without aliasing inlet state.
type Headers []Header

// Get returns the value of the first header with the given key and whether it
// was present. Keys are matched exactly (case-sensitive); inlets normalize
// backend casing when they build the slice.
func (h Headers) Get(key string) (string, bool) {
	for _, hd := range h {
		if hd.Key == key {
			return hd.Value, true
		}
	}
	return "", false
}

// Keys returns the header keys in order, including duplicates. The result is a
// fresh slice the caller may retain.
func (h Headers) Keys() []string {
	if len(h) == 0 {
		return nil
	}
	keys := make([]string, len(h))
	for i, hd := range h {
		keys[i] = hd.Key
	}
	return keys
}

// Cursor is an opaque, resumable position within a single stream: a Kafka
// offset, a JetStream stream sequence, a Redis entry ID. It is deliberately
// minimal — a stream-local coordinate that a [Seekable] inlet can resume from.
// Cursors are only meaningful within the stream that produced them; they are not
// comparable across inlets or topics.
type Cursor interface {
	// String renders the cursor for logs and diagnostics. It carries no
	// semantics beyond being stable for a given position.
	String() string
}

// Message is a backend-neutral inbound message. An inlet adapts its native
// record or message onto this interface; the underlying vendor value never
// appears in the public surface and is reachable only through As, the
// documented escape hatch for power users who must drop to the driver.
//
// Implementations are read-only from a handler's perspective: ack/nak/term is
// driven by the [Result] a [Handler] returns, which the [Hopper] applies via
// [Subscription.Settle] — a handler never mutates or settles a message itself
// (except deliberately, via As + a Manual result).
type Message interface {
	// Key is the partitioning/routing key, or nil if the producer set none.
	Key() []byte
	// Value is the raw payload bytes, pre-decode.
	Value() []byte
	// Headers is the message metadata.
	Headers() Headers
	// Subject is the topic (Kafka) or subject (JetStream) the message arrived on.
	Subject() string
	// PartitionKey identifies the ordering domain the message belongs to: the
	// Kafka "topic/partition", or "" on backends without partitions (the Hopper
	// then shards by Key). Two messages with the same non-empty PartitionKey are
	// delivered to the same ordered lane.
	PartitionKey() string
	// Cursor is the message's resumable position within its stream.
	Cursor() Cursor
	// As assigns the underlying backend message to target if the dynamic types
	// match, returning whether it did. It is the escape hatch to reach a vendor
	// value (for a manual ack, say); prefer the neutral surface above.
	As(target any) bool
}
