// SPDX-License-Identifier: Apache-2.0

package redis

import (
	goredis "github.com/redis/go-redis/v9"

	"github.com/stablekernel/crucible/source"
)

// KeyHeader is the entry field an inbound stream entry may set to carry an
// explicit partitioning/routing key. When present, the adapter uses its value
// as [source.Message.Key]; otherwise the key falls back to the stream name so
// the Hopper still shards deterministically. A Redis Stream has no partitions,
// so [source.Message.PartitionKey] is always "" and the Hopper shards by Key.
const KeyHeader = "crucible-key"

// ValueField is the entry field the adapter reads as the raw payload bytes when
// an entry carries one. A producer that writes a single payload field under
// this name round-trips cleanly: the field becomes [source.Message.Value] and
// every other field becomes a header. When the field is absent, Value is nil
// and the entry's fields are still exposed as headers.
const ValueField = "value"

// idCursor is the resumable position of a Redis Stream entry: its entry ID
// ("millisecondsTime-sequence"). It satisfies [source.Cursor] and is the value
// a [subscription.SeekToCursor] resumes from via XRANGE.
type idCursor string

// String renders the entry ID for logs and diagnostics.
func (c idCursor) String() string { return string(c) }

// message adapts a go-redis XMessage onto [source.Message]. The vendor entry is
// reachable only through As; the neutral accessors expose the common surface.
// The fields are snapshotted at construction so the neutral view is a stable
// value independent of later driver state.
type message struct {
	entry   goredis.XMessage
	stream  string
	headers source.Headers
	key     []byte
	value   []byte
}

// newMessage wraps a go-redis XMessage from stream, projecting its fields onto
// the neutral surface: the [ValueField] field (when present) becomes the raw
// value, the [KeyHeader] field (when present) becomes the routing key, and every
// field is exposed as a header. The key falls back to the stream name so the
// Hopper always has a deterministic shard key on a backend with no partitions.
func newMessage(stream string, entry goredis.XMessage) *message {
	headers := make(source.Headers, 0, len(entry.Values))
	var value []byte
	for k, v := range entry.Values {
		s := toString(v)
		headers = append(headers, source.Header{Key: k, Value: s})
		if k == ValueField {
			value = []byte(s)
		}
	}

	key := []byte(stream)
	if v, ok := entry.Values[KeyHeader]; ok {
		if s := toString(v); s != "" {
			key = []byte(s)
		}
	}

	return &message{
		entry:   entry,
		stream:  stream,
		headers: headers,
		key:     key,
		value:   value,
	}
}

// toString renders a Redis field value as a string. Redis stream fields are
// always strings on the wire; go-redis decodes them as string, so the type
// switch is defensive and the string branch is the live path.
func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	if s, ok := v.([]byte); ok {
		return string(s)
	}
	return ""
}

// Key returns the routing key: the [KeyHeader] field value when set, otherwise
// the stream name, so the Hopper always has a deterministic shard key on a
// backend with no partitions.
func (m *message) Key() []byte { return m.key }

// Value returns the raw payload bytes: the [ValueField] field when present, else
// nil. The full set of entry fields remains available through [message.Headers].
func (m *message) Value() []byte { return m.value }

// Headers returns the entry's fields as a value-type slice.
func (m *message) Headers() source.Headers { return m.headers }

// Subject returns the stream the entry arrived on.
func (m *message) Subject() string { return m.stream }

// PartitionKey returns "" because a Redis Stream has no partitions; the Hopper
// shards by Key instead.
func (m *message) PartitionKey() string { return "" }

// Cursor returns the entry ID as a resumable [source.Cursor].
func (m *message) Cursor() source.Cursor { return idCursor(m.entry.ID) }

// As assigns the underlying go-redis XMessage to target if target is a
// *redis.XMessage, returning whether it did. It is the escape hatch to reach the
// vendor entry (for the raw fields, say) without a vendor type in the neutral
// surface.
func (m *message) As(target any) bool {
	if p, ok := target.(*goredis.XMessage); ok {
		*p = m.entry
		return true
	}
	return false
}

// compile-time assertions.
var (
	_ source.Message = (*message)(nil)
	_ source.Cursor  = idCursor("")
)
