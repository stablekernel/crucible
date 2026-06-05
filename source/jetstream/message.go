// SPDX-License-Identifier: Apache-2.0

package jetstream

import (
	"strconv"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/stablekernel/crucible/source"
)

// seqCursor is the resumable position of a JetStream message: its stream
// sequence number. It satisfies [source.Cursor] and is the value a
// [subscription.SeekToCursor] resumes from.
type seqCursor uint64

// String renders the stream sequence for logs and diagnostics.
func (c seqCursor) String() string { return strconv.FormatUint(uint64(c), 10) }

// message adapts a jetstream.Msg onto [source.Message]. The vendor message is
// reachable only through As; the neutral accessors expose the common surface.
// PartitionKey is always "" because JetStream has no partitions, so the Hopper
// shards by Key (the KeyHeader value, falling back to the subject). The header
// slice is built lazily on the first [message.Headers] call and cached, so a
// message whose headers a handler never reads pays no per-message header
// allocation.
type message struct {
	msg     jetstream.Msg
	headers source.Headers
	built   bool // headers has been materialized
	key     []byte
	cursor  seqCursor
}

// newMessage wraps a jetstream.Msg, snapshotting its key and stream sequence so
// the neutral view is stable independent of later driver state. A metadata error
// (a non-JetStream message) leaves the cursor zero rather than failing the read.
// Headers are deferred to the first [message.Headers] call.
func newMessage(m jetstream.Msg) *message {
	key := []byte(m.Subject())
	if v := m.Headers().Get(KeyHeader); v != "" {
		key = []byte(v)
	}

	var cursor seqCursor
	if md, err := m.Metadata(); err == nil {
		cursor = seqCursor(md.Sequence.Stream)
	}

	return &message{msg: m, key: key, cursor: cursor}
}

// Key returns the routing key: the KeyHeader value when set, otherwise the
// subject, so the Hopper always has a deterministic shard key on a backend with
// no partitions.
func (m *message) Key() []byte { return m.key }

// Value returns the raw payload bytes.
func (m *message) Value() []byte { return m.msg.Data() }

// Headers returns the message metadata as a value-type slice, materializing it
// on first call and caching it so repeated reads are cheap. The order follows
// the NATS header map iteration and is not significant; a handler keys by name.
func (m *message) Headers() source.Headers {
	if !m.built {
		hdr := m.msg.Headers()
		headers := make(source.Headers, 0, len(hdr))
		for k, vs := range hdr {
			for _, v := range vs {
				headers = append(headers, source.Header{Key: k, Value: v})
			}
		}
		m.headers = headers
		m.built = true
	}
	return m.headers
}

// Subject returns the subject the message arrived on.
func (m *message) Subject() string { return m.msg.Subject() }

// PartitionKey returns "" because JetStream has no partitions; the Hopper shards
// by Key instead.
func (m *message) PartitionKey() string { return "" }

// Cursor returns the message's stream sequence as a resumable [source.Cursor].
func (m *message) Cursor() source.Cursor { return m.cursor }

// As assigns the underlying jetstream.Msg to target if target is a
// *jetstream.Msg, returning whether it did. It is the escape hatch for a manual
// ack (DoubleAck, batched commit) or any driver-level operation.
func (m *message) As(target any) bool {
	if p, ok := target.(*jetstream.Msg); ok {
		*p = m.msg
		return true
	}
	return false
}

// compile-time assertions.
var (
	_ source.Message = (*message)(nil)
	_ source.Cursor  = seqCursor(0)
)
