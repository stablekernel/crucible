// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"errors"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stablekernel/crucible/source"
)

// errNotKafkaMessage reports that a [source.Message] handed to Settle did not
// originate from this adapter (its As did not yield a *kgo.Record). It signals a
// programming error — mixing inlets — not a runtime data condition.
var errNotKafkaMessage = errors.New("source/kafka: message is not a kafka record")

// message adapts a franz-go *kgo.Record onto [source.Message]. It is read-only
// from a handler's view: settlement is driven by the [source.Result] the
// handler returns and applied by the subscription, never on the message.
type message struct {
	rec     *kgo.Record
	headers source.Headers
}

// newMessage wraps a record, snapshotting its headers into the neutral typed
// slice once so repeated Headers calls are allocation-free.
func newMessage(rec *kgo.Record) *message {
	var hs source.Headers
	if len(rec.Headers) > 0 {
		hs = make(source.Headers, len(rec.Headers))
		for i, h := range rec.Headers {
			hs[i] = source.Header{Key: h.Key, Value: string(h.Value)}
		}
	}
	return &message{rec: rec, headers: hs}
}

// Key returns the record's partitioning key, or nil if it had none.
func (m *message) Key() []byte { return m.rec.Key }

// Value returns the raw record payload, pre-decode.
func (m *message) Value() []byte { return m.rec.Value }

// Headers returns the record headers mapped onto the neutral typed slice.
func (m *message) Headers() source.Headers { return m.headers }

// Subject returns the topic the record arrived on.
func (m *message) Subject() string { return m.rec.Topic }

// PartitionKey returns the ordering domain as "topic/partition", the Kafka
// ordering unit the engine keys its lanes by, so records from one partition
// always run on one ordered lane.
func (m *message) PartitionKey() string {
	return m.rec.Topic + "/" + strconv.FormatInt(int64(m.rec.Partition), 10)
}

// Cursor returns the record's resumable position: its offset within the
// partition.
func (m *message) Cursor() source.Cursor {
	return offsetCursor{topic: m.rec.Topic, partition: m.rec.Partition, offset: m.rec.Offset}
}

// As assigns the underlying *kgo.Record to target if target is a **kgo.Record,
// returning whether it did. It is the documented escape hatch to reach the
// franz-go record (for a manual ack via the client, or to read broker
// metadata); prefer the neutral surface above.
func (m *message) As(target any) bool {
	if p, ok := target.(**kgo.Record); ok {
		*p = m.rec
		return true
	}
	return false
}

// recordOf recovers the *kgo.Record behind a [source.Message] via As, the path
// Settle uses to translate a neutral message back to its franz-go record. It
// reports false for a message from another inlet.
func recordOf(m source.Message) (*kgo.Record, bool) {
	if mm, ok := m.(*message); ok {
		return mm.rec, true
	}
	var rec *kgo.Record
	if m != nil && m.As(&rec) {
		return rec, true
	}
	return nil, false
}

// offsetCursor is the opaque resumable position for a Kafka record: its
// topic/partition/offset. It is meaningful only within the stream that produced
// it.
type offsetCursor struct {
	topic     string
	partition int32
	offset    int64
}

// String renders the cursor as "topic/partition@offset" for logs.
func (c offsetCursor) String() string {
	return c.topic + "/" + strconv.FormatInt(int64(c.partition), 10) + "@" + strconv.FormatInt(c.offset, 10)
}

var (
	_ source.Message = (*message)(nil)
	_ source.Cursor  = offsetCursor{}
)
