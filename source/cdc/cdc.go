// SPDX-License-Identifier: Apache-2.0

// Package cdc is a source codec that decodes change-data-capture (CDC) events
// into a typed [ChangeEvent]. It plugs into a [source.Registry] as an
// instance-scoped [source.Codec]: construct one with [New] and register it
// under the content types your CDC topic carries, or set it as the registry
// default. There is no package-global registration; every codec is constructed
// and injected, never shared by import.
//
// # Scope
//
// This codec handles the change-event envelope only: the standard Debezium JSON
// shape (an "op" of c/r/u/d, "before"/"after" row images, a "source" metadata
// block, and a "ts_ms" commit timestamp), which is also the de-facto OpenCDC
// normalized record shape. It decodes one row-change message into a
// [ChangeEvent] whose row images stay as deferred JSON ([RawJSON]) so a handler
// recovers a concrete row type only when it needs one, via [BeforeAs] / [AfterAs].
//
// A native database write-ahead-log connector (reading a Postgres logical
// replication slot, a MySQL binlog, and so on) is deliberately out of scope and
// tracked as future work. The intended pattern is to let an existing CDC
// connector (Debezium, or any producer emitting the same envelope) write change
// events to a topic, consume that topic through a backend inlet such as
// source/kafka, decode each message with this codec, and drive a statechart per
// primary key through source/statemachine.
//
// # Decoded value
//
// Decode yields a [ChangeEvent] (by value). Recover it from a registry result
// with [EventOf] or the one-call [DecodeEvent]; project its row images into a
// concrete type with [BeforeAs] / [AfterAs]. Useful fields from the source
// metadata block are surfaced as core [source.Headers] (see [SourceHeaders]) so
// a handler reads them through the same typed-header surface as any other
// inbound metadata instead of a magic-string map.
//
// # Tombstones
//
// A Kafka log-compaction tombstone (an empty payload) decodes to a [ChangeEvent]
// whose Operation is [OpTombstone] and whose row images are both absent. It is a
// valid, retryable-free outcome, not a decode failure: a handler routes it (a
// delete-and-forget for the key) or skips it.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package cdc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/stablekernel/crucible/source"
)

// DebeziumJSONContentType is the media type a Debezium JSON converter stamps on
// change-event messages. Register the [Codec] under this type to decode a
// Debezium topic, or set the codec as the registry default when the topic
// carries no content type.
const DebeziumJSONContentType = "application/vnd.debezium.cdc+json"

// Sentinel errors a malformed envelope reports. Each is wrapped by the codec
// with context and, through the [source.Registry], in a [*source.DecodeError]
// that reports [source.ErrPoison]: a structurally invalid change event cannot be
// retried into validity. Match them with errors.Is.
var (
	// ErrMalformedEnvelope reports a payload that is not a JSON object or whose
	// top-level shape is not a change-event envelope.
	ErrMalformedEnvelope = errors.New("cdc: malformed change-event envelope")
	// ErrUnknownOperation reports an "op" value the codec does not recognize.
	ErrUnknownOperation = errors.New("cdc: unknown operation")
	// ErrMissingImage reports a typed-image projection ([BeforeAs] / [AfterAs])
	// for a row image the operation does not carry (a "before" image on a create,
	// say).
	ErrMissingImage = errors.New("cdc: row image absent")
)

// Operation is the kind of row change a [ChangeEvent] carries, mirroring the
// Debezium "op" field.
type Operation uint8

const (
	// OpUnknown is the zero value: an envelope whose operation has not been set.
	OpUnknown Operation = iota
	// OpCreate is an insert ("c"): only an after-image is present.
	OpCreate
	// OpRead is a snapshot read ("r"): a row captured during the connector's
	// initial snapshot, carrying an after-image and no before-image.
	OpRead
	// OpUpdate is an update ("u"): both before- and after-images are present
	// when the connector is configured to capture them.
	OpUpdate
	// OpDelete is a delete ("d"): only a before-image is present; the after-image
	// is null.
	OpDelete
	// OpTombstone is a log-compaction tombstone: an empty message that follows a
	// delete on a compacted topic. It carries no images.
	OpTombstone
)

// String renders the operation as its Debezium op code ("c", "r", "u", "d"),
// "tombstone", or "unknown" for diagnostics and headers.
func (o Operation) String() string {
	switch o {
	case OpCreate:
		return "c"
	case OpRead:
		return "r"
	case OpUpdate:
		return "u"
	case OpDelete:
		return "d"
	case OpTombstone:
		return "tombstone"
	default:
		return "unknown"
	}
}

// operationFromCode maps a Debezium "op" code to an [Operation], reporting
// whether the code was recognized.
func operationFromCode(code string) (Operation, bool) {
	switch code {
	case "c":
		return OpCreate, true
	case "r":
		return OpRead, true
	case "u":
		return OpUpdate, true
	case "d":
		return OpDelete, true
	default:
		return OpUnknown, false
	}
}

// RawJSON is a deferred row image: the raw JSON bytes of a "before" or "after"
// row, decoded into a concrete type on demand via [BeforeAs] / [AfterAs] (or
// directly with [RawJSON.As]). It is nil when the image is absent. Keeping the
// image deferred lets one codec serve every table on a topic without binding to
// a row type at decode time.
type RawJSON []byte

// Present reports whether the row image is set (non-nil). A create has no
// before-image and a delete has no after-image, so a handler checks Present
// before projecting.
func (r RawJSON) Present() bool { return r != nil }

// As decodes the deferred row image into out. It returns [ErrMissingImage] when
// the image is absent (nil), and the json.Unmarshal error when the bytes do not
// fit out's shape.
func (r RawJSON) As(out any) error {
	if r == nil {
		return ErrMissingImage
	}
	if err := json.Unmarshal(r, out); err != nil {
		return fmt.Errorf("cdc: decode row image: %w", err)
	}
	return nil
}

// SourceMetadata is the decoded "source" block of a change event: the connector
// metadata Debezium attaches to every record. Fields absent from a given
// connector's payload stay at their zero value. The full block is retained as
// [SourceMetadata.Raw] for fields not surfaced here.
type SourceMetadata struct {
	// Connector is the Debezium connector name ("postgresql", "mysql", ...).
	Connector string
	// Name is the logical server / database-server name configured on the
	// connector.
	Name string
	// Database is the source database the change came from.
	Database string
	// Schema is the source schema (Postgres) the table lives in.
	Schema string
	// Table is the source table the row belongs to.
	Table string
	// Snapshot reports whether the record was captured during the connector's
	// initial snapshot (the Debezium "snapshot" marker is truthy).
	Snapshot bool
	// LSN is the source log sequence number / position, as a string to span the
	// per-connector representations (a Postgres LSN, a MySQL binlog coordinate).
	LSN string
	// TxID is the source transaction identifier, when the connector reports one.
	TxID string
	// Raw is the undecoded source block, for fields not surfaced above.
	Raw RawJSON
}

// ChangeEvent is a decoded CDC envelope: one row change with its before/after
// images, source metadata, and commit timestamp. The row images are deferred
// ([RawJSON]); project them with [BeforeAs] / [AfterAs] (or
// [ChangeEvent.Before] / [ChangeEvent.After] directly).
type ChangeEvent struct {
	// Operation is the kind of change (create, read/snapshot, update, delete, or
	// tombstone).
	Operation Operation
	// Before is the row image prior to the change ([RawJSON]); absent (nil) on a
	// create, a snapshot read, and a tombstone.
	Before RawJSON
	// After is the row image after the change ([RawJSON]); absent (nil) on a
	// delete and a tombstone.
	After RawJSON
	// Source is the decoded connector metadata block.
	Source SourceMetadata
	// Timestamp is the commit time the connector reported ("ts_ms"), in UTC; the
	// zero time when the envelope carried none.
	Timestamp time.Time
}

// envelope is the wire shape of a Debezium JSON change event. Images and the
// source block stay as json.RawMessage so the codec defers row decoding and
// surfaces only the metadata it understands.
type envelope struct {
	Op     string          `json:"op"`
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Source json.RawMessage `json:"source"`
	TsMS   *int64          `json:"ts_ms"`
}

// sourceBlock is the subset of the Debezium "source" block the codec surfaces.
// snapshot is decoded permissively (a bool or a string such as "true"/"last")
// across connector versions.
type sourceBlock struct {
	Connector string          `json:"connector"`
	Name      string          `json:"name"`
	Database  string          `json:"db"`
	Schema    string          `json:"schema"`
	Table     string          `json:"table"`
	Snapshot  json.RawMessage `json:"snapshot"`
	LSN       json.RawMessage `json:"lsn"`
	TxID      json.RawMessage `json:"txId"`
}

// Codec decodes a [source.Message] carrying a Debezium/OpenCDC JSON change-event
// envelope into a [ChangeEvent]. It holds no mutable state and is safe for
// concurrent use; the [source.Hopper] decodes from per-lane worker goroutines.
//
// Construct it with [New] and register it on a [source.Registry] (or set it as
// the default). It is the instance seam: there is no package-global
// registration.
type Codec struct{}

var _ source.Codec = Codec{}

// New returns a [Codec]. It is a constructor for symmetry with the rest of the
// suite and to leave room for future options; the zero value is equally valid
// since the codec carries no state.
func New() Codec { return Codec{} }

// Decode turns a message's bytes into a [ChangeEvent]. An empty payload is a
// log-compaction tombstone and decodes to a [ChangeEvent] with [OpTombstone].
// A non-empty payload must be a Debezium JSON envelope; a payload that is not a
// JSON object, or whose "op" is unrecognized, returns an error the
// [source.Registry] wraps in a [*source.DecodeError] (which reports
// [source.ErrPoison]).
//
// Headers are not consulted: the change-event shape is self-describing in the
// body. Recover the value with [EventOf] / [DecodeEvent] and project its row
// images with [BeforeAs] / [AfterAs].
func (Codec) Decode(data []byte, _ source.Headers) (any, error) {
	if len(data) == 0 {
		return ChangeEvent{Operation: OpTombstone}, nil
	}

	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return ChangeEvent{}, fmt.Errorf("%w: %v", ErrMalformedEnvelope, err)
	}

	op, ok := operationFromCode(env.Op)
	if !ok {
		return ChangeEvent{}, fmt.Errorf("%w: %q", ErrUnknownOperation, env.Op)
	}

	ev := ChangeEvent{
		Operation: op,
		Before:    rawImage(env.Before),
		After:     rawImage(env.After),
	}
	if env.TsMS != nil {
		ev.Timestamp = time.UnixMilli(*env.TsMS).UTC()
	}
	if len(env.Source) > 0 && !isJSONNull(env.Source) {
		meta, err := decodeSource(env.Source)
		if err != nil {
			return ChangeEvent{}, err
		}
		ev.Source = meta
	}
	return ev, nil
}

// decodeSource parses the source metadata block into a [SourceMetadata],
// retaining the raw block and normalizing the permissive snapshot/lsn/txId
// fields.
func decodeSource(raw json.RawMessage) (SourceMetadata, error) {
	var sb sourceBlock
	if err := json.Unmarshal(raw, &sb); err != nil {
		return SourceMetadata{}, fmt.Errorf("%w: source block: %v", ErrMalformedEnvelope, err)
	}
	return SourceMetadata{
		Connector: sb.Connector,
		Name:      sb.Name,
		Database:  sb.Database,
		Schema:    sb.Schema,
		Table:     sb.Table,
		Snapshot:  truthySnapshot(sb.Snapshot),
		LSN:       scalarString(sb.LSN),
		TxID:      scalarString(sb.TxID),
		Raw:       rawImage(raw),
	}, nil
}

// rawImage converts a json.RawMessage into a [RawJSON], returning nil for an
// absent or JSON-null image so [RawJSON.Present] reads correctly.
func rawImage(m json.RawMessage) RawJSON {
	if len(m) == 0 || isJSONNull(m) {
		return nil
	}
	out := make(RawJSON, len(m))
	copy(out, m)
	return out
}

// isJSONNull reports whether the raw message is the JSON null literal (allowing
// surrounding space).
func isJSONNull(m json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(m), []byte("null"))
}

// truthySnapshot decodes the permissive Debezium snapshot marker: a JSON bool,
// or a string such as "true", "last", "incremental" (anything but "false"/"" is
// truthy). An absent marker is false.
func truthySnapshot(m json.RawMessage) bool {
	if len(m) == 0 || isJSONNull(m) {
		return false
	}
	var b bool
	if err := json.Unmarshal(m, &b); err == nil {
		return b
	}
	var s string
	if err := json.Unmarshal(m, &s); err == nil {
		return s != "" && s != "false"
	}
	return false
}

// scalarString renders a permissive scalar (a JSON string or number) as a
// string, for LSN/TxID fields whose representation varies by connector. An
// absent or null value yields "".
func scalarString(m json.RawMessage) string {
	if len(m) == 0 || isJSONNull(m) {
		return ""
	}
	var s string
	if err := json.Unmarshal(m, &s); err == nil {
		return s
	}
	var n json.Number
	if err := json.Unmarshal(m, &n); err == nil {
		return n.String()
	}
	return ""
}
