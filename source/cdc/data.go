// SPDX-License-Identifier: Apache-2.0

package cdc

import (
	"github.com/stablekernel/crucible/source"
)

// Header keys under which [SourceHeaders] surfaces a change event's source
// metadata through the core [source.Headers], keeping CDC metadata legible as
// typed headers instead of a magic-string map.
const (
	// OperationHeader carries the change operation's code (see [Operation.String]).
	OperationHeader = "cdc-op"
	// ConnectorHeader carries the source connector name.
	ConnectorHeader = "cdc-connector"
	// DatabaseHeader carries the source database.
	DatabaseHeader = "cdc-database"
	// SchemaHeader carries the source schema.
	SchemaHeader = "cdc-schema"
	// TableHeader carries the source table.
	TableHeader = "cdc-table"
	// SnapshotHeader carries "true" when the record came from the initial
	// snapshot, and is omitted otherwise.
	SnapshotHeader = "cdc-snapshot"
	// LSNHeader carries the source log sequence number / position, when present.
	LSNHeader = "cdc-lsn"
	// TxIDHeader carries the source transaction id, when present.
	TxIDHeader = "cdc-txid"
)

// EventOf recovers the [ChangeEvent] a [Codec] decoded from a registry result.
// It is the typed bridge between [source.Registry.Decode] (which returns any)
// and a handler that works with change events: pass the decoded value, get back
// the event and whether the value was in fact a ChangeEvent.
//
// A false return means the registry routed the message to a different codec (the
// content type matched something other than this codec); it is not a decode
// failure.
func EventOf(v any) (ChangeEvent, bool) {
	e, ok := v.(ChangeEvent)
	return e, ok
}

// DecodeEvent decodes m through r and recovers the [ChangeEvent], the
// convenience path a CDC handler uses instead of calling [source.Registry.Decode]
// and [EventOf] in sequence. A decode failure returns the [*source.DecodeError]
// from the registry; a value that is not a ChangeEvent (some other codec matched)
// returns a *source.DecodeError wrapping [source.ErrPoison], since a payload that
// decoded to the wrong shape cannot be retried into the right one.
func DecodeEvent(r *source.Registry, m source.Message) (ChangeEvent, error) {
	v, err := r.Decode(m)
	if err != nil {
		return ChangeEvent{}, err
	}
	e, ok := EventOf(v)
	if !ok {
		return ChangeEvent{}, &source.DecodeError{
			Subject: m.Subject(),
			Err:     source.ErrPoison,
		}
	}
	return e, nil
}

// BeforeAs decodes a change event's before-image into a fresh value of type T:
// the typed view of the row prior to the change. It returns [ErrMissingImage]
// when the operation carries no before-image (a create, a snapshot read, a
// tombstone).
func BeforeAs[T any](e ChangeEvent) (T, error) {
	var v T
	if err := e.Before.As(&v); err != nil {
		return v, err
	}
	return v, nil
}

// AfterAs decodes a change event's after-image into a fresh value of type T: the
// typed view of the row after the change. It returns [ErrMissingImage] when the
// operation carries no after-image (a delete, a tombstone).
func AfterAs[T any](e ChangeEvent) (T, error) {
	var v T
	if err := e.After.As(&v); err != nil {
		return v, err
	}
	return v, nil
}

// SourceHeaders surfaces a change event's operation and source metadata as core
// [source.Headers], so a handler reads CDC metadata through the same typed-header
// surface as any other inbound metadata. Only non-empty fields are emitted; the
// snapshot header appears only when the record came from a snapshot. The result
// is a fresh slice in a stable order the caller may retain.
func SourceHeaders(e ChangeEvent) source.Headers {
	headers := make(source.Headers, 0, 8)
	headers = append(headers, source.Header{Key: OperationHeader, Value: e.Operation.String()})
	appendNonEmpty(&headers, ConnectorHeader, e.Source.Connector)
	appendNonEmpty(&headers, DatabaseHeader, e.Source.Database)
	appendNonEmpty(&headers, SchemaHeader, e.Source.Schema)
	appendNonEmpty(&headers, TableHeader, e.Source.Table)
	if e.Source.Snapshot {
		headers = append(headers, source.Header{Key: SnapshotHeader, Value: "true"})
	}
	appendNonEmpty(&headers, LSNHeader, e.Source.LSN)
	appendNonEmpty(&headers, TxIDHeader, e.Source.TxID)
	return headers
}

// appendNonEmpty appends a header only when its value is non-empty, so absent
// metadata never produces a blank header that shadows a real one.
func appendNonEmpty(h *source.Headers, key, value string) {
	if value == "" {
		return
	}
	*h = append(*h, source.Header{Key: key, Value: value})
}
