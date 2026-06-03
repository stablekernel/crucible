// SPDX-License-Identifier: Apache-2.0

// Package cloudevents is a source codec that decodes inbound messages into
// CloudEvents. It plugs into a source.Registry as an instance-scoped
// [source.Codec]: construct one with [New] and register it under the content
// types you accept; there is no global format registration (the CloudEvents
// SDK's package-level format registry is deliberately not used, so two codecs
// in one process never share mutable state).
//
// # Content modes
//
// The CloudEvents spec defines two ways an event rides a transport, and this
// codec accepts both, selecting between them by the message's content type:
//
//   - Structured mode: the entire event — attributes and data — is one JSON
//     document in the body, carried under "application/cloudevents+json".
//   - Binary mode: the event's attributes ride as "ce-"-prefixed headers and
//     the data is the raw body, with the body's own media type in the
//     "datacontenttype" header (or the message's content-type).
//
// A content type whose media type begins with "application/cloudevents" (the
// structured prefix) decodes as structured; anything else decodes as binary.
// See [Detect].
//
// # Decoded value
//
// Decode yields a [cloudevents.Event] (the SDK's canonical event). Recover it
// from a handler with [EventOf], and decode its data payload into a concrete
// type with [DataAs] or the generic [DecodeData] helper. Extension attributes
// are surfaced through the core [source.Headers] (see [Extensions]) rather than
// a magic-string map, so a handler reads them the same way it reads any other
// inbound metadata.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package cloudevents

import (
	"fmt"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/stablekernel/crucible/source"
)

// StructuredContentType is the media type that carries a CloudEvent in
// structured mode: attributes and data together as one JSON document. Register
// the [Codec] under this type to decode structured events.
const StructuredContentType = "application/cloudevents+json"

// structuredPrefix is the media-type prefix the CloudEvents spec reserves for
// structured content modes ("application/cloudevents..."). Any content type
// whose media type starts with it is structured; everything else is binary.
const structuredPrefix = "application/cloudevents"

// binaryAttrPrefix is the header-name prefix that carries CloudEvents context
// attributes in binary mode. The match is case-insensitive: inlets normalize
// backend casing differently, and the spec's transport bindings disagree on
// case (HTTP uses "ce-", Kafka "ce_"-or-"ce-"), so the codec accepts any
// casing and the "_"/"-" separator alike.
const binaryAttrPrefix = "ce-"

// dataContentTypeHeader is the binary-mode header that names the media type of
// the data payload, mirroring the CloudEvents "datacontenttype" attribute. When
// absent, the codec falls back to the message's [source.ContentTypeHeader].
const dataContentTypeHeader = "datacontenttype"

// ExtensionHeaderPrefix is the key prefix under which [Extensions] surfaces a
// decoded event's extension attributes through the core [source.Headers]. An
// extension named "traceparent" is exposed as the header
// "ce-ext-traceparent", keeping extensions legible as typed headers instead of
// a separate magic-string map.
const ExtensionHeaderPrefix = "ce-ext-"

// Mode is the CloudEvents content mode a message arrived in.
type Mode int

const (
	// Binary is the content mode in which context attributes ride as headers
	// and the data is the raw body.
	Binary Mode = iota
	// Structured is the content mode in which the whole event is one JSON
	// document in the body.
	Structured
)

// String renders the mode for logs and diagnostics.
func (m Mode) String() string {
	switch m {
	case Structured:
		return "structured"
	case Binary:
		return "binary"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// Detect reports the CloudEvents content mode for a content type. A media type
// beginning with the structured prefix ("application/cloudevents") is
// [Structured]; everything else, including the empty string, is [Binary]. Any
// parameters after a ";" (charset, for one) are ignored.
func Detect(contentType string) Mode {
	media := contentType
	if i := strings.IndexByte(media, ';'); i >= 0 {
		media = media[:i]
	}
	media = strings.ToLower(strings.TrimSpace(media))
	if strings.HasPrefix(media, structuredPrefix) {
		return Structured
	}
	return Binary
}

// Codec decodes a [source.Message] into a [cloudevents.Event], handling both
// CloudEvents content modes. It holds no mutable state and is safe for
// concurrent use; the [source.Hopper] decodes from per-lane worker goroutines.
//
// Construct it with [New] and register it on a [source.Registry] (or set it as
// the default). It is the instance seam — there is no package-global
// registration.
type Codec struct{}

var _ source.Codec = Codec{}

// New returns a [Codec]. It is a constructor for symmetry with the rest of the
// suite and to leave room for future options; the zero value is equally valid
// since the codec carries no state.
func New() Codec { return Codec{} }

// Decode turns a message's bytes and headers into a [cloudevents.Event],
// selecting the content mode from the content-type header via [Detect]. The
// returned value is a cloudevents.Event (by value); recover it with [EventOf]
// and read its data with [DataAs] or [DecodeData].
//
// A malformed structured payload, a binary event missing a required attribute,
// or an event that fails CloudEvents validation returns an error the
// [source.Registry] wraps in a [*source.DecodeError] (which reports
// [source.ErrPoison]): a structurally invalid event cannot be retried into
// validity.
func (Codec) Decode(data []byte, h source.Headers) (any, error) {
	ct, _ := h.Get(source.ContentTypeHeader)
	switch Detect(ct) {
	case Structured:
		return decodeStructured(data)
	default:
		return decodeBinary(data, h, ct)
	}
}

// decodeStructured parses a whole-event JSON document into a cloudevents.Event
// and validates it.
func decodeStructured(data []byte) (cloudevents.Event, error) {
	var e cloudevents.Event
	if err := e.UnmarshalJSON(data); err != nil {
		return cloudevents.Event{}, fmt.Errorf("cloudevents: parse structured event: %w", err)
	}
	if err := e.Validate(); err != nil {
		return cloudevents.Event{}, fmt.Errorf("cloudevents: invalid structured event: %w", err)
	}
	return e, nil
}

// decodeBinary reconstructs a cloudevents.Event from "ce-"-prefixed headers and
// a raw data body. ctFallback is the message's content-type header, used as the
// data media type when no "datacontenttype" header is present.
func decodeBinary(data []byte, h source.Headers, ctFallback string) (cloudevents.Event, error) {
	e := cloudevents.New(cloudevents.CloudEventsVersionV1)
	dataContentType := ctFallback

	for _, hd := range h {
		name, ok := trimAttrPrefix(hd.Key)
		if !ok {
			if strings.EqualFold(hd.Key, dataContentTypeHeader) {
				dataContentType = hd.Value
			}
			continue
		}
		if name == dataContentTypeHeader {
			dataContentType = hd.Value
			continue
		}
		if err := applyAttribute(&e, name, hd.Value); err != nil {
			return cloudevents.Event{}, err
		}
	}

	if len(data) > 0 {
		if err := e.SetData(dataContentType, data); err != nil {
			return cloudevents.Event{}, fmt.Errorf("cloudevents: set binary data: %w", err)
		}
	}
	if err := e.Validate(); err != nil {
		return cloudevents.Event{}, fmt.Errorf("cloudevents: invalid binary event: %w", err)
	}
	return e, nil
}

// trimAttrPrefix reports whether key carries the binary attribute prefix and,
// if so, returns the lowercased attribute name with the prefix removed. It
// accepts both the "ce-" and "ce_" separators across transport bindings.
func trimAttrPrefix(key string) (string, bool) {
	if len(key) < len(binaryAttrPrefix) {
		return "", false
	}
	if !strings.EqualFold(key[:len(binaryAttrPrefix)-1], "ce") {
		return "", false
	}
	sep := key[len(binaryAttrPrefix)-1]
	if sep != '-' && sep != '_' {
		return "", false
	}
	return strings.ToLower(key[len(binaryAttrPrefix):]), true
}

// applyAttribute sets a single context attribute on e by its lowercased name.
// Well-known spec attributes go through their typed setter; anything else is an
// extension attribute. A blank value is ignored so an empty header never
// shadows a required attribute into existence. A "time" header that is not a
// valid RFC3339 timestamp returns an error.
func applyAttribute(e *cloudevents.Event, name, value string) error {
	if value == "" {
		return nil
	}
	switch name {
	case "specversion":
		e.SetSpecVersion(value)
	case "id":
		e.SetID(value)
	case "source":
		e.SetSource(value)
	case "type":
		e.SetType(value)
	case "subject":
		e.SetSubject(value)
	case "dataschema":
		e.SetDataSchema(value)
	case "time":
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return fmt.Errorf("cloudevents: parse time attribute %q: %w", value, err)
		}
		e.SetTime(t)
	default:
		e.SetExtension(name, value)
	}
	return nil
}
