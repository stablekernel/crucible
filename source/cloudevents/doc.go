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
