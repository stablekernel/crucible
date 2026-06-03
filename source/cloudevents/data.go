// SPDX-License-Identifier: Apache-2.0

package cloudevents

import (
	"fmt"
	"sort"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/stablekernel/crucible/source"
)

// EventOf recovers the [cloudevents.Event] a [Codec] decoded from a registry
// result. It is the typed bridge between [source.Registry.Decode] (which
// returns any) and a handler that works with CloudEvents: pass the decoded
// value, get back the event and whether the value was in fact a CloudEvent.
//
// A false return means the registry routed the message to a different codec
// (the content type matched something other than this codec); it is not a
// decode failure.
func EventOf(v any) (cloudevents.Event, bool) {
	e, ok := v.(cloudevents.Event)
	return e, ok
}

// DecodeEvent decodes m through r and recovers the [cloudevents.Event],
// the convenience path a CloudEvents handler uses instead of calling
// [source.Registry.Decode] and [EventOf] in sequence. A decode failure returns
// the [*source.DecodeError] from the registry; a value that is not a
// CloudEvent (some other codec matched) returns a *source.DecodeError wrapping
// [source.ErrPoison], since a payload that decoded to the wrong shape cannot be
// retried into the right one.
func DecodeEvent(r *source.Registry, m source.Message) (cloudevents.Event, error) {
	v, err := r.Decode(m)
	if err != nil {
		return cloudevents.Event{}, err
	}
	e, ok := EventOf(v)
	if !ok {
		return cloudevents.Event{}, &source.DecodeError{
			Subject: m.Subject(),
			Err:     source.ErrPoison,
		}
	}
	return e, nil
}

// DataAs decodes the event's data payload into out, honoring the event's data
// content type (JSON, for the common case). It is a thin pass-through to the
// SDK's typed-data decode, named for symmetry with the rest of the codec
// surface. A decode failure is returned plainly; a caller routing through a
// [Codec] will already have a valid event, so a failure here is a data-shape
// mismatch the caller decides how to classify.
func DataAs(e cloudevents.Event, out any) error {
	if err := e.DataAs(out); err != nil {
		return fmt.Errorf("cloudevents: decode data: %w", err)
	}
	return nil
}

// DecodeData decodes m through r into a [cloudevents.Event] and then decodes
// that event's data payload into a fresh value of type T: the one-call typed
// path a handler uses to go from raw message to typed CloudEvents data. A
// decode failure (of the event or its data) returns an error; the event-level
// failure is the registry's [*source.DecodeError], and a data-shape failure is
// wrapped with context.
func DecodeData[T any](r *source.Registry, m source.Message) (cloudevents.Event, T, error) {
	var zero T
	e, err := DecodeEvent(r, m)
	if err != nil {
		return cloudevents.Event{}, zero, err
	}
	var data T
	if err := DataAs(e, &data); err != nil {
		return e, zero, err
	}
	return e, data, nil
}

// Extensions surfaces an event's extension attributes as core
// [source.Headers], each key prefixed with [ExtensionHeaderPrefix], so a
// handler reads CloudEvents extensions through the same typed-header surface as
// any other inbound metadata instead of a separate magic-string map. Values are
// rendered to their string form; non-string extensions use fmt's default
// rendering. The result is a fresh slice in stable (sorted-by-name) order the
// caller may retain.
func Extensions(e cloudevents.Event) source.Headers {
	ext := e.Extensions()
	if len(ext) == 0 {
		return nil
	}
	names := make([]string, 0, len(ext))
	for name := range ext {
		names = append(names, name)
	}
	sort.Strings(names)

	headers := make(source.Headers, 0, len(ext))
	for _, name := range names {
		headers = append(headers, source.Header{
			Key:   ExtensionHeaderPrefix + name,
			Value: fmt.Sprintf("%v", ext[name]),
		})
	}
	return headers
}
