// SPDX-License-Identifier: Apache-2.0

package source

import (
	"encoding/json"
	"sync"
)

// ContentTypeHeader is the header key a [Registry] reads to select a [Codec] for
// a message. An inlet that carries a content type maps its backend's native
// header onto this key; when it is absent or unmatched, the registry falls back
// to its default codec.
const ContentTypeHeader = "content-type"

// Codec decodes a raw message payload into a domain value. It is the instance
// seam for turning bytes on the wire into the value a [Handler] works with;
// there is no package-global codec registration (the global-registry
// anti-pattern is deliberately avoided), so every Codec is constructed and
// registered into a [Registry] that is injected, never shared by import.
//
// Implementations must be safe for concurrent use: the [Hopper] decodes from
// per-lane worker goroutines.
type Codec interface {
	// Decode turns raw bytes and their headers into a domain value. A failure is
	// reported plainly; the Hopper wraps it in a [*DecodeError] with the selecting
	// content type and subject before routing the message to dead-letter.
	Decode(data []byte, h Headers) (any, error)
}

// CodecFunc adapts a plain function to a [Codec].
type CodecFunc func(data []byte, h Headers) (any, error)

// Decode calls the underlying function.
func (f CodecFunc) Decode(data []byte, h Headers) (any, error) { return f(data, h) }

// Registry maps a content type to a [Codec], with an optional default for
// messages that carry no content type or one the registry does not know. There
// is no package-level registry and no init-time state: every Registry is
// constructed with NewRegistry and injected. It is safe for concurrent Register,
// SetDefault, and Decode.
type Registry struct {
	mu       sync.RWMutex
	codecs   map[string]Codec
	fallback Codec
}

// NewRegistry returns an empty Registry with no default codec. Register codecs
// by content type and optionally SetDefault before use, or pass it to
// [WithRegistry]; a registry with a single default codec behaves like an
// always-decode-this-way pipeline.
func NewRegistry() *Registry {
	return &Registry{codecs: make(map[string]Codec)}
}

// Register binds contentType to codec, overwriting any prior codec for that
// type. It returns the registry for chaining.
func (r *Registry) Register(contentType string, codec Codec) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.codecs[contentType] = codec
	return r
}

// SetDefault sets the codec used when a message carries no content type, or one
// no registered codec matches. It returns the registry for chaining.
func (r *Registry) SetDefault(codec Codec) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fallback = codec
	return r
}

// codecFor resolves the codec for the given headers: the one keyed to the
// content-type header if present and registered, else the default. It reports
// the resolved content type (for [*DecodeError]) and whether a codec was found.
func (r *Registry) codecFor(h Headers) (Codec, string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if ct, ok := h.Get(ContentTypeHeader); ok {
		if c, ok := r.codecs[ct]; ok {
			return c, ct, true
		}
		if r.fallback != nil {
			return r.fallback, ct, true
		}
		return nil, ct, false
	}
	if r.fallback != nil {
		return r.fallback, "", true
	}
	return nil, "", false
}

// Decode resolves a codec for m's headers and decodes its value. A resolution
// miss returns a [*DecodeError] wrapping [ErrNoCodec]; a codec failure returns a
// *DecodeError wrapping the codec's error. Both report ErrPoison via errors.Is.
func (r *Registry) Decode(m Message) (any, error) {
	codec, ct, ok := r.codecFor(m.Headers())
	if !ok {
		return nil, &DecodeError{ContentType: ct, Subject: m.Subject(), Err: ErrNoCodec}
	}
	v, err := codec.Decode(m.Value(), m.Headers())
	if err != nil {
		return nil, &DecodeError{ContentType: ct, Subject: m.Subject(), Err: err}
	}
	return v, nil
}

// JSONCodec is a built-in [Codec] that decodes a JSON payload into a fresh value
// of type T. It is the zero-dependency default: register it under
// "application/json" or as the registry default.
//
// Construct it with [NewJSONCodec]; the zero value works too, since it holds no
// state.
type JSONCodec[T any] struct{}

// NewJSONCodec returns a [JSONCodec] that decodes payloads into values of type T.
func NewJSONCodec[T any]() JSONCodec[T] { return JSONCodec[T]{} }

// Decode unmarshals data as JSON into a new T and returns it. Headers are
// ignored; JSON carries its own shape. A malformed payload returns the
// json.Unmarshal error, which the [Registry] wraps in a [*DecodeError].
func (JSONCodec[T]) Decode(data []byte, _ Headers) (any, error) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// DecodeTyped decodes m through r and asserts the result to T, the generic
// helper a typed handler uses to recover a concrete value from the registry. A
// decode failure returns the [*DecodeError] from the registry; a type mismatch
// (the codec produced some other type) returns a *DecodeError wrapping
// [ErrPoison], since a payload that decodes to the wrong shape cannot be retried
// into the right one.
func DecodeTyped[T any](r *Registry, m Message) (T, error) {
	var zero T
	v, err := r.Decode(m)
	if err != nil {
		return zero, err
	}
	t, ok := v.(T)
	if !ok {
		return zero, &DecodeError{
			Subject: m.Subject(),
			Err:     ErrPoison,
		}
	}
	return t, nil
}
