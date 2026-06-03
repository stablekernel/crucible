// SPDX-License-Identifier: Apache-2.0

// Package schema provides a validating [source.Middleware] for the crucible source
// seam: it checks a message against a caller-supplied [Validator] before the
// handler runs, and rejects an invalid message as poison ([source.Term] with a
// [SchemaError], which reports [source.ErrPoison]) so it routes straight to
// dead-letter instead of wasting redeliveries.
//
// The package defines the [Validator] interface and a simple [ContentTypeValidator]
// example; real validators (proto, Avro, JSON-Schema, a CloudEvents schema
// registry) are caller-provided so the core takes on no schema-library dependency.
package schema

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/source"
)

// Validator decides whether a message is well-formed before it reaches the
// handler. A nil error means valid; a non-nil error rejects the message as
// poison. Implementations must be safe for concurrent use; the Hopper validates
// from several lanes at once.
type Validator interface {
	// Validate checks m and returns nil if it conforms, or an error describing why
	// it does not. The middleware wraps a non-nil error in a [SchemaError].
	Validate(ctx context.Context, m source.Message) error
}

// ValidatorFunc adapts a function to the [Validator] interface.
type ValidatorFunc func(ctx context.Context, m source.Message) error

// Validate implements [Validator].
func (f ValidatorFunc) Validate(ctx context.Context, m source.Message) error { return f(ctx, m) }

// SchemaError wraps a validation failure with the subject the message arrived on
// and the underlying reason. It is errors.Is / errors.As friendly via Unwrap and
// reports [source.ErrPoison] from Is, so a schema rejection is recognized as
// poison (and dead-lettered) with a single errors.Is(err, source.ErrPoison) check
// — never match on its Error string. It mirrors the shape of source.DecodeError.
type SchemaError struct {
	// Subject is the topic or subject the invalid message arrived on.
	Subject string
	// Err is the wrapped underlying validation error.
	Err error
}

// Error implements error.
func (e *SchemaError) Error() string {
	return fmt.Sprintf("source/schema: validation failed for subject %q: %v", e.Subject, e.Err)
}

// Unwrap returns the wrapped validation error so errors.As reaches it.
func (e *SchemaError) Unwrap() error { return e.Err }

// Is reports a *SchemaError as matching [source.ErrPoison], so middleware can
// route a schema rejection to dead-letter as poison without string-matching.
func (e *SchemaError) Is(target error) bool { return target == source.ErrPoison }

// Middleware returns a [source.Middleware] that validates each message with v
// before invoking the wrapped handler. A message that fails validation is
// rejected with [source.Term] carrying a [SchemaError] (classified
// [source.Poison]); the handler is never invoked. A valid message flows to the
// handler unchanged. A nil validator makes the middleware a pass-through.
func Middleware(v Validator) source.Middleware {
	return func(next source.Handler) source.Handler {
		if v == nil {
			return next
		}
		return func(ctx context.Context, m source.Message) source.Result {
			if err := v.Validate(ctx, m); err != nil {
				return source.Term(&SchemaError{Subject: m.Subject(), Err: err})
			}
			return next(ctx, m)
		}
	}
}

// ContentTypeValidator is a simple example [Validator]: it requires a message to
// carry a content-type header whose value is in an allow-list, and (optionally)
// to have a non-empty payload. It is intended as a starting point and a test
// double, not a substitute for a real schema validator.
type ContentTypeValidator struct {
	// Header is the header key the content type is read from. If empty,
	// [DefaultContentTypeHeader] is used.
	Header string
	// Allowed is the set of acceptable content-type values. An empty set accepts
	// any present content type.
	Allowed []string
	// RequireValue rejects a message with an empty [source.Message.Value] when true.
	RequireValue bool
}

// DefaultContentTypeHeader is the header key [ContentTypeValidator] reads when
// its Header field is empty.
const DefaultContentTypeHeader = "content-type"

// Validate implements [Validator]: it checks the content-type header is present
// and (when an allow-list is set) permitted, and that the payload is non-empty
// when RequireValue is set.
func (v ContentTypeValidator) Validate(_ context.Context, m source.Message) error {
	if v.RequireValue && len(m.Value()) == 0 {
		return fmt.Errorf("empty payload")
	}
	key := v.Header
	if key == "" {
		key = DefaultContentTypeHeader
	}
	ct, ok := m.Headers().Get(key)
	if !ok || ct == "" {
		return fmt.Errorf("missing %q header", key)
	}
	if len(v.Allowed) == 0 {
		return nil
	}
	for _, a := range v.Allowed {
		if ct == a {
			return nil
		}
	}
	return fmt.Errorf("content-type %q not allowed", ct)
}

var _ Validator = ContentTypeValidator{}
