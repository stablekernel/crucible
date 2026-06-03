// SPDX-License-Identifier: Apache-2.0

package source

import (
	"errors"
	"fmt"
)

// ErrDrained reports that a [Subscription] has been closed and every delivered
// message settled: there is nothing more to consume. A [Subscription.Next] that
// returns ErrDrained tells the [Hopper] its fetch loop is done and to exit
// cleanly. Match it with errors.Is.
var ErrDrained = errors.New("source: subscription drained")

// ErrNoCodec reports that a [Registry] holds no [Codec] able to decode a
// message — neither one keyed to the message's content type nor a default. The
// Hopper treats it as a poison condition: an undecodable payload cannot be
// retried into legibility, so the message is terminated, not re-delivered. Match
// it with errors.Is.
var ErrNoCodec = errors.New("source: no codec for message")

// ErrPoison classifies a permanently-bad message that retrying cannot fix (an
// undecodable payload, a violated invariant). It is the sentinel a handler or
// middleware matches against with errors.Is to recognize a poison failure
// regardless of the concrete error wrapping it; the [Poison] classification is
// its [Classification] counterpart.
var ErrPoison = errors.New("source: poison message")

// ErrRetryable classifies a transient failure worth retrying (a timeout, a
// connection blip). It is the errors.Is-matchable sentinel for the [Retryable]
// classification, so retry middleware can recognize a retryable failure by
// errors.Is rather than by string-matching the underlying error.
var ErrRetryable = errors.New("source: retryable error")

// ErrInvalidForState classifies a well-formed message that is not legal for its
// target's current state — a guard rejection from the state-machine bridge. It
// is the errors.Is-matchable sentinel for the [InvalidForState] classification,
// distinct from [ErrPoison] so "wrong time" is legible separately from "wrong
// message". A [GuardRejection] unwraps to it.
var ErrInvalidForState = errors.New("source: event invalid for current state")

// DecodeError wraps a codec failure with the content type that selected the
// codec and the underlying error. It is errors.Is / errors.As friendly via
// Unwrap and reports ErrPoison from Is, so a decode failure is recognized as
// poison without string-matching; never match on its Error string.
type DecodeError struct {
	// ContentType is the content type (or header value) that selected the codec,
	// or "" when the default codec was used.
	ContentType string
	// Subject is the topic or subject the undecodable message arrived on.
	Subject string
	// Err is the wrapped underlying codec error.
	Err error
}

// Error implements error.
func (e *DecodeError) Error() string {
	if e.ContentType == "" {
		return fmt.Sprintf("source: decode failed for subject %q: %v", e.Subject, e.Err)
	}
	return fmt.Sprintf("source: decode failed for content-type %q on subject %q: %v",
		e.ContentType, e.Subject, e.Err)
}

// Unwrap returns the wrapped codec error so errors.As reaches it.
func (e *DecodeError) Unwrap() error { return e.Err }

// Is reports a *DecodeError as matching [ErrPoison], so middleware can route a
// decode failure to dead-letter with a single errors.Is(err, ErrPoison) check.
func (e *DecodeError) Is(target error) bool { return target == ErrPoison }

// GuardRejection wraps a guard/Assay rejection: a well-formed event that is not
// legal for the target's current state. It is errors.Is / errors.As friendly via
// Unwrap and reports ErrInvalidForState from Is, so a guard rejection is
// recognized as state-invalid (and routed to dead-letter as a distinct, "wrong
// time" outcome) without string-matching; never match on its Error string.
type GuardRejection struct {
	// Event names the inbound event that was rejected, for diagnostics.
	Event string
	// State names the target's current state at rejection time.
	State string
	// Err is the wrapped underlying rejection error.
	Err error
}

// Error implements error.
func (e *GuardRejection) Error() string {
	return fmt.Sprintf("source: event %q invalid for state %q: %v", e.Event, e.State, e.Err)
}

// Unwrap returns the wrapped rejection error so errors.As reaches it.
func (e *GuardRejection) Unwrap() error { return e.Err }

// Is reports a *GuardRejection as matching [ErrInvalidForState], so middleware
// can route a guard rejection with a single errors.Is(err, ErrInvalidForState)
// check.
func (e *GuardRejection) Is(target error) bool { return target == ErrInvalidForState }
