// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"time"
)

// Handler processes one decoded message and returns a [Result] describing how
// the [Hopper] should settle it. A handler performs the work; it does not ack,
// nak, or commit — returning the Result is how it asks the engine to. Handlers
// must be safe for concurrent use: the Hopper invokes them from per-lane worker
// goroutines.
type Handler func(ctx context.Context, m Message) Result

// Action is the disposition the Hopper applies to a message after its handler
// returns. It maps onto each backend's native settle vocabulary.
type Action uint8

const (
	// ActionAck reports success: the message is acknowledged (JetStream Ack;
	// Kafka offset marked for commit). This is the zero value, so a Result{}
	// means "ack".
	ActionAck Action = iota
	// ActionNak asks for redelivery: the message failed transiently and should
	// be tried again (JetStream Nak/NakWithDelay; Kafka declines to commit so the
	// record is re-read). Result.Requeue is an optional backoff delay.
	ActionNak
	// ActionTerm rejects the message permanently: it must not be redelivered
	// (JetStream Term; Kafka routes it to a dead-letter topic, then commits).
	ActionTerm
	// ActionInProgress reports the handler needs more time, extending the ack
	// deadline without settling (JetStream InProgress). A no-op on backends with
	// no ack deadline.
	ActionInProgress
	// ActionManual reports the handler already settled the message itself through
	// Message.As and the backend client; the Hopper takes no settle action.
	ActionManual
)

// String renders the action for logs and metrics attributes.
func (a Action) String() string {
	switch a {
	case ActionAck:
		return "ack"
	case ActionNak:
		return "nak"
	case ActionTerm:
		return "term"
	case ActionInProgress:
		return "in_progress"
	case ActionManual:
		return "manual"
	default:
		return "unknown"
	}
}

// Classification tags why a handler failed, so middleware (retry, dead-letter)
// can route without string-matching errors. It is orthogonal to [Action]: an
// ActionNak is usually Retryable, an ActionTerm usually Poison or
// InvalidForState.
type Classification uint8

const (
	// Unclassified is the zero value: no specific class was reported.
	Unclassified Classification = iota
	// Retryable marks a transient failure worth retrying (a timeout, a
	// connection blip). Retry middleware backs off and re-delivers.
	Retryable
	// Poison marks a permanently-bad message (an undecodable payload, a failed
	// invariant) that retrying cannot fix. It routes straight to dead-letter.
	Poison
	// InvalidForState marks a message that is well-formed but not legal for the
	// target's current state (a guard rejection from the state-machine bridge).
	// Distinct from Poison so consumers can treat "wrong time" differently from
	// "wrong message".
	InvalidForState
	// Drop marks a message to discard silently (a duplicate, an out-of-scope
	// event): acked, not retried, not dead-lettered.
	Drop
)

// String renders the classification for logs and metrics attributes.
func (c Classification) String() string {
	switch c {
	case Retryable:
		return "retryable"
	case Poison:
		return "poison"
	case InvalidForState:
		return "invalid_for_state"
	case Drop:
		return "drop"
	default:
		return "unclassified"
	}
}

// Result is what a [Handler] returns: the [Action] the Hopper should apply, an
// optional [Classification] of the failure, an optional redelivery delay, and
// the underlying error. The zero Result is a successful ack, so a handler that
// returns Result{} (or uses [Ack]) acknowledges the message.
type Result struct {
	// Action is the disposition to apply. The zero value is ActionAck.
	Action Action
	// Class classifies a failure for middleware routing. Ignored for ActionAck.
	Class Classification
	// Requeue is an optional backoff before redelivery, honored on backends that
	// support delayed nak (JetStream); best-effort elsewhere.
	Requeue time.Duration
	// Err is the underlying error, surfaced to telemetry and dead-letter
	// metadata. Nil for a successful ack.
	Err error
}

// Ack returns a successful Result: the message is acknowledged.
func Ack() Result { return Result{Action: ActionAck} }

// Nak returns a Result asking for immediate redelivery, classified Retryable.
func Nak(err error) Result { return Result{Action: ActionNak, Class: Retryable, Err: err} }

// NakAfter returns a Result asking for redelivery after delay d, classified
// Retryable. The delay is honored where the backend supports it.
func NakAfter(d time.Duration, err error) Result {
	return Result{Action: ActionNak, Class: Retryable, Requeue: d, Err: err}
}

// Term returns a Result rejecting the message permanently (dead-letter),
// classified Poison.
func Term(err error) Result { return Result{Action: ActionTerm, Class: Poison, Err: err} }

// Reject returns a Result rejecting the message as invalid for the target's
// current state, classified InvalidForState. It dead-letters like Term but
// carries the distinct class so a "wrong time" rejection is legible.
func Reject(err error) Result {
	return Result{Action: ActionTerm, Class: InvalidForState, Err: err}
}

// Skip returns a Result that acknowledges and discards the message, classified
// Drop: used for duplicates or out-of-scope events that should neither retry nor
// dead-letter.
func Skip() Result { return Result{Action: ActionAck, Class: Drop} }

// InProgress returns a Result that extends the ack deadline without settling.
func InProgress() Result { return Result{Action: ActionInProgress} }

// Manual returns a Result reporting the handler settled the message itself.
func Manual() Result { return Result{Action: ActionManual} }

// Failed reports whether the result carries a failure (anything that is not a
// plain ack/skip).
func (r Result) Failed() bool {
	return r.Action == ActionNak || r.Action == ActionTerm
}
