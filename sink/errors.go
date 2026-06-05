// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"errors"
	"fmt"
)

// ErrUnregistered reports that an outlet has no transformer registered for a
// payload's concrete type. The Manifold treats it as a silent skip — not a
// failure — so attaching an outlet that only handles some payload types is
// normal and uncounted. Match it with errors.Is.
var ErrUnregistered = errors.New("sink: no transformer registered for payload type")

// Phase names the stage of an outlet's work that failed, for diagnostics.
type Phase string

const (
	// PhaseTransform marks a failure turning a payload into a destination
	// operation. Registry transformers cannot themselves return an error, so the
	// built-in Emitter never emits this phase today; it is reserved for outlets
	// whose payload-to-operation step can fail and want to classify it.
	PhaseTransform Phase = "transform"
	// PhaseApply marks a failure applying the operation to the destination
	// client (the write itself).
	PhaseApply Phase = "apply"
	// PhaseFlush marks a failure flushing a buffered outlet.
	PhaseFlush Phase = "flush"
)

// Error wraps a destination failure with structured context: which outlet
// failed, during which Phase, for which payload type, and the underlying error.
// It is errors.Is / errors.As friendly via Unwrap; never match on its Error
// string.
type Error struct {
	// Outlet is the name of the outlet that failed.
	Outlet string
	// Phase is the stage of work that failed.
	Phase Phase
	// PayloadType is the concrete type name of the payload being sunk.
	PayloadType string
	// Err is the wrapped underlying error.
	Err error
}

// Error implements error.
func (e *Error) Error() string {
	return fmt.Sprintf("sink: outlet %q failed during %s for %s: %v",
		e.Outlet, e.Phase, e.PayloadType, e.Err)
}

// Unwrap returns the wrapped error so errors.Is / errors.As see through it.
func (e *Error) Unwrap() error { return e.Err }
