package durable

import "errors"

// Sentinel errors a Store reports. Callers match them with errors.Is.
var (
	// ErrInstanceNotFound is reported by Store.Load for an instance that has
	// never been written.
	ErrInstanceNotFound = errors.New("crucible/durable: instance not found")

	// ErrStepOutOfOrder is reported by Store.Append when a Record's Step does not
	// strictly follow the instance's highest recorded Step (and is not an
	// idempotent re-append of an already-recorded Step).
	ErrStepOutOfOrder = errors.New("crucible/durable: record step out of order")

	// ErrCheckpointNotAdvancing is reported by Store.Checkpoint when throughStep
	// does not advance beyond the instance's current checkpoint.
	ErrCheckpointNotAdvancing = errors.New("crucible/durable: checkpoint does not advance")

	// ErrInstanceExists is reported by Runner.Start when the InstanceID is already
	// present in the Store, so a fresh Start does not clobber a recorded instance.
	ErrInstanceExists = errors.New("crucible/durable: instance already exists")
)
