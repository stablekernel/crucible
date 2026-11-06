package durable

import "errors"

// Sentinel error text. The Err values below and ErrStepOutOfRange build on it.
const (
	errorPrefix = "durable: "

	errorInstanceNotFound       = "instance not found"
	errorStepOutOfOrder         = "record step out of order"
	errorStepConflict           = "step already recorded with a different record"
	errorFenced                 = "write fenced by a newer token"
	errorCheckpointNotAdvancing = "checkpoint does not advance"
	errorInstanceExists         = "instance already exists"
	errorEffectDispatch         = "effect dispatch failed"
	errorStepOutOfRange         = "time-travel step out of range"
)

// Sentinel errors a Store reports. Callers match them with errors.Is.
var (
	// ErrInstanceNotFound is reported by Store.Load for an instance that has
	// never been written.
	ErrInstanceNotFound = errors.New(errorPrefix + errorInstanceNotFound)

	// ErrStepOutOfOrder is reported by Store.Append when a Record's Step does not
	// strictly follow the instance's highest recorded Step (and is not an
	// idempotent re-append of an already-recorded Step).
	ErrStepOutOfOrder = errors.New(errorPrefix + errorStepOutOfOrder)

	// ErrStepConflict is reported by Store.Append when a Step is already recorded
	// with a different Record: the writer's view of the instance has diverged from
	// the journal, usually because another writer got there first. The Record was
	// not written.
	ErrStepConflict = errors.New(errorPrefix + errorStepConflict)

	// ErrFenced is reported by Store.Append and Store.Checkpoint for a write whose
	// fence token is lower than the highest the Store has seen for the instance:
	// a newer owner has taken the instance over. Nothing is written.
	ErrFenced = errors.New(errorPrefix + errorFenced)

	// ErrCheckpointNotAdvancing is reported by Store.Checkpoint when throughStep
	// does not advance beyond the instance's current checkpoint.
	ErrCheckpointNotAdvancing = errors.New(errorPrefix + errorCheckpointNotAdvancing)

	// ErrInstanceExists is reported by Runner.Start when the InstanceID is already
	// present in the Store, so a fresh Start does not clobber a recorded instance.
	ErrInstanceExists = errors.New(errorPrefix + errorInstanceExists)

	// ErrEffectDispatch wraps a failure returned by the caller-supplied effect
	// handler (WithEffectHandler) while applying an emitted effect. The effect was
	// persisted (write-ahead) but not marked dispatched, so a later recovery
	// retries it; callers match this with errors.Is to distinguish a dispatch
	// failure from a kernel transition or persistence error.
	ErrEffectDispatch = errors.New(errorPrefix + errorEffectDispatch)
)
