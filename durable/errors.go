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
	errorEventDataEncoding      = "event data can't be recorded"
	errorRecordTooLarge         = "record too large for the journal"
	errorHandleFailed           = "handle is out of step with the journal"
	errorReentrantStep          = "step called from inside a step on the same handle"
)

// Sentinel errors a Store or a Handle reports. Callers match them with errors.Is.
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

	// ErrEventDataEncoding wraps a Handle.Fire refused before the kernel ran, because
	// its state.WithEventData payload has no JSON form that decodes again, decodes as a
	// different type than the caller passed (see WithEventDataDecoder), panics while
	// encoding (such as an error that's a typed nil), or exceeds WithMaxEventDataBytes.
	// Nothing applied, so the Handle stays usable.
	ErrEventDataEncoding = errors.New(errorPrefix + errorEventDataEncoding)

	// ErrRecordTooLarge is reported by FileStore.Append for a Record whose journal line
	// exceeds the store's line limit, which a later Load could not read back, even
	// without its Snapshot. No journal line is written, but a Handle's kernel has already
	// applied the step, so the Handle fails with ErrHandleFailed.
	ErrRecordTooLarge = errors.New(errorPrefix + errorRecordTooLarge)

	// ErrHandleFailed is reported by a Handle's Fire, Tick, RunService, DeliverToActor
	// and View, on the call that failed the Handle and every later one. It wraps the
	// cause, which is one of:
	//   - a step applied to the live instance but may not have been recorded;
	//   - a Tick, RunService or DeliverToActor ran while its context ended, so a Fire it
	//     drove may have been refused: it had already consumed the timer, service or
	//     delivery, so it can't simply refuse the way Fire does;
	//   - an effect handler failed.
	// Call Handle.Drain, then Recover a new Handle with the same options to continue,
	// unless the cause is ErrFenced: then a newer owner holds the instance, so stop.
	ErrHandleFailed = errors.New(errorPrefix + errorHandleFailed)

	// ErrReentrantStep is reported by a Handle's Fire, Tick, RunService, DeliverToActor
	// and View when called with the context a step passed to host code, such as a
	// service's context or the Ctx a guard, action or reducer gets: the step holds the
	// Handle's lock, so waiting would deadlock. Nothing applied. Effect handlers run
	// after the lock is released and may call back in. Drain reports it when called
	// from inside a step or an effect handler.
	ErrReentrantStep = errors.New(errorPrefix + errorReentrantStep)

	// ErrCheckpointNotAdvancing is reported by Store.Checkpoint when throughStep
	// does not advance beyond the instance's current checkpoint.
	ErrCheckpointNotAdvancing = errors.New(errorPrefix + errorCheckpointNotAdvancing)

	// ErrInstanceExists is reported by Runner.Start when the InstanceID is already
	// present in the Store, so a fresh Start does not clobber a recorded instance.
	ErrInstanceExists = errors.New(errorPrefix + errorInstanceExists)

	// ErrEffectDispatch wraps a failure returned by the caller-supplied effect
	// handler (WithEffectHandler) while applying an emitted effect, or a panic in it. The effect was
	// persisted (write-ahead) but not marked dispatched, so the Handle fails with
	// ErrHandleFailed wrapping it, and Recover dispatches it again. Callers match
	// this with errors.Is to tell a dispatch failure from a kernel transition or
	// persistence error.
	ErrEffectDispatch = errors.New(errorPrefix + errorEffectDispatch)
)
