package durable

import (
	"context"

	"github.com/stablekernel/crucible/state"
)

// InstanceID is the stable identity of one durable instance: the key under
// which a Store records and reconstructs that instance's checkpoint and
// journal/effect tail. It is host-assigned and opaque to the Store.
type InstanceID string

// Record is one durable append: the unit a Store persists for a single Fire
// step. It carries the nondeterministic results recorded during the step
// (Entries), the effects the step emitted (Effects, each carrying a stamped
// EffectID for idempotent dispatch), and optionally a full marshaled Snapshot
// checkpoint taken at this step. A Record is identified within its instance by
// its Step ordinal; appending the same Record at the same Step twice is a no-op,
// and appending a different one is rejected (see Store.Append).
//
// The Stores compare Records by a digest of their JSON form, taken through cloneRecord.
// A new field needs `json:",omitempty"`, or every digest changes and a retried Append
// after an upgrade reads as a conflict. It must also be copied in cloneRecord, or the
// digest ignores it. The same applies to state.JournalEntry and state.EffectEnvelope.
type Record struct {
	// Step is the ordinal this Record was produced at. Ordinals rise with each Record,
	// and a tick barrier consumes one per timer it fired plus its own, so they don't
	// index the instance's Traces. It is the per-instance idempotency key: a
	// Store treats a second Append of the same Record at the same Step as
	// already-applied.
	Step int

	// Event is the structured, JSON form of the driving event the Fire at this
	// Step consumed — the kernel's Trace.EventPayload, captured verbatim. Replay
	// reconstructs the exact event from this payload and re-Fires it to advance
	// the restored instance one step, so the recorded run reaches byte-identical
	// state without re-deriving the event from its human-readable label. It is
	// empty for a Record that carries only a checkpoint or only nondeterministic
	// Entries (an event with no JSON form is omitted, matching the kernel's
	// additive EventPayload contract).
	Event []byte

	// EventData is the JSON form of the payload the Fire at this Step carried
	// through state.WithEventData, or empty when it carried none. The live Fire and
	// a replay both decode it through the Runner's EventDataDecoder, so the transition's
	// Assign sees the same value.
	EventData []byte `json:",omitempty"`

	// EventDataError is the message of an error payload, which has no useful JSON
	// form. The live Fire and a replay both supply an error with this message.
	EventDataError *string `json:",omitempty"`

	// Entries are the nondeterministic results recorded during this step — the
	// invoked-service done/error payloads, actor messages, clock reads, and
	// randomness draws the kernel consumed — in the order they resolved. Replay
	// returns these verbatim instead of re-invoking their sources.
	Entries []state.JournalEntry

	// Effects are the effects the step emitted, each with its deterministically
	// stamped EffectID, persisted before dispatch so a crash between persist and
	// dispatch is recoverable: Resume re-dispatches, deduped by EffectID.
	Effects []state.EffectEnvelope

	// Snapshot, when non-empty, is a full marshaled state.Snapshot captured at
	// this step — a checkpoint the instance can be reconstructed from without
	// replaying the whole journal from the start. It is produced by
	// state.MarshalSnapshot and consumed by state.UnmarshalSnapshot; the Store
	// treats it as opaque bytes.
	Snapshot []byte

	// Tick marks a scheduler-tick barrier rather than an externally fired step: a
	// Record produced when the durable Handle ticked its delayed-transition
	// scheduler, carrying the clock readings that tick consumed (in Entries) and
	// the count of timer steps it produced (TickSteps). Replay re-derives the
	// timers by re-ticking the recovered scheduler against those recorded
	// readings rather than re-firing the events directly, so each timer fires at
	// its recorded instant. It is false for externally fired steps, which carry a
	// driving Event instead.
	Tick bool

	// TickSteps is the number of timer transitions the tick at this barrier fired
	// (zero for a tick that found nothing due). It lets replay advance its step
	// accounting across a re-derived tick without a recorded Event per timer.
	TickSteps int

	// EndedActors, when set, holds the child-machine actors the instance's state
	// declared once this step ended that ran and have ended, settled or stopped, or
	// whose spawn failed and whose onError applied (the journal holds that onError), so
	// Recover doesn't spawn them again. An actor whose spawn failed with no onError to
	// route to isn't one, and Recover tries it again. The first step after Start records the
	// set, and later steps record it when they change it. It is nil otherwise, for a
	// machine with no actor palette, and on a Record from before this field. An empty
	// set is not nil: a Store must keep it, though it treats other empty fields as nil.
	EndedActors *ActorSet `json:",omitempty"`

	// UnstartedActors, when set, holds the child-machine actors the instance's state
	// declared once this step ended that aren't running and haven't ended: their latest
	// spawn failed with no onError to route to. Recover spawns them as new, so the
	// messages they send the parent as they start apply and are journaled. A declared
	// actor in neither set was running, and the journal holds every parent Fire its start
	// drove, so Recover spawns it again without letting those reach the parent. It's
	// recorded as EndedActors is: with the first step after Start, then when a step
	// changes it, and an empty set is not nil. When the journal holds none, as on a
	// Record from before this field, Recover lets every spawn reach the parent.
	UnstartedActors *ActorSet `json:",omitempty"`
}

// ActorSet is a set of child-machine actor ids at a point in a durable instance's
// journal.
type ActorSet struct {
	// IDs are the actors' ids, sorted.
	IDs []string `json:"ids"`
}

// followingStep returns the step ordinal after r. A tick barrier consumes one
// ordinal per timer it fired plus its own; every other Record consumes one.
func (r *Record) followingStep() int {
	if r.Tick {
		return r.Step + r.TickSteps + 1
	}
	return r.Step + 1
}

// Store is the durable-execution persistence seam. It records an instance's
// per-step Records and periodic Snapshot checkpoints, and reconstructs the
// instance from the latest checkpoint plus the tail of Records appended after
// it. The in-memory MemStore is the reference implementation; persistent
// backends implement the same contract.
//
// # Contract
//
// Ordering: Records for an instance are totally ordered by Step. Append accepts
// Records in increasing Step order; Load returns the post-checkpoint tail in
// that same Step order. A Store preserves the relative order of Entries and
// Effects within each Record verbatim.
//
// Idempotency: Append is idempotent per (InstanceID, Step). Appending the same
// Record at a Step already present is a no-op — the stored Record is retained
// unchanged and the original append sequence is returned — so an at-least-once
// caller (a crash-and-retry between persist and acknowledge) never double-applies
// a step. Appending a different Record at that Step is rejected with
// ErrStepConflict, because two writers disagree about the step and dropping one
// would acknowledge a step the journal doesn't hold. The idempotency record for a
// Step survives a Checkpoint that compacts that Step away, so a delayed retry of an
// already-checkpointed Step is still recognized rather than rejected as out of
// order, and a different Record there is still a conflict. Two Records are the
// same when every field matches, with a nil and an empty field counting as equal,
// except EventDataError, where nil means no error and an empty message is an error,
// and EndedActors and UnstartedActors, where nil means none recorded and an empty set
// means none.
// An Append carrying WithIdempotencyKey (a backend reads it with
// AppendIdempotencyKey) dedups on that key alone, so the caller vouches for the
// Record: no comparison is made. Keys starting "step:" are reserved for the Store's
// own per-Step key. A Step recorded under an explicit key is not recognized by a
// later Append without that key, or with a different one: that Append returns
// ErrStepOutOfOrder.
//
// Fencing: an Append or Checkpoint may carry a fence token (WithFence,
// WithCheckpointFence; a backend reads it with AppendFence and CheckpointFence). A
// Store keeps the highest token it has seen per instance, as durably as its Records.
// The token check must be serialized with the write it guards, against the Store's
// own state. A write whose
// token is lower returns ErrFenced and writes nothing; this check comes before
// every other. A token that passes raises the highest even if the write then fails
// another check, so a new owner's refused first write still fences the old owner.
// A write with no token counts as token zero, so once an instance has
// been fenced an unfenced write is refused. An equal token passes, so a retry by
// the same owner still works. Tokens alone can't separate two writers on one token;
// ErrStepConflict does.
//
// Atomicity: each Append and each Checkpoint is atomic. A concurrent Load never
// observes a partially written Record or a checkpoint torn against its tail; it
// observes either the state before the call or the state fully after it.
//
// Consistency: Load returns the most recent checkpoint Snapshot together with
// every Record whose Step is strictly greater than the checkpoint's
// throughStep — the exact journal/effect tail needed to bring that checkpoint
// up to date. For an instance with no checkpoint, Snapshot is nil and the tail
// is the full Record history. For an unknown instance, Load reports
// ErrInstanceNotFound.
//
// Fields: a Store keeps every Record field, including EventData and EventDataError. A
// Store that drops one of those loses that payload on recovery. Snapshot is the
// exception: recovery reads the checkpoint, so a Store may drop a tail Record's copy.
//
// All methods are safe for concurrent use by multiple goroutines.
type Store interface {
	// Append persists rec for the instance and returns its monotonic per-instance
	// append sequence. Appending the same Record at a Step already recorded for the
	// instance is a no-op that returns the existing sequence (idempotency); a
	// different Record there is rejected with ErrStepConflict. Records must be
	// appended in increasing Step order; an out-of-order Step is rejected with
	// ErrStepOutOfOrder. A fence token below the highest seen is rejected with
	// ErrFenced. A Store with a size limit reports a Record over it, as FileStore does
	// with ErrRecordTooLarge.
	Append(ctx context.Context, id InstanceID, rec Record, opts ...AppendOption) (seq int64, err error)

	// Load returns the instance's latest checkpoint Snapshot bytes (nil if the
	// instance has been appended to but never checkpointed) together with the
	// tail of Records appended after that checkpoint, in Step order. It reports
	// ErrInstanceNotFound for an instance that has never been written.
	Load(ctx context.Context, id InstanceID) (snapshot []byte, tail []Record, err error)

	// Checkpoint persists snapshot as the instance's checkpoint at throughStep and
	// compacts the journal/effect tail through that step, so a subsequent Load
	// returns this Snapshot plus only the Records appended after throughStep. A
	// Checkpoint that does not advance throughStep beyond the current checkpoint
	// is rejected with ErrCheckpointNotAdvancing. A fence token below the highest
	// seen is rejected with ErrFenced.
	Checkpoint(ctx context.Context, id InstanceID, snapshot []byte, throughStep int, opts ...CheckpointOption) error
}

// StepReporter is an optional Store capability: the highest Step recorded for an
// instance, across its checkpoint and its tail. Recover asks for it when a checkpoint
// from before checkpoints carried the next step leaves no Record to number from, so
// the next step lands past every recorded one. MemStore and FileStore implement it. A
// Store without it falls back to the restored Trace count, which can number a step
// the Store already holds, so every Append then fails with ErrStepOutOfOrder.
type StepReporter interface {
	// LastStep returns the highest Step recorded for id, across its checkpoint and its
	// tail, and false when it holds none. Recover calls it only for an instance Load just
	// found, and fails on any error it returns.
	LastStep(ctx context.Context, id InstanceID) (step int, ok bool, err error)
}
