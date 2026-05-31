// Package durable is the host-side durable-execution runtime for the Crucible
// state kernel. It records the nondeterministic results a running instance
// consumes and persists them, so an instance can be checkpointed, crash, and
// resume by replaying recorded values back through the kernel's pure transition
// function rather than re-invoking their external sources.
//
// Import path: github.com/stablekernel/crucible/durable
//
// This package is additive over the state kernel: it consumes the kernel's
// already-reserved persistence seams — Snapshot.Journal ([]state.JournalEntry),
// the EffectEnvelope.EffectID correlation slot, and the injectable Clock /
// ServiceRunner / ActorSystem drivers — without requiring any change to the
// kernel, which stays pure and stdlib-only.
//
// # Store
//
// Store is the persistence seam. A durable instance is an ordered log of
// Records (one per Fire step) layered over periodic full-Snapshot checkpoints.
// Load reconstructs an instance from its latest checkpoint plus the journal /
// effect tail recorded after it. MemStore is the in-memory reference
// implementation that ships here; persistent backends (a Postgres or DynamoDB
// store/sql sub-module) implement the same interface out of tree, so heavy
// database drivers never burden this core module's dependency or vulnerability
// surface.
//
// # Durable timers (the clock seam)
//
// A running instance reads time only through its host driver — the
// state.Scheduler the durable Handle arms and ticks for delayed (`after`)
// transitions — never through the kernel's pure Fire step. The Runner wraps the
// real clock (WithRunnerClock, defaulting to state.SystemClock) in a recording
// clock on the live path: every reading the scheduler consumes, when arming a
// timer's absolute deadline or testing which deadlines are due, is journaled as a
// JournalClockRead in Record.Entries, in read order. On recovery the Runner
// installs a replay clock that returns those recorded readings back in order
// instead of reading the wall clock, so the recovered scheduler re-derives
// identical deadlines and fires the same timers at the same recorded instants —
// replay is wall-clock-independent. A purely event-driven machine reads no clock
// and records no clock entries. Drive timers with Handle.Tick (a host from its
// own timer loop, a test after advancing a fake clock).
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
// its Step ordinal; appending the same Step twice is a no-op (see Store.Append).
type Record struct {
	// Step is the Fire ordinal this Record was produced at, indexing the
	// instance's recorded Traces. It is the per-instance idempotency key: a
	// Store treats a second Append at the same Step as already-applied.
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
// Idempotency: Append is idempotent per (InstanceID, Step). Appending a Step
// that is already present is a no-op — the stored Record is retained unchanged
// and the original append sequence is returned — so an at-least-once caller
// (a crash-and-retry between persist and acknowledge) never double-applies a
// step. The first writer of a Step wins. The idempotency record for a Step
// survives a Checkpoint that compacts that Step away, so a delayed retry of an
// already-checkpointed Step is still recognized as a no-op rather than rejected
// as out of order.
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
// All methods are safe for concurrent use by multiple goroutines.
type Store interface {
	// Append persists rec for the instance and returns its monotonic per-instance
	// append sequence. Appending a Step already recorded for the instance is a
	// no-op that returns the existing sequence (idempotency). Records must be
	// appended in increasing Step order; an out-of-order Step is rejected with
	// ErrStepOutOfOrder.
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
	// is rejected with ErrCheckpointNotAdvancing.
	Checkpoint(ctx context.Context, id InstanceID, snapshot []byte, throughStep int, opts ...CheckpointOption) error
}
