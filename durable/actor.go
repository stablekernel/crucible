package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// This file implements the actor record/replay seam, the third and last
// nondeterministic source the durable runtime records (after the clock and the
// invoked service). A child-machine actor (InvokeActor, or a dynamically spawned
// actor) produces whatever its behavior computes — its done-data on completion, its
// error on failure, or a message it sends that drives the parent — so its result is
// NOT a pure function of (configuration, context, event payload, machine
// definition) and MUST be recorded to reproduce a run.
//
// # Run once per recorded result, replay the result
//
// A durable actor's behavior runs on the live path and the result it routes into the
// parent is recorded; on recovery the recorded result is replayed back through the SAME
// parent transition the live run drove, so a settled actor never re-runs. This is the
// standard durable-execution guarantee: recovery is a pure replay against the parent
// kernel rather than a re-execution of the actor tree. An InvokeActor actor still running
// at recovery restarts from its initial state, because its own state isn't journaled, and
// a Spawn actor doesn't return.
//
// The Runner wraps the kernel's reusable host driver, state.ActorSystem, which
// turns SpawnActor / StopActor effects into running child actors, owns each actor's
// mailbox, and on a child's completion (or failure, or a message it sends)
// re-fires the parent's onDone / onError (or the message event) back through the
// parent's Fire. On the live path DeliverToActor delegates to ActorSystem.Deliver
// (enqueue + step the actor, settling it when it reaches its final state), then
// records each parent transition the delivery drove — the parent event together
// with the actor's done-data / error it carried — as a JournalActorMessage
// correlated by the actor id. On recovery the recorded transitions are replayed in
// order by re-firing the parent directly with the recorded event and data, so the
// kernel re-derives byte-identical context having run no actor.
//
// # Correlation and ordering
//
// Each recorded entry's CorrelationID is the actor id (state.ActorID / the explicit
// invocation id), the same id the kernel stamps on the SpawnActor effect and the
// host delivers to, so replay attributes a recorded result to the exact actor
// invocation it stands in for. Entries are recorded in the order the delivery drove
// the parent — any message the actor sent first, then the settle that completes it —
// and replayed in that order, interleaved with the clock reads, service settlements,
// and external steps of the surrounding Records, so a mix of actors, services,
// timers, and external events in one instance reproduces deterministically.
//
// # Payload typing (serialized, polyglot boundary)
//
// An actor's done-data, error, and message payloads are recorded as SERIALIZED JSON,
// consistent with the kernel's EventPayload contract and the polyglot serializable
// boundary the durable runtime records every nondeterministic result across (the
// clock and service seams record JSON too). The recorded value round-trips through
// encoding/json. A done-data payload is decoded through the Runner's
// WithEventDataDecoder for the parent's onDone event before it reaches the parent,
// live as on replay, as a WithEventData payload is: a decoder for that event hands
// the parent's Assign the decoder's type, and without one the payload arrives in
// JSON's generic form (a struct as a map, an integer as a float64, which loses
// precision above 2^53) — the same boundary the service seam documents. The journal
// records the JSON the live settle decoded and the done-data's Go type, so replay
// decodes the same bytes and holds the decoder to the type the live settle did. An
// error is message-only: an error with the same message, any invalid UTF-8 replaced,
// live and on replay. A done-data payload that won't decode, or a decoder that
// returns another type than the actor produced, aborts the settle — nothing fires —
// and the step fails the Handle, so the actor runs again on Recover; there is no
// parking for actors, as a service result that won't decode has (see RunService).
// A settle that routes no completion — the actor was spawned with neither the onDone
// an output settles through nor the onError a failure does — runs no transform: it
// consults no decoder, so a done-data that won't encode can't fail the step, and it
// journals nothing for the settle.

// actorMessagePayload is the wire form of one recorded parent transition the actor
// system drove: the parent Event it fired, and — for the settling onDone / onError
// fire — the actor outcome (Data) the kernel carried into that transition's Assign
// via the done-event payload. A message-driven fire (an actor's SendParent that
// advanced the parent) records HasData false: the parent event itself carries the
// data, exactly as the live ActorSystem re-fired it with no separate payload.
type actorMessagePayload struct {
	// Event is the structured, JSON form of the parent event the actor system fired
	// — the kernel's Trace.EventPayload for the parent transition. Replay decodes it
	// to the parent's event value and re-fires it.
	Event json.RawMessage `json:"event,omitempty"`
	// Data is the actor's done-data (on onDone) or error message (on onError) the
	// settling fire carried into the parent transition's Assign through the done-event
	// payload. It is present only when HasData is true.
	Data json.RawMessage `json:"data,omitempty"`
	// Error, when present, is the failure message of a settling onError fire,
	// reconstructed into an error and carried into the parent transition's Assign so
	// a reducer reading an error from AssignCtx.Event observes it as on the live run.
	Error *string `json:"error,omitempty"`
	// Type is the done-data's Go type, with its import path (see typeName): the type
	// the actor's completion produced. Every settling onDone fire records it, so replay
	// holds the decoder to it, as the live settle did. It is empty for an error, and
	// for a Record from before settles recorded the type, which accepts any type.
	Type string `json:"type,omitempty"`
	// HasData reports whether the parent fire carried a separate actor outcome
	// payload (Data or Error) via WithEventData. A settling onDone / onError fire sets
	// it; a message-driven fire (SendParent) leaves it false so replay re-fires the
	// parent plainly, letting the event itself supply the assign's data.
	HasData bool `json:"hasData,omitempty"`
	// Refused reports that the live parent Fire was refused (a guard failed, no
	// transition matched, ...), so replay expects the same refusal instead of failing
	// on it. A refusal because a context ended is never recorded: that step fails the
	// Handle instead.
	Refused bool `json:"refused,omitempty"`
	// Before reports a parent Fire that ran before the step's own work: one a spawn at
	// Start or Recover drove, recorded with the step that followed. Replay re-fires it
	// first.
	Before bool `json:"before,omitempty"`
}

// DeliverToActor routes event into the running actor identified by ref, runs the
// actor (settling it when it reaches its final state), records each parent
// transition the delivery drove, and persists the produced step. The actor
// behavior is resolved and run against the palette supplied with WithActorPalette;
// the done-data, error, or parent-driving message it routes is journaled as a
// JournalActorMessage correlated by the actor id so recovery replays it without
// re-instantiating the actor.
//
// The done-data reaches the parent's onDone Assign decoded for that event, live as
// on replay: through the Runner's WithEventDataDecoder when one covers it, else in
// JSON's generic form. An error is message-only: an error with the same message, any
// invalid UTF-8 replaced, which errors.Is and errors.As no longer match, and an
// Error method that panics settles with a message saying so. A done-data payload
// that won't decode, or decodes as another type than the actor produced, aborts the
// settle before it fires anything: DeliverToActor returns ErrHandleFailed, wrapping
// ErrEventDataEncoding, no Assign ran, and nothing is recorded — an actor outcome is
// never parked, as a service result that won't decode is (see RunService), because
// the actor can run again. The Handle stays failed: Recover with a decoder that
// decodes the payload, then deliver the event again, and the actor re-runs.
//
// It returns whether the actor was found running: false for a ref that names no live
// actor (already settled, stopped, or never spawned), or false and an error when no
// palette is wired, the Handle has failed, or ctx ended before the delivery. A
// completion that routes nothing — the actor was spawned with no onDone for its
// outcome — is a no-op: no decoder is consulted, nothing is recorded for the settle,
// and the Handle stays healthy, whatever the actor's output encodes as. Once the
// actor ran it returns true with any error. A result that won't encode, a ctx that ended
// by the end of the delivery, or a parent Fire refused because a context ended (ctx, or
// one middleware derived from it) returns ErrHandleFailed and records nothing: a ctx
// that ended before the actor handled the event made the kernel refuse the actor's own
// Fire, and the event was lost. On a
// recovered Handle an InvokeActor actor restarts from its initial state, and a Spawn
// actor doesn't return. A failed Append also returns ErrHandleFailed, but the write may
// have landed anyway, and Recover finds out which. If another goroutine failed the Handle while this step was
// recording, it returns true and ErrHandleFailed saying the step was recorded. A failed
// Checkpoint returns the plain error, and the delivery is recorded. Resending a recorded
// delivery repeats any message it sends the parent.
func (h *Handle[S, E, C]) DeliverToActor(ctx context.Context, ref state.ActorRef, event any) (bool, error) {
	if err := h.acquire(ctx); err != nil {
		return false, err
	}
	defer h.release()
	defer h.failOnPanic()
	if err := h.refuse(ctx); err != nil {
		return false, err
	}
	if h.actors == nil {
		return false, fmt.Errorf("durable: no actor palette wired for %q (supply WithActorPalette)", h.id)
	}

	stepCtx := h.stepContext(ctx)
	// Restore in a defer, so a panic out of host code doesn't leave the collector driving
	// under this step's context.
	delivered := func() bool {
		defer h.actorFires.driving(stepCtx)()
		return h.actors.Deliver(stepCtx, ref, event)
	}()
	fires := h.actorFires.take()
	if !delivered {
		return false, nil
	}
	// A context that ended during the delivery can make the kernel refuse the actor's
	// own Fire, losing the event, or a parent Fire it drove. A refused Fire rolls back,
	// so live never applied it, but journaling the delivery would replay it under a live
	// context, where it applies. Record nothing.
	if err := stepCanceled(ctx, parentFireErrs(fires)...); err != nil {
		return true, h.fail(fmt.Errorf("durable: delivering to actor %q for %q: %w", ref.ID, h.id, err))
	}
	// The actor's done-data wouldn't decode for its onDone event, or decodes as another
	// type than the actor produced: the settle aborted, firing nothing, so no Assign ran
	// and the instance saw nothing for it. There is no parking for an actor outcome, as
	// a service result that won't decode has (see RunService): the service already ran,
	// but the actor can run again. Fail the Handle and record nothing, so a recovered
	// Handle runs the actor from its initial state and the delivery again.
	if abort := h.actorFires.takeAborts(); abort != nil {
		return true, h.fail(fmt.Errorf("durable: delivering to actor %q for %q: %w", ref.ID, h.id, abort))
	}

	entries, err := recordParentFires(fires, false)
	if err != nil {
		return true, h.fail(fmt.Errorf("durable: recording actor %q result for %q: %w", ref.ID, h.id, err))
	}

	// A parent Fire the delivery drove can arm a timer, which reads the clock. Its effects
	// aren't dispatched, so the step names none, and finishStep only offers its
	// checkpoint.
	rec := &Record{Step: h.nextStep, Entries: append(h.drainClock(), entries...)}
	if _, err := h.recordStep(ctx, rec, nil); err != nil {
		return true, err
	}
	return true, h.finishStep(ctx, rec, nil)
}

// DeliverToActorByID is DeliverToActor keyed by raw actor id, for a host that
// tracks ids rather than refs.
func (h *Handle[S, E, C]) DeliverToActorByID(ctx context.Context, id string, event any) (bool, error) {
	return h.DeliverToActor(ctx, state.ActorRef{ID: id}, event)
}

// ActorRef returns the ActorRef for the running actor under id, and whether such an
// actor is running, so a host can address a spawned child for DeliverToActor. It is
// a thin pass-through to the underlying ActorSystem; a handle with no actor palette
// wired runs no actors and always reports false.
func (h *Handle[S, E, C]) ActorRef(id string) (state.ActorRef, bool) {
	if h.actors == nil {
		return state.ActorRef{}, false
	}
	return h.actors.Ref(id)
}

// recordParentFires builds the JournalActorMessage entries for the parent Fires an
// actor system drove, in fire order, each under the actor it came from. before marks
// Fires that ran before the step's own work.
func recordParentFires[S comparable](fires []notedFire[S], before bool) ([]state.JournalEntry, error) {
	entries := make([]state.JournalEntry, 0, len(fires))
	for _, nf := range fires {
		entry, err := actorEntry(nf, before)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// actorEntry builds one JournalActorMessage entry: the parent Fire's event payload,
// whether the live Fire was refused, and, for a Fire that settled an actor, the
// actor's outcome as the settle decoded it — the JSON the transform decoded for the
// onDone event and its Go type (see replayableOutcome), which it passed as event
// data — so replay re-fires the same onDone or onError with it.
func actorEntry[S comparable](nf notedFire[S], before bool) (state.JournalEntry, error) {
	pf := nf.pf
	var mp actorMessagePayload
	mp.Event = append(json.RawMessage(nil), pf.Result.Trace.EventPayload...)
	mp.Refused = pf.Result.Err != nil
	mp.Before = before
	if pf.Settle {
		mp.HasData = true
		switch {
		case pf.Err != nil:
			msg := pf.Err.Error()
			mp.Error = &msg
		case len(nf.settled.raw) > 0:
			// The JSON the settle decoded, which the journal keeps as the bytes replay
			// decodes, with the Go type it recorded, which replay holds the decoder to.
			mp.Data = append(json.RawMessage(nil), nf.settled.raw...)
			mp.Type = nf.settled.typ
		}
	}
	payload, err := json.Marshal(mp)
	if err != nil {
		return state.JournalEntry{}, fmt.Errorf("marshaling actor transition: %w", err)
	}
	return state.JournalEntry{Kind: state.JournalActorMessage, CorrelationID: pf.ActorID, Payload: payload}, nil
}

// decodedActorEntry is a recorded actor entry with its payload decoded once.
type decodedActorEntry struct {
	actorID string
	mp      actorMessagePayload
}

// splitActorEntries decodes a Record's actor entries, in recorded order, and returns the
// ones that ran before the step's own work apart from the rest.
func splitActorEntries(rec *Record) (before, rest []decodedActorEntry, err error) {
	for _, e := range actorEntries(rec) {
		d := decodedActorEntry{actorID: e.CorrelationID}
		if err := json.Unmarshal(e.Payload, &d.mp); err != nil {
			return nil, nil, fmt.Errorf("durable: decoding recorded actor %q transition: %w", e.CorrelationID, err)
		}
		if d.mp.Before {
			before = append(before, d)
		} else {
			rest = append(rest, d)
		}
	}
	return before, rest, nil
}

// replayActor re-fires one recorded parent transition directly through the
// instance, in place of re-running the actor: it decodes the recorded payload and
// re-fires the parent's event — carrying the recorded done-data / error via
// WithEventData for a settling fire, or plainly for a message-driven fire — so the
// kernel re-derives the same parent transition with the same data, running no actor
// behavior. A settling onDone fire carries its done-data decoded through decode for
// that event, held to the recorded type, as the live settle's transform did. It
// returns the effects of a re-fired transition that applied, so the caller arms the
// timers and services it declares, as the live run did. It reports an error only when
// the recorded entry cannot be decoded or the re-fired transition fails (a corrupt or
// mis-ordered journal), or the recorded done-data won't decode for its event, as the
// live settle refused it. aliases carries the Runner's WithResultTypeAlias renames,
// which the recorded check honors, as a recorded service result's replay does.
func replayActor[S comparable, E comparable, C any](ctx context.Context, inst *state.Instance[S, E, C], codec EventCodec[E], decode EventDataDecoder[E], aliases map[string]string, entry decodedActorEntry) ([]state.Effect, error) {
	mp := entry.mp
	// A Fire refused before the kernel saw it (middleware that denied it) has no event
	// to re-fire; replay is refused the same way.
	if mp.Refused && len(mp.Event) == 0 {
		return nil, nil
	}
	event, err := codec.Decode(mp.Event)
	if err != nil {
		return nil, fmt.Errorf("durable: decoding recorded actor %q event: %w", entry.actorID, err)
	}
	var res state.FireResult[S]
	if mp.HasData {
		data, derr := replayActorData(mp, event, decode, aliases)
		if derr != nil {
			return nil, fmt.Errorf("durable: decoding recorded actor %q done-data: %w", entry.actorID, derr)
		}
		res = inst.Fire(ctx, event, state.WithEventData(data))
	} else {
		res = inst.Fire(ctx, event)
	}
	// The kernel reports every refusal with an error, so the error alone says whether
	// replay was refused, as it said for the live Fire.
	switch {
	case res.Err != nil && mp.Refused:
		return nil, nil // the live run was refused the same way
	case res.Err != nil:
		return nil, fmt.Errorf("durable: replaying actor %q transition: %w", entry.actorID, res.Err)
	case mp.Refused:
		return nil, fmt.Errorf("durable: replaying actor %q transition: it applied, but the live run's was refused", entry.actorID)
	}
	return res.Effects, nil
}

// replayActorData reconstructs the actor outcome a settling fire carried into the
// parent transition's Assign: the reconstructed error for an onError fire, the
// done-data for an onDone fire, or nil when the actor completed with no output. The
// done-data goes through the transform the live settle used (see
// replayableOutcome), as the recorded JSON the live run decoded, held to the recorded
// type — through the aliases the Runner's WithResultTypeAlias maps, as a recorded
// service result is; one that won't decode fails the replay, as the live settle
// refused it. A payload that fails to decode is surfaced as an error rather than
// silently dropped, so a corrupt journal fails replay loudly instead of re-firing the
// parent with nil data it never saw on the live path.
func replayActorData[E comparable](mp actorMessagePayload, event E, decode EventDataDecoder[E], aliases map[string]string) (any, error) {
	if mp.Error != nil {
		return errors.New(*mp.Error), nil
	}
	if len(mp.Data) == 0 {
		return nil, nil
	}
	// The transform decodes the recorded JSON for the onDone event, as it did live,
	// and holds the decoder to the recorded type. An abort is the decode's refusal.
	out, _, abort := replayableOutcome(decode, aliases, nil, "actor done-data", "actor error")(event, recordedResult{raw: mp.Data, typ: mp.Type}, nil)
	if abort != nil {
		return nil, abort
	}
	return out, nil
}

// actorEntries returns the recorded actor transitions carried by a single Record,
// in recorded (fire) order, so replay re-fires them in the order the live run drove
// the parent.
func actorEntries(rec *Record) []state.JournalEntry {
	var out []state.JournalEntry
	for _, e := range rec.Entries {
		if e.Kind == state.JournalActorMessage {
			out = append(out, e)
		}
	}
	return out
}
