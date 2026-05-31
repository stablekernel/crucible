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
// # Run exactly once, replay the result
//
// A durable actor's behavior runs exactly once, on the live path, and the result
// it routes into the parent is recorded; on recovery the recorded result is
// replayed back through the SAME parent transition the live run drove, and the
// actor behavior is NEVER re-instantiated. This is the standard durable-execution
// guarantee: the child machine (with its side effects and its nondeterministic
// output) runs once, and recovery is a pure replay against the parent kernel rather
// than a re-execution of the actor tree.
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
// encoding/json and is re-fired through the parent transition's Assign exactly as on
// the live run. The limitation this carries: a parent reducer that type-asserts a
// concrete, non-JSON Go type from AssignCtx.Event will observe the JSON-decoded
// shape (for example a map[string]any rather than a struct) on the replayed onDone,
// the same boundary the service seam documents. A typed-codec option to record and
// replay the concrete Go value is reserved for a later, additive change and is not
// built here.

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
	// HasData reports whether the parent fire carried a separate actor outcome
	// payload (Data or Error) via WithEventData. A settling onDone / onError fire sets
	// it; a message-driven fire (SendParent) leaves it false so replay re-fires the
	// parent plainly, letting the event itself supply the assign's data.
	HasData bool `json:"hasData,omitempty"`
}

// DeliverToActor routes event into the running actor identified by ref exactly
// once, runs the actor (settling it when it reaches its final state), records each
// parent transition the delivery drove, and persists the produced step. The actor
// behavior is resolved and run against the palette supplied with WithActorPalette;
// the done-data, error, or parent-driving message it routes is journaled as a
// JournalActorMessage correlated by the actor id so recovery replays it without
// re-instantiating the actor.
//
// It returns whether the actor was found running (false for a ref that names no
// live actor — already settled, stopped, or never spawned), and a persistence
// failure as the error. The kernel transitions the delivery drove do not
// themselves fail.
func (h *Handle[S, E, C]) DeliverToActor(ctx context.Context, ref state.ActorRef, event any) (bool, error) {
	if h.actors == nil {
		return false, fmt.Errorf("durable: no actor palette wired for %q (supply WithActorPalette)", h.id)
	}

	before := len(h.inst.History())
	delivered := h.actors.Deliver(ctx, ref, event)
	if !delivered {
		return false, nil
	}

	output, settled, settleErr := h.actorOutcome()
	entries, err := recordActorTransitions(ref.ID, h.inst.History(), before, output, settled, settleErr)
	if err != nil {
		return true, fmt.Errorf("durable: recording actor %q result for %q: %w", ref.ID, h.id, err)
	}
	if h.actorBuf != nil {
		*h.actorBuf = append(*h.actorBuf, entries...)
	}

	step := h.nextStep
	rec := Record{Step: step, Entries: h.drainActors()}
	if err := h.persistStep(ctx, step, &rec); err != nil {
		return true, err
	}
	h.nextStep++
	return true, nil
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

// actorOutcome reads the most recently settled actor's output / error from the
// underlying ActorSystem, the outcome the settling parent fire carried. It reports
// whether a settlement occurred at all (so a delivery that only sent a message and
// did not complete the actor records no settle data).
func (h *Handle[S, E, C]) actorOutcome() (output any, settled bool, settleErr error) {
	if h.actors == nil {
		return nil, false, nil
	}
	if err := h.actors.LastError(); err != nil {
		return nil, true, err
	}
	if out, ok := h.actors.LastOutput(); ok {
		return out, true, nil
	}
	return nil, false, nil
}

// drainActors returns and clears the actor transitions recorded since the last
// drain, so each persisted step carries exactly the transitions that resolved
// during it, in fire order.
func (h *Handle[S, E, C]) drainActors() []state.JournalEntry {
	if h.actorBuf == nil || len(*h.actorBuf) == 0 {
		return nil
	}
	out := make([]state.JournalEntry, len(*h.actorBuf))
	copy(out, *h.actorBuf)
	*h.actorBuf = (*h.actorBuf)[:0]
	return out
}

// recordActorTransitions builds the JournalActorMessage entries for the parent
// Traces the just-completed delivery appended (those at index before..len). Each
// new Trace is one parent transition the actor system drove; the entry carries that
// transition's event payload. When the actor settled (output or error present), the
// settling onDone / onError fire is the LAST of those Traces — a SendParent message
// fires the parent before the actor drains and completes — so the outcome is
// attributed to the last entry, which records HasData; earlier entries are
// message-driven and carry no separate payload.
func recordActorTransitions(id string, history []state.Trace, before int, output any, settled bool, settleErr error) ([]state.JournalEntry, error) {
	if before >= len(history) {
		return nil, nil
	}
	traces := history[before:]
	entries := make([]state.JournalEntry, 0, len(traces))
	for i, tr := range traces {
		var mp actorMessagePayload
		mp.Event = append(json.RawMessage(nil), tr.EventPayload...)
		// Attribute the actor outcome to the settling fire — the last parent Trace —
		// so the onDone / onError transition replays with the recorded done-data /
		// error, while a message-driven fire that preceded it replays plainly.
		if settled && i == len(traces)-1 {
			mp.HasData = true
			if settleErr != nil {
				msg := settleErr.Error()
				mp.Error = &msg
			} else if output != nil {
				raw, err := json.Marshal(output)
				if err != nil {
					return nil, fmt.Errorf("marshaling actor output: %w", err)
				}
				mp.Data = raw
			}
		}
		payload, err := json.Marshal(mp)
		if err != nil {
			return nil, fmt.Errorf("marshaling actor transition: %w", err)
		}
		entries = append(entries, state.JournalEntry{
			Kind:          state.JournalActorMessage,
			CorrelationID: id,
			Payload:       payload,
		})
	}
	return entries, nil
}

// replayActor re-fires one recorded parent transition directly through the
// instance, in place of re-running the actor: it decodes the recorded payload and
// re-fires the parent's event — carrying the recorded done-data / error via
// WithEventData for a settling fire, or plainly for a message-driven fire — so the
// kernel re-derives the same parent transition with the same data, running no actor
// behavior. It reports an error only when the recorded entry cannot be decoded or
// the re-fired transition fails (a corrupt or mis-ordered journal).
func replayActor[S comparable, E comparable, C any](ctx context.Context, inst *state.Instance[S, E, C], codec EventCodec[E], entry state.JournalEntry) error {
	var mp actorMessagePayload
	if err := json.Unmarshal(entry.Payload, &mp); err != nil {
		return fmt.Errorf("durable: decoding recorded actor %q transition: %w", entry.CorrelationID, err)
	}
	event, err := codec.Decode(mp.Event)
	if err != nil {
		return fmt.Errorf("durable: decoding recorded actor %q event: %w", entry.CorrelationID, err)
	}
	var res state.FireResult[S]
	if mp.HasData {
		res = inst.Fire(ctx, event, state.WithEventData(replayActorData(mp)))
	} else {
		res = inst.Fire(ctx, event)
	}
	if res.Err != nil {
		return fmt.Errorf("durable: replaying actor %q transition: %w", entry.CorrelationID, res.Err)
	}
	return nil
}

// replayActorData reconstructs the actor outcome a settling fire carried into the
// parent transition's Assign: the reconstructed error for an onError fire, the
// JSON-decoded done-data for an onDone fire, or nil when the actor completed with no
// output.
func replayActorData(mp actorMessagePayload) any {
	if mp.Error != nil {
		return errors.New(*mp.Error)
	}
	if len(mp.Data) > 0 {
		var data any
		if err := json.Unmarshal(mp.Data, &data); err == nil {
			return data
		}
	}
	return nil
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

// hasActorEntry reports whether a Record carries any recorded actor transition, so
// replay can route it to the actor re-fire path rather than re-firing an external
// event.
func hasActorEntry(rec *Record) bool {
	for _, e := range rec.Entries {
		if e.Kind == state.JournalActorMessage {
			return true
		}
	}
	return false
}
