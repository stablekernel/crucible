package durable

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/stablekernel/crucible/state"
)

// This file carries the durable-layer machinery that lets pending delayed
// (`after`) timers survive checkpoint compaction. A timer is armed when the
// scheduler absorbs a ScheduleAfter effect, which records its absolute deadline
// against the recording clock; that arming step's clock read is journaled into the
// step's Record. But when a checkpoint compacts the journal tail through the
// arming step, the ScheduleAfter is no longer in the tail, so replay alone cannot
// re-arm the timer — the recorded arm is gone with the compacted prefix.
//
// The fix mirrors the state kernel's own model (Snapshot.Pending.Timers +
// ResumeEffects re-derive a ScheduleAfter for the restored configuration) but adds
// the one thing the configuration cannot supply: the timer's ABSOLUTE deadline.
// The declared `after` Delay re-arms a fresh timer at now+Delay; a timer pending at
// a checkpoint has already burned part of its delay, so the durable layer persists
// each armed timer's absolute deadline alongside the checkpoint and, on recovery,
// re-arms the remainder (deadline − resumed now) rather than the full declared
// Delay. The deadline is itself a recorded clock read, so the re-armed timer fires
// at the same recorded instant regardless of the wall clock at recovery time, and
// the recovered run is byte-identical to one that never crashed.

// pendingTimer is one armed delayed (`after`) timer the durable Handle tracks
// alongside the kernel scheduler, so its absolute deadline can be persisted at a
// checkpoint and re-armed after compaction. The kernel Scheduler holds the
// authoritative pending set but does not expose deadlines; the Handle mirrors the
// same arming arithmetic (the recorded clock read the Absorb consumed, plus the
// effect's Delay) to recover each deadline without an extra clock read.
type pendingTimer struct {
	// id is the stable scheduleID the kernel assigns the delayed edge, matching the
	// ScheduleAfter/CancelScheduled effect ID, so the Handle's table and the
	// scheduler's pending set key identically.
	id string
	// due is the timer's absolute fire deadline (the recorded arming clock read plus
	// the declared Delay), the value persisted so recovery re-arms the remainder.
	due time.Time
	// event is the delayed event the timer re-fires through Fire when it elapses,
	// type-erased for the abstract ScheduleAfter surface and re-typed by the
	// scheduler on re-arm.
	event any
}

// pendingTimerWire is the serializable form of a pendingTimer persisted in a
// checkpoint envelope: the scheduleID, the absolute deadline as Unix nanoseconds
// (so a re-arm is wall-clock-independent), and the JSON-encoded delayed event.
type pendingTimerWire struct {
	// ID is the timer's stable scheduleID.
	ID string `json:"id"`
	// DueUnixNano is the absolute fire deadline in Unix nanoseconds.
	DueUnixNano int64 `json:"dueUnixNano"`
	// Event is the JSON-encoded delayed event the timer re-fires.
	Event json.RawMessage `json:"event,omitempty"`
}

// checkpointEnvelope is the durable layer's wrapper around the bytes a Store keeps
// for a checkpoint: the opaque state.Snapshot bytes plus the pending-timer table
// armed at the checkpoint instant. The durable layer fully owns the checkpoint
// bytes (it both marshals them at Checkpoint and unmarshals them at recover), so
// wrapping the kernel snapshot in this envelope is additive and invisible to the
// Store, which treats checkpoint bytes as opaque. A checkpoint with no armed timer
// carries an empty Timers list and round-trips to exactly the kernel snapshot it
// wraps.
type checkpointEnvelope struct {
	// Snapshot is the marshaled state.Snapshot the kernel produced — the
	// authoritative instance state the checkpoint restores from.
	Snapshot json.RawMessage `json:"snapshot"`
	// Timers is the durable-layer side-channel: the absolute deadlines of the
	// delayed (`after`) timers armed for the checkpointed configuration, so a
	// recovery whose compacted tail no longer carries the arming ScheduleAfter can
	// still re-arm them at their recorded instants.
	Timers []pendingTimerWire `json:"timers,omitempty"`
}

// marshalCheckpoint wraps the marshaled kernel snapshot and the armed pending
// timers into a checkpoint envelope's bytes. Timers are emitted in scheduleID
// order so the persisted form is deterministic regardless of the map iteration
// order they were collected in.
func marshalCheckpoint(snap []byte, timers map[string]pendingTimer) ([]byte, error) {
	env := checkpointEnvelope{Snapshot: snap}
	if len(timers) > 0 {
		ids := make([]string, 0, len(timers))
		for id := range timers {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		env.Timers = make([]pendingTimerWire, 0, len(ids))
		for _, id := range ids {
			pt := timers[id]
			evRaw, err := json.Marshal(pt.event)
			if err != nil {
				return nil, fmt.Errorf("durable: encoding pending timer %q event: %w", id, err)
			}
			env.Timers = append(env.Timers, pendingTimerWire{
				ID:          id,
				DueUnixNano: pt.due.UnixNano(),
				Event:       evRaw,
			})
		}
	}
	out, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("durable: encoding checkpoint envelope: %w", err)
	}
	return out, nil
}

// unmarshalCheckpoint splits a checkpoint envelope's bytes back into the kernel
// snapshot bytes and the persisted pending-timer wires. It tolerates a bare
// (pre-envelope) snapshot: bytes that are not a checkpoint envelope are returned
// as the snapshot with no timers, so an older checkpoint still restores. A
// checkpoint envelope is distinguished by carrying a non-empty "snapshot" member.
func unmarshalCheckpoint(b []byte) ([]byte, []pendingTimerWire, error) {
	var env checkpointEnvelope
	if err := json.Unmarshal(b, &env); err != nil || len(env.Snapshot) == 0 {
		// Not an envelope (or an envelope without the wrapped snapshot member): treat
		// the bytes as a bare kernel snapshot, the pre-timer-survival checkpoint form.
		return b, nil, nil
	}
	return env.Snapshot, env.Timers, nil
}

// decodeTimerEvent decodes a persisted timer event wire back into the machine's
// event type E through the same codec the Runner replays driving events with, so
// the re-armed ScheduleAfter carries a correctly-typed event the scheduler accepts.
func decodeTimerEvent[E comparable](codec EventCodec[E], raw json.RawMessage) (E, error) {
	return codec.Decode(raw)
}

// mirrorArmed folds the ScheduleAfter/CancelScheduled effects of a replayed step
// into the armed timer table at the recorded instant now, so recovery tracks which
// timers the tail replay re-established and at what deadlines. It is the replay-side
// analog of the Handle's live arming, sharing the recorded clock read rather than a
// live one.
func mirrorArmed(armed map[string]pendingTimer, effects []state.Effect, now time.Time) {
	for _, eff := range effects {
		switch e := eff.(type) {
		case state.ScheduleAfter:
			armed[e.ID] = pendingTimer{id: e.ID, due: now.Add(e.Delay), event: e.Event}
		case state.CancelScheduled:
			delete(armed, e.ID)
		}
	}
}

// reArmEffects builds the ScheduleAfter effects that re-arm persisted timers at
// their recorded absolute deadlines, relative to now: each timer's remaining delay
// is its deadline minus now, so absorbing the effects through the live scheduler
// reproduces the same absolute deadline the live run held. A deadline already at or
// before now yields a non-positive delay, which the scheduler arms as immediately
// due, so an elapsed-while-down timer fires on the next Tick. Timers whose id is
// already armed (re-established by tail replay) are skipped, so a timer the tail
// still carries is not double-armed. Returns the effects and the durable-side
// pendingTimer table to seed the Handle, both in scheduleID order.
func reArmEffects[E comparable](
	codec EventCodec[E],
	wires []pendingTimerWire,
	already map[string]pendingTimer,
	now time.Time,
) ([]state.Effect, map[string]pendingTimer, error) {
	var effects []state.Effect
	seeded := map[string]pendingTimer{}
	for _, w := range wires {
		if _, armed := already[w.ID]; armed {
			continue // tail replay already re-armed this timer; do not double-arm.
		}
		ev, err := decodeTimerEvent(codec, w.Event)
		if err != nil {
			return nil, nil, fmt.Errorf("durable: decoding pending timer %q event: %w", w.ID, err)
		}
		due := time.Unix(0, w.DueUnixNano).UTC()
		effects = append(effects, state.ScheduleAfter{
			ID:    w.ID,
			Delay: due.Sub(now),
			Event: ev,
		})
		seeded[w.ID] = pendingTimer{id: w.ID, due: due, event: ev}
	}
	return effects, seeded, nil
}
