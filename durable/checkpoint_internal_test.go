package durable

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// TestMarshalCheckpoint_RoundTrip confirms a checkpoint envelope carrying pending
// timers round-trips through marshal/unmarshal: the kernel snapshot bytes survive
// verbatim and each timer's id, deadline, and event are recovered.
func TestMarshalCheckpoint_RoundTrip(t *testing.T) {
	snap := []byte(`{"machine":"m","current":"a"}`)
	due := time.Unix(0, 1_700_000_000_000_000_000).UTC()
	timers := map[string]pendingTimer{
		"m:a:after:0": {id: "m:a:after:0", due: due, event: "elapsed"},
	}
	b, err := marshalCheckpoint(checkpointEnvelope{Snapshot: snap}, timers)
	if err != nil {
		t.Fatalf("marshalCheckpoint: %v", err)
	}
	gotSnap, gotWires, _ := unmarshalCheckpointParts(b)
	if string(gotSnap) != string(snap) {
		t.Fatalf("snapshot round-trip: want %s, got %s", snap, gotSnap)
	}
	if len(gotWires) != 1 {
		t.Fatalf("want 1 timer wire, got %d", len(gotWires))
	}
	w := gotWires[0]
	if w.ID != "m:a:after:0" || w.DueUnixNano != due.UnixNano() {
		t.Fatalf("timer wire mismatch: %+v", w)
	}
	var ev string
	if err := json.Unmarshal(w.Event, &ev); err != nil || ev != "elapsed" {
		t.Fatalf("event decode: ev=%q err=%v", ev, err)
	}
}

// TestMarshalCheckpoint_NoTimers omits the timers member entirely when none are
// armed, so an event-driven checkpoint's envelope stays minimal.
func TestMarshalCheckpoint_NoTimers(t *testing.T) {
	snap := []byte(`{"machine":"m"}`)
	b, err := marshalCheckpoint(checkpointEnvelope{Snapshot: snap}, nil)
	if err != nil {
		t.Fatalf("marshalCheckpoint: %v", err)
	}
	var env checkpointEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if len(env.Timers) != 0 {
		t.Fatalf("want no timers, got %d", len(env.Timers))
	}
}

// TestUnmarshalCheckpoint_BareSnapshotFallback proves a bare (pre-envelope)
// checkpoint — raw kernel snapshot bytes without the envelope wrapper — is
// tolerated: the bytes are returned as the snapshot with no timers, so an older
// checkpoint still restores.
func TestUnmarshalCheckpoint_BareSnapshotFallback(t *testing.T) {
	bare := []byte(`{"machine":"m","current":"a","configuration":["a"]}`)
	snap, wires, _ := unmarshalCheckpointParts(bare)
	if string(snap) != string(bare) {
		t.Fatalf("bare snapshot not passed through: want %s, got %s", bare, snap)
	}
	if wires != nil {
		t.Fatalf("bare snapshot yielded timers: %+v", wires)
	}
}

// TestUnmarshalCheckpoint_Garbage returns garbage bytes as a bare snapshot rather
// than erroring, matching the bare-snapshot tolerance contract (the caller surfaces
// any real decode failure when it unmarshals the kernel snapshot).
func TestUnmarshalCheckpoint_Garbage(t *testing.T) {
	garbage := []byte(`not json`)
	snap, wires, _ := unmarshalCheckpointParts(garbage)
	if string(snap) != string(garbage) || wires != nil {
		t.Fatalf("garbage not passed through: snap=%s wires=%+v", snap, wires)
	}
}

// TestReArmEffects_SkipsAlreadyArmed confirms a persisted timer already armed by
// tail replay is not re-armed (no double-arm), while an un-armed one is re-armed at
// its remaining delay.
func TestReArmEffects_SkipsAlreadyArmed(t *testing.T) {
	now := time.Unix(0, 1_000_000_000).UTC()
	wires := []pendingTimerWire{
		{ID: "armed", DueUnixNano: now.Add(5 * time.Second).UnixNano(), Event: json.RawMessage(`"x"`)},
		{ID: "compacted", DueUnixNano: now.Add(8 * time.Second).UnixNano(), Event: json.RawMessage(`"y"`)},
	}
	already := map[string]pendingTimer{"armed": {id: "armed"}}
	effects, seeded, err := reArmEffects[string](jsonEventCodec[string]{}, wires, already, now)
	if err != nil {
		t.Fatalf("reArmEffects: %v", err)
	}
	if len(effects) != 1 || len(seeded) != 1 {
		t.Fatalf("want exactly the compacted timer re-armed, got effects=%d seeded=%d", len(effects), len(seeded))
	}
	sa, ok := effects[0].(state.ScheduleAfter)
	if !ok || sa.ID != "compacted" {
		t.Fatalf("want compacted ScheduleAfter, got %+v", effects[0])
	}
	if sa.Delay != 8*time.Second {
		t.Fatalf("remaining delay: want 8s, got %s", sa.Delay)
	}
}

// TestReArmEffects_ElapsedDeadlineImmediate confirms a deadline already at or before
// now yields a non-positive delay (armed as immediately due), so an elapsed-while-
// down timer fires on the next Tick.
func TestReArmEffects_ElapsedDeadlineImmediate(t *testing.T) {
	now := time.Unix(0, 10_000_000_000).UTC()
	wires := []pendingTimerWire{
		{ID: "past", DueUnixNano: now.Add(-3 * time.Second).UnixNano(), Event: json.RawMessage(`"z"`)},
	}
	effects, _, err := reArmEffects[string](jsonEventCodec[string]{}, wires, map[string]pendingTimer{}, now)
	if err != nil {
		t.Fatalf("reArmEffects: %v", err)
	}
	if len(effects) != 1 {
		t.Fatalf("want 1 effect, got %d", len(effects))
	}
	if sa := effects[0].(state.ScheduleAfter); sa.Delay > 0 {
		t.Fatalf("elapsed deadline should arm non-positive delay, got %s", sa.Delay)
	}
}

// TestReArmEffects_BadEventErrors surfaces a decode failure for a corrupt persisted
// timer event rather than silently dropping the timer.
func TestReArmEffects_BadEventErrors(t *testing.T) {
	wires := []pendingTimerWire{
		{ID: "bad", DueUnixNano: 1, Event: json.RawMessage(`{not valid`)},
	}
	_, _, err := reArmEffects[string](jsonEventCodec[string]{}, wires, map[string]pendingTimer{}, time.Unix(0, 0))
	if err == nil {
		t.Fatal("want decode error for corrupt timer event, got nil")
	}
}

// TestMarshalCheckpoint_BadEventErrors surfaces an encode failure for a timer
// whose event value cannot be JSON-marshaled rather than persisting a broken
// envelope.
func TestMarshalCheckpoint_BadEventErrors(t *testing.T) {
	timers := map[string]pendingTimer{
		"bad": {id: "bad", due: time.Unix(0, 1), event: make(chan int)},
	}
	if _, err := marshalCheckpoint(checkpointEnvelope{Snapshot: []byte(`{}`)}, timers); err == nil {
		t.Fatal("want encode error for unmarshalable timer event, got nil")
	}
}

// TestTickInstant covers the no-clock-read branch (a tick that fired nothing
// records no read) alongside the happy path.
func TestTickInstant(t *testing.T) {
	if _, ok := tickInstant(nil); ok {
		t.Fatal("empty entries should report no instant")
	}
	entries := []state.JournalEntry{
		{Kind: state.JournalClockRead, ClockUnixNano: 42},
	}
	got, ok := tickInstant(entries)
	if !ok || got.UnixNano() != 42 {
		t.Fatalf("tickInstant: ok=%v got=%d", ok, got.UnixNano())
	}
}

// TestCheckpoint_NextStepRoundTrips proves the envelope carries the ordinal after
// the checkpoint, and that a checkpoint without one reports zero, the old form.
func TestCheckpoint_NextStepRoundTrips(t *testing.T) {
	b, err := marshalCheckpoint(checkpointEnvelope{Snapshot: []byte(`{"v":1}`), NextStep: 9}, nil)
	if err != nil {
		t.Fatalf("marshalCheckpoint: %v", err)
	}
	if _, _, next := unmarshalCheckpointParts(b); next != 9 {
		t.Fatalf("unmarshalCheckpoint next = %d, want 9", next)
	}
	if _, _, next := unmarshalCheckpointParts([]byte(`{"snapshot":{"v":1}}`)); next != 0 {
		t.Fatalf("old envelope next = %d, want 0", next)
	}
}

// unmarshalCheckpointParts is unmarshalCheckpoint split into the parts these tests check.
func unmarshalCheckpointParts(b []byte) ([]byte, []pendingTimerWire, int) {
	env := unmarshalCheckpoint(b)
	return env.Snapshot, env.Timers, env.NextStep
}
