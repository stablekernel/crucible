package durable

import (
	"context"
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

// armingMachine is a minimal machine for the arming tests: idle -elapsed-> fired,
// so a Tick that fires an armed timer moves the instance.
func armingMachine() *state.Machine[string, string, int] {
	return state.ForgeFor[int]("arming").
		State("idle").
		State("fired").
		Initial("idle").
		Transition("idle").On("elapsed").GoTo("fired").
		// fired keeps a self-loop, so a second armed timer fires cleanly once the
		// first has moved the instance there.
		Transition("fired").On("elapsed").GoTo("fired").
		Quench()
}

// readCountClock wraps a Clock and counts its Now() readings, so a test can prove
// an arming took none.
type readCountClock struct {
	state.Clock
	reads int
}

func (c *readCountClock) Now() time.Time {
	c.reads++
	return c.Clock.Now()
}

func (c *readCountClock) Advance(d time.Duration) {
	c.Clock.(interface{ Advance(time.Duration) }).Advance(d)
}

// TestArmPersistedTimers_ArmsAtTheDeadlineWithoutReadingTheClock confirms the
// pre-walk arming arms each persisted timer into the scheduler at its recorded
// absolute deadline and seeds the mirror's table, taking no clock reading: a read
// there would consume the recorded reading the first tail Record owns. A deadline
// already in the past fires on the next Tick, and a later one when the clock
// reaches it.
func TestArmPersistedTimers_ArmsAtTheDeadlineWithoutReadingTheClock(t *testing.T) {
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := &readCountClock{Clock: state.NewFakeClock(base)}
	inst := armingMachine().Cast(0, state.WithInitialState("idle"), state.WithClock[string](clk))
	sched := state.NewScheduler(inst)
	past := base.Add(-3 * time.Second)
	future := base.Add(8 * time.Second)
	wires := []pendingTimerWire{
		{ID: "past", DueUnixNano: past.UnixNano(), Event: json.RawMessage(`"elapsed"`)},
		{ID: "future", DueUnixNano: future.UnixNano(), Event: json.RawMessage(`"elapsed"`)},
	}
	armed := map[string]pendingTimer{}
	if err := armPersistedTimers[string, string, int](jsonEventCodec[string]{}, sched, wires, armed); err != nil {
		t.Fatalf("armPersistedTimers: %v", err)
	}
	if clk.reads != 0 {
		t.Fatalf("arming took %d clock readings, want none", clk.reads)
	}
	for id, due := range map[string]time.Time{"past": past, "future": future} {
		if !sched.HasPending(id) {
			t.Fatalf("timer %q not armed", id)
		}
		if got := armed[id]; got.id != id || !got.due.Equal(due) {
			t.Fatalf("armed table for %q = %+v, want due %v", id, got, due)
		}
	}
	// The elapsed deadline fires on the next Tick; the future one stays armed.
	fired := sched.Tick(context.Background())
	if len(fired) != 1 || inst.Current() != "fired" {
		t.Fatalf("tick fired %d, state %q, want the elapsed timer alone", len(fired), inst.Current())
	}
	if !sched.HasPending("future") {
		t.Fatal("the future timer should stay armed")
	}
	clk.Advance(8 * time.Second)
	if fired = sched.Tick(context.Background()); len(fired) != 1 {
		t.Fatalf("tick at the deadline fired %d, want the future timer", len(fired))
	}
}

// TestArmPersistedTimers_BadEventErrors surfaces a decode failure for a corrupt
// persisted timer event rather than silently dropping the timer.
func TestArmPersistedTimers_BadEventErrors(t *testing.T) {
	wires := []pendingTimerWire{
		{ID: "bad", DueUnixNano: 1, Event: json.RawMessage(`{not valid`)},
	}
	clk := state.NewFakeClock(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))
	inst := armingMachine().Cast(0, state.WithInitialState("idle"), state.WithClock[string](clk))
	err := armPersistedTimers[string, string, int](jsonEventCodec[string]{}, state.NewScheduler(inst), wires, map[string]pendingTimer{})
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

// TestRecover_SeedsTheTimerTableWithThePersistedDeadline proves the Handle's timer
// table after a Recover whose tail replays no timer step ends with each
// still-pending timer at its persisted absolute deadline, so the next checkpoint
// persists it again and the recovered scheduler holds it armed.
func TestRecover_SeedsTheTimerTableWithThePersistedDeadline(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[int]("seeded").
		State("idle").
		State("waiting").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("arm").GoTo("waiting").
		Transition("waiting").On("poke").GoTo("waiting").
		Transition("waiting").After(10 * time.Second).On("elapsed").GoTo("done").
		Quench()
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	store, id := NewMemStore(), InstanceID("seeded")
	h, err := NewRunner(m, store,
		WithRunnerClock[string, string, int](state.NewFakeClock(base)),
		WithCheckpointEvery[string, string, int](2)).
		Start(ctx, id, 0, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"arm", "poke"} {
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire(%q): %v", ev, err)
		}
	}
	rec, err := Recover(ctx, m, store, id,
		WithRunnerClock[string, string, int](state.NewFakeClock(base.Add(time.Hour))),
		WithCheckpointEvery[string, string, int](2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(rec.timers) != 1 {
		t.Fatalf("recovered timer table = %v, want one pending timer", rec.timers)
	}
	for id, pt := range rec.timers {
		if want := base.Add(10 * time.Second); !pt.due.Equal(want) {
			t.Fatalf("timer %q due %v, want the persisted %v", id, pt.due, want)
		}
		if !rec.sched.HasPending(id) {
			t.Fatalf("timer %q is in the table but not armed on the scheduler", id)
		}
	}
}
