package state_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// afterMachine builds a flat machine whose "armed" state has a delayed
// transition: after 5s it fires the "elapsed" event and moves to "fired". A
// plain "leave" event exits "armed" early (to "idle"), so a test can exercise
// auto-cancel-on-exit. The "fired" state itself arms nothing.
func afterMachine() *state.Machine[string, string, *trec] {
	return state.ForgeFor[*trec]("timed").
		Action("entry", noteAction("entry")).
		Action("exit", noteAction("exit")).
		State("idle").
		State("armed").
		State("fired").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("go").GoTo("armed").
		Transition("armed").After(5 * time.Second).On("elapsed").GoTo("fired").
		Transition("armed").On("leave").GoTo("idle").
		Quench()
}

// TestAfter_FiresAfterDelay drives a delayed transition with a fake clock: the
// schedule effect arms a timer, advancing past the delay fires the delayed event,
// and the instance lands in the target state.
func TestAfter_FiresAfterDelay(t *testing.T) {
	m := afterMachine()
	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("idle"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	ctx := context.Background()

	res := inst.Fire(ctx, "go")
	sch.Absorb(ctx, res.Effects)

	if inst.Current() != "armed" {
		t.Fatalf("after go, want armed, got %q", inst.Current())
	}
	wantID := state.ScheduleID("timed", "armed", 0)
	if !sch.HasPending(wantID) {
		t.Fatalf("entering armed should arm timer %q; pending=%d", wantID, sch.Pending())
	}
	assertScheduleEffect(t, res.Effects, wantID, 5*time.Second, "elapsed")

	// Not yet due: a tick before the delay fires nothing.
	clk.Advance(4 * time.Second)
	if fired := sch.Tick(ctx); len(fired) != 0 {
		t.Fatalf("tick before delay fired %d events", len(fired))
	}
	if inst.Current() != "armed" {
		t.Fatalf("before delay, want armed, got %q", inst.Current())
	}

	// Past the delay: the timer fires the delayed event and the instance advances.
	clk.Advance(2 * time.Second)
	fired := sch.Tick(ctx)
	if len(fired) != 1 {
		t.Fatalf("tick past delay want 1 fired event, got %d", len(fired))
	}
	if inst.Current() != "fired" {
		t.Fatalf("after delay, want fired, got %q", inst.Current())
	}
	if sch.Pending() != 0 {
		t.Fatalf("timer should be consumed; pending=%d", sch.Pending())
	}
}

// TestAfter_CanceledOnExit asserts auto-cancel-on-exit: leaving the armed
// state before the delay emits CancelScheduled and the delayed event never fires.
func TestAfter_CanceledOnExit(t *testing.T) {
	m := afterMachine()
	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("idle"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	ctx := context.Background()

	sch.Absorb(ctx, inst.Fire(ctx, "go").Effects)
	id := state.ScheduleID("timed", "armed", 0)
	if !sch.HasPending(id) {
		t.Fatalf("timer %q should be armed after entering armed", id)
	}

	// Leave armed before the delay: a CancelScheduled effect drops the timer.
	leave := inst.Fire(ctx, "leave")
	assertCancelEffect(t, leave.Effects, id)
	sch.Absorb(ctx, leave.Effects)

	if inst.Current() != "idle" {
		t.Fatalf("after leave, want idle, got %q", inst.Current())
	}
	if sch.HasPending(id) || sch.Pending() != 0 {
		t.Fatalf("timer should be auto-canceled on exit; pending=%d", sch.Pending())
	}

	// Advancing well past the original delay must not fire anything.
	clk.Advance(time.Hour)
	if fired := sch.Tick(ctx); len(fired) != 0 {
		t.Fatalf("canceled timer fired %d events", len(fired))
	}
	if inst.Current() != "idle" {
		t.Fatalf("canceled timer changed state to %q", inst.Current())
	}
}

// TestAfter_ExplicitCancel asserts the Cancel built-in: a transition's Cancel(id)
// emits a CancelScheduled effect that drops a pending timer, with no host
// registration of the built-in action.
func TestAfter_ExplicitCancel(t *testing.T) {
	id := state.ScheduleID("explicit", "armed", 0)
	m := state.ForgeFor[*trec]("explicit").
		State("idle").
		State("armed").
		State("done").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("go").GoTo("armed").
		// armed arms a 10s timer; the "abort" event cancels it explicitly. The
		// target stays armed (internal abort) so the timer is not also auto-canceled
		// on exit — the Cancel built-in alone drops it.
		Transition("armed").After(10 * time.Second).On("elapsed").GoTo("done").
		Transition("armed").On("abort").GoTo("armed").Cancel(id).
		Quench()

	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("idle"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	ctx := context.Background()

	sch.Absorb(ctx, inst.Fire(ctx, "go").Effects)
	if !sch.HasPending(id) {
		t.Fatalf("entering armed should arm timer %q", id)
	}

	// Firing "abort" runs the transition whose Cancel(id) built-in emits a
	// CancelScheduled for the armed timer, with no host-registered action. The
	// abort is internal (target == source), so no auto-cancel-on-exit runs.
	res := inst.Fire(ctx, "abort")
	assertCancelEffect(t, res.Effects, id)
	sch.Absorb(ctx, res.Effects)
	if sch.HasPending(id) {
		t.Fatalf("explicit Cancel should drop timer %q; pending=%d", id, sch.Pending())
	}

	// The canceled timer must never fire.
	clk.Advance(time.Hour)
	if fired := sch.Tick(ctx); len(fired) != 0 {
		t.Fatalf("canceled timer fired %d events", len(fired))
	}
}

// TestAfter_RoundTrip asserts the `after` IR round-trips losslessly: the delay
// and target survive ToJSON -> LoadFromJSON, and the rehydrated machine schedules
// the same timer.
func TestAfter_RoundTrip(t *testing.T) {
	m := afterMachine()
	raw, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}

	// The delay survives serialization as a JSON number (nanoseconds).
	var probe struct {
		States []struct {
			Name        string `json:"name"`
			Transitions []struct {
				After *int64 `json:"after"`
			} `json:"transitions"`
		} `json:"states"`
	}
	if uerr := json.Unmarshal(raw, &probe); uerr != nil {
		t.Fatalf("probe unmarshal err = %v", uerr)
	}
	foundDelay := false
	for _, s := range probe.States {
		for _, tr := range s.Transitions {
			if tr.After != nil {
				foundDelay = true
				if time.Duration(*tr.After) != 5*time.Second {
					t.Fatalf("after delay round-trip = %v, want 5s", time.Duration(*tr.After))
				}
			}
		}
	}
	if !foundDelay {
		t.Fatal("serialized IR carried no after delay")
	}

	ir, err := state.LoadFromJSON[string, string, *trec](raw)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	reg := state.NewRegistry[*trec]().
		Action("entry", noteAction("entry")).
		Action("exit", noteAction("exit"))
	m2 := ir.Provide(reg).Quench()

	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m2.Cast(&trec{}, state.WithInitialState("idle"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	ctx := context.Background()

	sch.Absorb(ctx, inst.Fire(ctx, "go").Effects)
	clk.Advance(5 * time.Second)
	if fired := sch.Tick(ctx); len(fired) != 1 {
		t.Fatalf("rehydrated machine want 1 fired event, got %d", len(fired))
	}
	if inst.Current() != "fired" {
		t.Fatalf("rehydrated after fire, want fired, got %q", inst.Current())
	}
}

// assertScheduleEffect fails unless effects contains a ScheduleAfter matching id,
// delay, and event.
func assertScheduleEffect(t *testing.T, effects []state.Effect, id string, delay time.Duration, event string) {
	t.Helper()
	for _, e := range effects {
		s, ok := e.(state.ScheduleAfter)
		if !ok {
			continue
		}
		if s.ID == id {
			if s.Delay != delay {
				t.Fatalf("schedule %q delay = %v, want %v", id, s.Delay, delay)
			}
			if s.Event != any(event) {
				t.Fatalf("schedule %q event = %v, want %q", id, s.Event, event)
			}
			return
		}
	}
	t.Fatalf("no ScheduleAfter effect for id %q in %v", id, effects)
}

// assertCancelEffect fails unless effects contains a CancelScheduled for id.
func assertCancelEffect(t *testing.T, effects []state.Effect, id string) {
	t.Helper()
	for _, e := range effects {
		if c, ok := e.(state.CancelScheduled); ok && c.ID == id {
			return
		}
	}
	t.Fatalf("no CancelScheduled effect for id %q in %v", id, effects)
}
