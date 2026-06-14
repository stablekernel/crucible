package state_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// This file pins the production wall-clock seam, SystemClock (scheduler.go:217),
// and its Now / After methods. The deterministic FakeClock-backed tests
// (scheduler_test.go) never touch this path: they advance a fake clock and never
// wait on real time. These tests exercise the REAL systemClock — once driving a
// genuine `after` transition to fire through a Scheduler, and once asserting the
// raw Now / After contract — with tiny real durations so they stay fast and
// non-flaky.

// TestSystemClock_DrivesRealAfterTransition drives a delayed (`after`) transition
// with the production SystemClock (no FakeClock): entering "armed" schedules a
// real timer, and after a tiny real wait the Scheduler's Tick reads the wall clock
// (systemClock.Now), finds the timer due, and fires the delayed event so the
// instance lands in the target state. This is the only test that exercises the
// real SystemClock-backed elapse path the fake-clock tests bypass.
func TestSystemClock_DrivesRealAfterTransition(t *testing.T) {
	const delay = 5 * time.Millisecond
	m := state.Forge[string, string, *trec]("realtimed").
		State("idle").
		State("armed").
		State("fired").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("go").GoTo("armed").
		Transition("armed").After(delay).On("elapsed").GoTo("fired").
		Quench()

	// Wire the production SystemClock explicitly to pin the SystemClock() entry
	// point; omitting WithClock would default to the same systemClock{}, so this
	// drives the real wall-clock path either way.
	clk := state.SystemClock()
	inst := m.Cast(&trec{}, state.WithInitialState("idle"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	ctx := context.Background()

	res := inst.Fire(ctx, "go")
	sch.Absorb(ctx, res.Effects)
	if inst.Current() != "armed" {
		t.Fatalf("after go, want armed, got %q", inst.Current())
	}
	wantID := state.ScheduleID("realtimed", "armed", 0)
	if !sch.HasPending(wantID) {
		t.Fatalf("entering armed should arm timer %q; pending=%d", wantID, sch.Pending())
	}

	// Before the real delay elapses, ticking fires nothing (the wall clock has not
	// advanced past the due time yet). This is racy if the machine is slow, so we
	// only assert the negative immediately after arming.
	if fired := sch.Tick(ctx); len(fired) != 0 {
		t.Fatalf("tick before real delay fired %d events", len(fired))
	}
	if inst.Current() != "armed" {
		t.Fatalf("before real delay, want armed, got %q", inst.Current())
	}

	// Wait for real time to pass the delay (with margin), then Tick: systemClock.Now
	// now reports a time at/after the due time, so the timer is due and fires.
	deadline := time.Now().Add(time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("real after-transition did not fire within 1s; current=%q pending=%d",
				inst.Current(), sch.Pending())
		}
		time.Sleep(2 * time.Millisecond)
		fired := sch.Tick(ctx)
		if len(fired) > 0 {
			if len(fired) != 1 {
				t.Fatalf("tick past real delay fired %d events, want 1", len(fired))
			}
			break
		}
	}

	if inst.Current() != "fired" {
		t.Fatalf("after real delay, want fired, got %q", inst.Current())
	}
	if sch.Pending() != 0 {
		t.Fatalf("timer should be consumed after firing; pending=%d", sch.Pending())
	}
}

// TestSystemClock_NowTracksWallClock asserts SystemClock().Now reports the real
// wall clock: two reads bracketing a real sleep are non-decreasing and the second
// is at least the slept duration after the first.
func TestSystemClock_NowTracksWallClock(t *testing.T) {
	clk := state.SystemClock()
	const pause = 5 * time.Millisecond

	before := clk.Now()
	wall := time.Now()
	if d := before.Sub(wall); d < -time.Second || d > time.Second {
		t.Fatalf("SystemClock.Now = %v drifts from time.Now = %v by %v, want near-zero", before, wall, d)
	}

	time.Sleep(pause)
	after := clk.Now()
	if !after.After(before) {
		t.Fatalf("SystemClock.Now did not advance: before=%v after=%v", before, after)
	}
	if elapsed := after.Sub(before); elapsed < pause {
		t.Fatalf("SystemClock.Now advanced %v across a %v sleep, want >= %v", elapsed, pause, pause)
	}
}

// TestSystemClock_AfterFiresAfterDelay asserts SystemClock().After returns a
// channel that receives once a real duration elapses (mirroring time.After), the
// method the Scheduler conformance contract relies on for a host that selects on
// the channel rather than polling Now+Tick.
func TestSystemClock_AfterFiresAfterDelay(t *testing.T) {
	clk := state.SystemClock()
	const delay = 5 * time.Millisecond

	start := time.Now()
	ch := clk.After(delay)

	select {
	case got := <-ch:
		if elapsed := time.Since(start); elapsed < delay {
			t.Fatalf("After channel fired after %v, want >= %v", elapsed, delay)
		}
		// time.After delivers the instant the timer fired; it must be at/after start.
		if got.Before(start) {
			t.Fatalf("After delivered instant %v before call time %v", got, start)
		}
	case <-time.After(time.Second):
		t.Fatal("SystemClock.After channel did not fire within 1s")
	}
}
