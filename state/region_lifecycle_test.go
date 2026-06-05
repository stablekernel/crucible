package state_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// These tests cover on-entry and on-exit lifecycle effects (after / invoke /
// actor) for states entered *inside* a parallel region. The region-entry path
// (applyRegionTransition) must emit the same ScheduleAfter / StartService /
// SpawnActor effects on entry — and the symmetric CancelScheduled / StopService
// / StopActor effects on exit — that the normal entry/exit cascade emits, so a
// timer/service/actor declared on a region substate actually starts and stops.

// regionAfterMachine builds a machine that enters a parallel state "par" on the
// "go" event. Region "work" starts in "wIdle" and, on "arm", transitions to
// "wArmed", which declares a 5s `after` timer firing "elapsed" -> "wFired". A
// "release" event moves "wArmed" -> "wIdle" early, exercising auto-cancel-on-
// exit. Region "side" holds a single flat leaf so the parallel state stays
// active while "work" advances.
func regionAfterMachine() *state.Machine[string, string, *trec] {
	return state.Forge[string, string, *trec]("region-after").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("work").
		Initial("wIdle").
		SubState("wIdle").
		SubState("wArmed").
		SubState("wFired").
		EndRegion().
		Region("side").
		Initial("sIdle").
		SubState("sIdle").
		EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(*trec) string { return "off" }).
		// Region-internal transitions on "work": arm/release/elapsed.
		Transition("wIdle").On("arm").GoTo("wArmed").
		Transition("wArmed").After(5 * time.Second).On("elapsed").GoTo("wFired").
		Transition("wArmed").On("release").GoTo("wIdle").
		Quench()
}

// TestRegion_AfterArmsAndFires proves a state entered inside a parallel region
// arms its `after` timer and the FakeClock driver fires it: entering "wArmed"
// via a region transition emits ScheduleAfter, advancing past the delay fires
// "elapsed", and the region leaf advances to "wFired".
func TestRegion_AfterArmsAndFires(t *testing.T) {
	m := regionAfterMachine()
	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("off"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	ctx := context.Background()

	// Enter the parallel state; neither region substate arms a timer yet.
	sch.Absorb(ctx, inst.Fire(ctx, "go").Effects)

	// A region-internal transition enters "wArmed", which declares the `after`.
	res := inst.Fire(ctx, "arm")
	sch.Absorb(ctx, res.Effects)

	wantID := state.ScheduleID("region-after", "wArmed", 0)
	if !sch.HasPending(wantID) {
		t.Fatalf("entering region substate wArmed should arm timer %q; pending=%d", wantID, sch.Pending())
	}
	assertScheduleEffect(t, res.Effects, wantID, 5*time.Second, "elapsed")
	if !configHas(inst, "wArmed") {
		t.Fatalf("configuration = %v, want wArmed active", inst.Configuration())
	}

	// Before the delay: nothing fires.
	clk.Advance(4 * time.Second)
	if fired := sch.Tick(ctx); len(fired) != 0 {
		t.Fatalf("tick before delay fired %d events", len(fired))
	}

	// Past the delay: the region timer fires "elapsed" and the region advances.
	clk.Advance(2 * time.Second)
	if fired := sch.Tick(ctx); len(fired) != 1 {
		t.Fatalf("tick past delay want 1 fired event, got %d", len(fired))
	}
	if !configHas(inst, "wFired") {
		t.Fatalf("after delay configuration = %v, want wFired active", inst.Configuration())
	}
	if sch.Pending() != 0 {
		t.Fatalf("region timer should be consumed; pending=%d", sch.Pending())
	}
}

// TestRegion_AfterCanceledOnExit proves auto-cancel-on-exit on the region path:
// leaving "wArmed" before the delay emits CancelScheduled and the timer never
// fires.
func TestRegion_AfterCanceledOnExit(t *testing.T) {
	m := regionAfterMachine()
	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(&trec{}, state.WithInitialState("off"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	ctx := context.Background()

	sch.Absorb(ctx, inst.Fire(ctx, "go").Effects)
	sch.Absorb(ctx, inst.Fire(ctx, "arm").Effects)
	id := state.ScheduleID("region-after", "wArmed", 0)
	if !sch.HasPending(id) {
		t.Fatalf("timer %q should be armed after entering wArmed", id)
	}

	// Leave wArmed via a region transition before the delay.
	rel := inst.Fire(ctx, "release")
	assertCancelEffect(t, rel.Effects, id)
	sch.Absorb(ctx, rel.Effects)
	if sch.HasPending(id) || sch.Pending() != 0 {
		t.Fatalf("region timer should be auto-canceled on exit; pending=%d", sch.Pending())
	}

	// Advancing past the original delay must not fire anything.
	clk.Advance(time.Hour)
	if fired := sch.Tick(ctx); len(fired) != 0 {
		t.Fatalf("canceled region timer fired %d events", len(fired))
	}
	if !configHas(inst, "wIdle") {
		t.Fatalf("after release configuration = %v, want wIdle active", inst.Configuration())
	}
}

// regionInvokeMachine builds a machine whose parallel region substate "wLoading"
// invokes the "fetch" service: on success it fires "ok" -> "wReady"; on failure
// "fail" -> "wErrored". A "cancel" event exits "wLoading" early, exercising auto-
// stop-on-exit.
func regionInvokeMachine() *state.Machine[string, string, *trec] {
	return state.Forge[string, string, *trec]("region-loader").
		Service("fetch", func(context.Context, state.ServiceCtx[*trec]) (any, error) {
			return "payload", nil
		}).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("work").
		Initial("wIdle").
		SubState("wIdle").
		SubState("wLoading").Invoke("fetch", state.WithInvokeOnDone("ok"), state.WithInvokeOnError("fail")).
		SubState("wReady").
		SubState("wErrored").
		EndRegion().
		Region("side").
		Initial("sIdle").
		SubState("sIdle").
		EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(*trec) string { return "off" }).
		Transition("wIdle").On("load").GoTo("wLoading").
		Transition("wLoading").On("ok").GoTo("wReady").
		Transition("wLoading").On("fail").GoTo("wErrored").
		Transition("wLoading").On("cancel").GoTo("wIdle").
		Quench()
}

// TestRegion_InvokeStartsAndRoutesOnDone proves a region substate's invoked
// service starts and routes onDone: entering "wLoading" via a region transition
// emits StartService, settling it done fires "ok", and the region lands in
// "wReady".
func TestRegion_InvokeStartsAndRoutesOnDone(t *testing.T) {
	m := regionInvokeMachine()
	inst := m.Cast(&trec{}, state.WithInitialState("off"))
	run := state.NewServiceRunner(inst, nil)
	ctx := context.Background()

	run.Absorb(ctx, inst.Fire(ctx, "go").Effects)

	res := inst.Fire(ctx, "load")
	run.Absorb(ctx, res.Effects)

	id := state.InvokeID("region-loader", "wLoading", 0)
	if !run.HasPending(id) {
		t.Fatalf("entering region substate wLoading should start service %q; pending=%d", id, run.Pending())
	}
	assertStartEffect(t, res.Effects, id, "fetch", "ok", "fail")

	if _, ok := run.SettleDone(ctx, id, "payload"); !ok {
		t.Fatalf("SettleDone reported no in-flight service %q", id)
	}
	if !configHas(inst, "wReady") {
		t.Fatalf("after onDone configuration = %v, want wReady active", inst.Configuration())
	}
	if run.Pending() != 0 {
		t.Fatalf("service should be settled; pending=%d", run.Pending())
	}
}

// TestRegion_InvokeRoutesOnError proves the onError routing on the region path: a
// failed service settles "fail" and the region lands in "wErrored".
func TestRegion_InvokeRoutesOnError(t *testing.T) {
	m := regionInvokeMachine()
	inst := m.Cast(&trec{}, state.WithInitialState("off"))
	run := state.NewServiceRunner(inst, nil)
	ctx := context.Background()

	run.Absorb(ctx, inst.Fire(ctx, "go").Effects)
	run.Absorb(ctx, inst.Fire(ctx, "load").Effects)
	id := state.InvokeID("region-loader", "wLoading", 0)

	if _, ok := run.SettleError(ctx, id, errors.New("boom")); !ok {
		t.Fatalf("SettleError reported no in-flight service %q", id)
	}
	if !configHas(inst, "wErrored") {
		t.Fatalf("after onError configuration = %v, want wErrored active", inst.Configuration())
	}
}

// TestRegion_InvokeStoppedOnExit proves auto-stop-on-exit on the region path:
// leaving "wLoading" before the service completes emits StopService and the
// service can no longer settle.
func TestRegion_InvokeStoppedOnExit(t *testing.T) {
	m := regionInvokeMachine()
	inst := m.Cast(&trec{}, state.WithInitialState("off"))
	run := state.NewServiceRunner(inst, nil)
	ctx := context.Background()

	run.Absorb(ctx, inst.Fire(ctx, "go").Effects)
	run.Absorb(ctx, inst.Fire(ctx, "load").Effects)
	id := state.InvokeID("region-loader", "wLoading", 0)
	if !run.HasPending(id) {
		t.Fatalf("service %q should be in flight after entering wLoading", id)
	}

	cancel := inst.Fire(ctx, "cancel")
	assertStopEffect(t, cancel.Effects, id)
	run.Absorb(ctx, cancel.Effects)
	if run.HasPending(id) || run.Pending() != 0 {
		t.Fatalf("service should be auto-stopped on exit; pending=%d", run.Pending())
	}
	if _, ok := run.SettleDone(ctx, id, "payload"); ok {
		t.Fatal("stopped region service should not settle")
	}
	if !configHas(inst, "wIdle") {
		t.Fatalf("after cancel configuration = %v, want wIdle active", inst.Configuration())
	}
}

// regionActorMachine builds a parent whose parallel region substate "wSuper"
// invokes a child-MACHINE actor: on the child's completion it fires "childDone"
// -> "wDone"; a "cancel" event exits "wSuper" early, exercising auto-stop-on-
// exit.
func regionActorMachine() *state.Machine[string, string, *trec] {
	return state.Forge[string, string, *trec]("region-parent").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Region("work").
		Initial("wIdle").
		SubState("wIdle").
		SubState("wSuper").InvokeActor("child", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childErr")).
		SubState("wDone").
		EndRegion().
		Region("side").
		Initial("sIdle").
		SubState("sIdle").
		EndRegion().
		EndSuperState().
		Initial("off").
		CurrentStateFn(func(*trec) string { return "off" }).
		Transition("wIdle").On("supervise").GoTo("wSuper").
		Transition("wSuper").On("childDone").GoTo("wDone").
		Transition("wSuper").On("cancel").GoTo("wIdle").
		Quench()
}

// TestRegion_ActorSpawnsAndRoutesOnDone proves a region substate's invoked actor
// spawns and routes onDone: entering "wSuper" via a region transition emits
// SpawnActor, finishing the child routes "childDone", and the region lands in
// "wDone".
func TestRegion_ActorSpawnsAndRoutesOnDone(t *testing.T) {
	m := regionActorMachine()
	parent := m.Cast(&trec{}, state.WithInitialState("off"))
	sys := state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()

	sys.Absorb(ctx, parent.Fire(ctx, "go").Effects)

	res := parent.Fire(ctx, "supervise")
	sys.Absorb(ctx, res.Effects)

	id := state.ActorID(m.Name(), "wSuper", 0)
	if !sys.IsRunning(id) {
		t.Fatalf("entering region substate wSuper should spawn actor %q", id)
	}
	if sys.Running() != 1 {
		t.Fatalf("running actors = %d, want 1", sys.Running())
	}

	ref, ok := sys.Ref(id)
	if !ok {
		t.Fatalf("no actor ref for id %q", id)
	}
	if !sys.Deliver(ctx, ref, "finish") {
		t.Fatal("Deliver returned false; actor not running")
	}
	if !configHas(parent, "wDone") {
		t.Fatalf("after childDone configuration = %v, want wDone active", parent.Configuration())
	}
	if sys.Running() != 0 {
		t.Fatalf("running actors after completion = %d, want 0", sys.Running())
	}
}

// TestRegion_ActorStoppedOnExit proves auto-stop-on-exit on the region path:
// leaving "wSuper" before the child completes stops the actor.
func TestRegion_ActorStoppedOnExit(t *testing.T) {
	m := regionActorMachine()
	parent := m.Cast(&trec{}, state.WithInitialState("off"))
	sys := state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()

	sys.Absorb(ctx, parent.Fire(ctx, "go").Effects)
	sys.Absorb(ctx, parent.Fire(ctx, "supervise").Effects)
	id := state.ActorID(m.Name(), "wSuper", 0)
	if !sys.IsRunning(id) {
		t.Fatal("child actor not running after region spawn")
	}

	sys.Absorb(ctx, parent.Fire(ctx, "cancel").Effects)
	if sys.IsRunning(id) {
		t.Fatal("child actor still running after exiting region substate wSuper")
	}
	if sys.Running() != 0 {
		t.Fatalf("running = %d, want 0 after auto-stop-on-exit", sys.Running())
	}
}

// configHas reports whether the instance's active configuration contains leaf.
func configHas(inst *state.Instance[string, string, *trec], leaf string) bool {
	for _, l := range inst.Configuration() {
		if l == leaf {
			return true
		}
	}
	return false
}
