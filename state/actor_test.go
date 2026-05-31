package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// childEntity is the entity a child-machine actor is bound to. result records the
// output the child exposes on completion.
type childEntity struct {
	result string
}

// childMachine builds a flat child machine: it starts in "working" and reaches the
// final "done" state on the "finish" event. On entering "done" it records a result
// on its entity, which the actor adapter surfaces as the child's output.
func childMachine() *state.Machine[string, string, *childEntity] {
	return state.Forge[string, string, *childEntity]("child").
		Action("record", func(c state.ActionCtx[*childEntity]) (state.Effect, error) {
			c.Entity.result = "child-output"
			return nil, nil
		}).
		State("working").
		State("done").Final().OnEntry("record").
		Initial("working").
		Transition("working").On("finish").GoTo("done").
		Quench()
}

// childBehavior returns an ActorBehavior that Casts a fresh child machine per
// spawn, exposing the child entity's result as the actor output.
func childBehavior() state.ActorBehavior {
	cm := childMachine()
	return func(input map[string]any) (state.ActorInstance, error) {
		inst := cm.Cast(&childEntity{}, state.WithInitialState("working"))
		return state.NewActor(inst, func(i *state.Instance[string, string, *childEntity]) any {
			return i.Entity().result
		}), nil
	}
}

// parentInvokeMachine builds a parent machine whose "supervising" state invokes a
// child MACHINE actor: on the child's done it fires "childDone" and moves to
// "complete"; a plain "cancel" exits "supervising" early (to "idle"), exercising
// auto-stop-on-exit. The childDone action records the child's output read from the
// system.
func parentInvokeMachine(sys **state.ActorSystem[string, string, *trec]) *state.Machine[string, string, *trec] {
	return state.Forge[string, string, *trec]("parent").
		Action("captureOutput", func(c state.ActionCtx[*trec]) (state.Effect, error) {
			if o, ok := (*sys).LastOutput(); ok {
				c.Entity.notes = append(c.Entity.notes, "output:"+o.(string))
			}
			return nil, nil
		}).
		State("idle").
		State("supervising").InvokeActor("child", "childDone", "childErr").
		State("complete").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childDone").GoTo("complete").Do("captureOutput").
		Transition("supervising").On("cancel").GoTo("idle").
		Quench()
}

// TestActor_InvokeChildMachine_RoutesOnDone drives a child-machine actor to
// completion: entering supervising spawns the child, delivering "finish" steps the
// child to its final state, and the child's completion routes the parent's
// childDone event so the parent lands in complete with the child output captured.
func TestActor_InvokeChildMachine_RoutesOnDone(t *testing.T) {
	var sys *state.ActorSystem[string, string, *trec]
	m := parentInvokeMachine(&sys)
	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("idle"))
	sys = state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()

	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q, want supervising", parent.Current())
	}
	if sys.Running() != 1 {
		t.Fatalf("running actors = %d, want 1", sys.Running())
	}

	id := state.ActorID(m.Name(), "supervising", 0)
	ref, ok := sys.Ref(id)
	if !ok {
		t.Fatalf("no actor ref for id %q", id)
	}

	// Deliver the finishing event to the spawned actor; it completes and routes
	// childDone through the parent.
	if !sys.Deliver(ctx, ref, "finish") {
		t.Fatal("Deliver returned false; actor not running")
	}
	if parent.Current() != "complete" {
		t.Fatalf("parent state = %q, want complete", parent.Current())
	}
	if sys.Running() != 0 {
		t.Fatalf("running actors after completion = %d, want 0", sys.Running())
	}
	if len(entity.notes) != 1 || entity.notes[0] != "output:child-output" {
		t.Fatalf("captured notes = %v, want [output:child-output]", entity.notes)
	}
}

// TestActor_SpawnYieldsUsableRef asserts the dynamic spawn built-in creates an
// actor at runtime and yields a ref the holder can address (deliver to and step).
func TestActor_SpawnYieldsUsableRef(t *testing.T) {
	m := state.Forge[string, string, *trec]("spawner").
		State("idle").
		State("active").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("go").GoTo("active").
		Spawn("child", "worker-1",
			state.WithSpawnOnDone("workerDone"), state.WithSpawnOnError("workerErr")).
		Transition("active").On("workerDone").GoTo("idle").
		Quench()

	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()

	res := parent.Fire(ctx, "go")
	sys.Absorb(ctx, res.Effects)
	ref, ok := sys.Ref("worker-1")
	if !ok {
		t.Fatal("spawn did not yield a usable ref for worker-1")
	}
	if ref.ID != "worker-1" || ref.Src != "child" {
		t.Fatalf("ref = %+v, want ID=worker-1 Src=child", ref)
	}
	// A locally-spawned actor's ref is local: its Node locator is empty. Empty
	// Node is the in-process projection of the opaque-locator shape; a remote
	// host stamps Node additively without changing any local-ref holder.
	if ref.Node != "" {
		t.Fatalf("local ref Node = %q, want empty", ref.Node)
	}
	if sys.Running() != 1 {
		t.Fatalf("running = %d, want 1", sys.Running())
	}

	// The ref is usable: delivering an event steps the actor to completion and
	// routes workerDone back through the parent.
	if !sys.Deliver(ctx, ref, "finish") {
		t.Fatal("Deliver to spawned ref returned false")
	}
	if parent.Current() != "idle" {
		t.Fatalf("parent state = %q, want idle after workerDone", parent.Current())
	}
}

// TestActor_DeliverStepsActor asserts Deliver routes an event into a spawned
// actor's mailbox and steps it (advancing the actor's own state) without yet
// completing it.
func TestActor_DeliverStepsActor(t *testing.T) {
	// A two-step child: working -> midway -> done(final).
	cm := state.Forge[string, string, *childEntity]("twostep").
		State("working").
		State("midway").
		State("done").Final().
		Initial("working").
		Transition("working").On("next").GoTo("midway").
		Transition("midway").On("finish").GoTo("done").
		Quench()
	behavior := func(map[string]any) (state.ActorInstance, error) {
		inst := cm.Cast(&childEntity{}, state.WithInitialState("working"))
		return state.NewActor(inst, nil), nil
	}

	m := parentInvokeMachineWith(cm)
	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).Register("child", behavior)
	ctx := context.Background()

	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	id := state.ActorID(m.Name(), "supervising", 0)
	ref, _ := sys.Ref(id)

	// Deliver "next": the actor steps to midway but is not done, so the parent
	// stays in supervising.
	sys.Deliver(ctx, ref, "next")
	if !sys.IsRunning(id) {
		t.Fatal("actor stopped early after a non-final step")
	}
	if parent.Current() != "supervising" {
		t.Fatalf("parent moved early to %q, want supervising", parent.Current())
	}
	// Deliver "finish": the actor reaches done and routes childDone.
	sys.Deliver(ctx, ref, "finish")
	if parent.Current() != "complete" {
		t.Fatalf("parent state = %q, want complete", parent.Current())
	}
}

// parentInvokeMachineWith builds the parent for a given child machine, without the
// output-capturing action (the child carries no output here).
func parentInvokeMachineWith(_ *state.Machine[string, string, *childEntity]) *state.Machine[string, string, *trec] {
	return state.Forge[string, string, *trec]("parent").
		State("idle").
		State("supervising").InvokeActor("child", "childDone", "childErr").
		State("complete").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childDone").GoTo("complete").
		Transition("supervising").On("cancel").GoTo("idle").
		Quench()
}

// TestActor_StopParentStopsChildren asserts that exiting the supervising state
// (auto-stop-on-exit) stops the running child actor.
func TestActor_StopParentStopsChildren(t *testing.T) {
	var sys *state.ActorSystem[string, string, *trec]
	m := parentInvokeMachine(&sys)
	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("idle"))
	sys = state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()

	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	id := state.ActorID(m.Name(), "supervising", 0)
	if !sys.IsRunning(id) {
		t.Fatal("child actor not running after spawn")
	}

	// Cancel exits supervising before the child completes; the StopActor effect
	// stops the child.
	res = parent.Fire(ctx, "cancel")
	sys.Absorb(ctx, res.Effects)
	if sys.IsRunning(id) {
		t.Fatal("child actor still running after parent exited supervising")
	}
	if sys.Running() != 0 {
		t.Fatalf("running = %d, want 0 after auto-stop-on-exit", sys.Running())
	}
}

// TestActor_StopParentStopsNestedChildren asserts stopping a parent actor
// recursively stops the actors it spawned.
func TestActor_StopParentStopsNestedChildren(t *testing.T) {
	// Grandchild: a trivial child machine.
	grand := childMachine()
	grandBehavior := func(map[string]any) (state.ActorInstance, error) {
		inst := grand.Cast(&childEntity{}, state.WithInitialState("working"))
		return state.NewActor(inst, nil), nil
	}
	// Middle actor: on entering its initial state it invokes the grandchild.
	middle := state.Forge[string, string, *childEntity]("middle").
		State("run").InvokeActor("grand", "gDone", "gErr").
		State("end").Final().
		Initial("run").
		Transition("run").On("stop").GoTo("end").
		Quench()
	middleBehavior := func(map[string]any) (state.ActorInstance, error) {
		inst := middle.Cast(&childEntity{}, state.WithInitialState("run"))
		return state.NewActor(inst, nil), nil
	}

	m := parentInvokeMachineWith(nil)
	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).
		Register("child", middleBehavior).
		Register("grand", grandBehavior)
	ctx := context.Background()

	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if sys.Running() < 2 {
		t.Fatalf("running = %d, want >= 2 (middle + grandchild)", sys.Running())
	}

	// Cancel stops the middle actor; its grandchild is torn down with it.
	res = parent.Fire(ctx, "cancel")
	sys.Absorb(ctx, res.Effects)
	if sys.Running() != 0 {
		t.Fatalf("running = %d, want 0 after parent stop cascades", sys.Running())
	}
}

// TestActor_UnboundSrcRoutesOnError asserts a spawn whose src is unregistered
// settles as an error and routes the parent's onError rather than hanging.
func TestActor_UnboundSrcRoutesOnError(t *testing.T) {
	m := state.Forge[string, string, *trec]("parent").
		State("idle").
		State("supervising").InvokeActor("missing", "childDone", "childErr").
		State("errored").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childErr").GoTo("errored").
		Quench()

	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent) // no behavior registered for "missing"
	ctx := context.Background()

	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if parent.Current() != "errored" {
		t.Fatalf("parent state = %q, want errored (onError routed)", parent.Current())
	}
}

// TestActor_IRRoundTrip asserts an InvokeActor block (kind + systemId + src +
// onDone/onError) round-trips losslessly through ToJSON -> LoadFromJSON, and a
// dynamic Spawn built-in's params survive too.
func TestActor_IRRoundTrip(t *testing.T) {
	m := state.Forge[string, string, *trec]("parent").
		State("idle").
		State("supervising").
		InvokeActor("child", "childDone", "childErr",
			state.WithInvokeID("sup-actor"), state.WithSystemID("supervisor")).
		State("complete").
		Initial("idle").
		Transition("idle").On("go").GoTo("supervising").
		Spawn("child", "dyn-1", state.WithSpawnSystemID("dyn")).
		Transition("supervising").On("childDone").GoTo("complete").
		Quench()

	raw, err := m.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}

	ir, err := state.LoadFromJSON[string, string, *trec](raw)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	m2 := ir.Provide(state.NewRegistry[*trec]()).Quench()

	raw2, err := m2.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON (round 2) err = %v", err)
	}
	if string(raw) != string(raw2) {
		t.Fatalf("IR round-trip diverged:\n%s\n--- vs ---\n%s", raw, raw2)
	}

	// Confirm the actor invocation's kind/systemId survived the round-trip.
	var doc struct {
		States []struct {
			Name   string `json:"name"`
			Invoke []struct {
				Kind     int    `json:"kind"`
				SystemID string `json:"systemId"`
				Src      struct {
					Name string `json:"name"`
				} `json:"src"`
			} `json:"invoke"`
		} `json:"states"`
	}
	if err := json.Unmarshal(raw2, &doc); err != nil {
		t.Fatalf("unmarshal IR err = %v", err)
	}
	var found bool
	for _, s := range doc.States {
		for _, inv := range s.Invoke {
			if inv.Src.Name == "child" {
				found = true
				if inv.Kind != int(state.ActorKindMachine) {
					t.Fatalf("invoke kind = %d, want ActorKindMachine", inv.Kind)
				}
				if inv.SystemID != "supervisor" {
					t.Fatalf("invoke systemId = %q, want supervisor", inv.SystemID)
				}
			}
		}
	}
	if !found {
		t.Fatal("actor invocation not found in round-tripped IR")
	}
}

// TestActor_RefBySystemID asserts an actor spawned with a systemId is addressable
// by that well-known name.
func TestActor_RefBySystemID(t *testing.T) {
	var sys *state.ActorSystem[string, string, *trec]
	m := state.Forge[string, string, *trec]("parent").
		State("idle").
		State("supervising").
		InvokeActor("child", "childDone", "childErr", state.WithSystemID("supervisor")).
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Quench()

	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("idle"))
	sys = state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()

	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	ref, ok := sys.RefBySystemID("supervisor")
	if !ok {
		t.Fatal("actor not addressable by systemId")
	}
	if ref.SystemID != "supervisor" {
		t.Fatalf("ref.SystemID = %q, want supervisor", ref.SystemID)
	}
}

// TestActor_SettleErrorRoutesOnError asserts a host-detected actor failure routes
// the parent's onError.
func TestActor_SettleErrorRoutesOnError(t *testing.T) {
	m := state.Forge[string, string, *trec]("parent").
		State("idle").
		State("supervising").InvokeActor("child", "childDone", "childErr").
		State("errored").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childErr").GoTo("errored").
		Quench()

	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()

	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	id := state.ActorID(m.Name(), "supervising", 0)

	if _, ok := sys.SettleError(ctx, id, errors.New("boom")); !ok {
		t.Fatal("SettleError did not route")
	}
	if parent.Current() != "errored" {
		t.Fatalf("parent state = %q, want errored", parent.Current())
	}
	if sys.LastError() == nil {
		t.Fatal("LastError = nil, want the settled error")
	}
}
