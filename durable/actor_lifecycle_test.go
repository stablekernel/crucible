package durable_test

// The Recover tests for actors whose lifecycle a step journals: an actor spawned and
// stopped within one step, and failed spawns on re-entry, by a Fire, a settle, or a
// recovered Handle, which Recover retries as it always has.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// driverChild: on "go" it sends the parent "toZ" then "kill" in one step.
func driverChild() state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		child := state.ForgeFor[actorCtx]("driver").
			State("ready").
			Initial("ready").
			Transition("ready").On("go").GoTo("ready").SendParent("toZ").SendParent("kill").
			Quench()
		return state.NewActor(child.Cast(actorCtx{}, state.WithInitialState[string]("ready")), nil), nil
	}
}

func flipMachine() *state.Machine[string, string, actorCtx] {
	return state.ForgeFor[actorCtx]("flip").
		Actor("spawnDriver").
		Actor("spawnChild").
		State("y").InvokeActor("spawnDriver").
		State("z").InvokeActor("spawnChild").
		Initial("y").
		Transition("y").On("toZ").GoTo("z").
		Transition("z").On("kill").GoTo("z").StopActor(state.ActorID("flip", "z", 0)).
		Transition("z").On("poke").GoTo("z").
		Quench()
}

// An actor spawned and stopped within one delivery is never seen at a step boundary,
// so the ended set misses it and Recover spawns it though live has it stopped.
func TestRecover_ActorSpawnedAndStoppedInOneStep_StaysEnded(t *testing.T) {
	ctx := context.Background()
	childID := state.ActorID("flip", "z", 0)
	driverID := state.ActorID("flip", "y", 0)
	for _, every := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("checkpoint every %d", every), func(t *testing.T) {
			var runs int64
			pal := map[string]state.ActorBehavior{"spawnDriver": driverChild(), "spawnChild": counterChild(&runs)}
			opts := []durable.Option[string, string, actorCtx]{durable.WithActorPalette[string, string, actorCtx](pal)}
			if every > 0 {
				opts = append(opts, durable.WithCheckpointEvery[string, string, actorCtx](every))
			}
			m := flipMachine()
			store := durable.NewMemStore()
			id := durable.InstanceID(fmt.Sprintf("flip-%d", every))
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("y"))
			if err != nil {
				t.Fatal(err)
			}
			ref, ok := h.ActorRef(driverID)
			if !ok {
				t.Fatal("no driver")
			}
			if _, err := h.DeliverToActor(ctx, ref, "go"); err != nil {
				t.Fatal(err)
			}
			if got := h.Instance().Current(); got != "z" {
				t.Fatalf("live state %v", got)
			}
			if _, ok := h.ActorRef(childID); ok {
				t.Fatal("live: child should be stopped")
			}
			rec, err := durable.Recover(ctx, m, store, id, opts...)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := rec.ActorRef(childID); ok {
				t.Error("Recover: child is running, live had it stopped")
			}
			// A step on the recovered Handle, then a second Recover.
			if _, err := rec.Fire(ctx, "poke"); err != nil {
				t.Fatal(err)
			}
			again, err := durable.Recover(ctx, m, store, id, opts...)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := again.ActorRef(childID); ok {
				t.Error("second Recover: child is running, live had it stopped")
			}
		})
	}
}

type probeCtx struct {
	Notes []string `json:"notes"`
}

func probeChild() state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		child := state.ForgeFor[probeCtx]("child").
			State("working").State("done").Final().Initial("working").
			Transition("working").On("finish").GoTo("done").Quench()
		inst := child.Cast(probeCtx{}, state.WithInitialState[string]("working"))
		return state.NewActor(inst, func(*state.Instance[string, string, probeCtx]) any { return "out" }), nil
	}
}

func probeMachine() *state.Machine[string, string, probeCtx] {
	return state.ForgeFor[probeCtx]("probe").
		Actor("spawnChild").
		State("working").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone")).
		State("done").Final().
		Initial("working").
		Transition("working").On("re").GoTo("working").Reenter().
		Transition("working").On("poke").GoTo("working").
		Transition("working").On("childDone").GoTo("done").
		Quench()
}

// First spawn succeeds, later ones fail.
func TestRecover_RetriesAFailedReentrySpawn(t *testing.T) {
	ctx := context.Background()
	m := probeMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("p1")
	var n int64
	pal := map[string]state.ActorBehavior{"spawnChild": func(in map[string]any) (state.ActorInstance, error) {
		if atomic.AddInt64(&n, 1) > 1 {
			return nil, errors.New("can't start")
		}
		return probeChild()(in)
	}}
	h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, probeCtx](pal)).
		Start(ctx, id, probeCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Fire(ctx, "poke"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Fire(ctx, "re"); err != nil {
		t.Fatal(err)
	}
	aid := state.ActorID("probe", "working", 0)
	if _, ok := h.ActorRef(aid); ok {
		t.Fatal("setup: respawn should have failed")
	}
	good := map[string]state.ActorBehavior{"spawnChild": probeChild()}
	rec, err := durable.Recover(ctx, m, store, id, durable.WithActorPalette[string, string, probeCtx](good))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rec.ActorRef(aid); !ok {
		t.Fatal("Recover didn't retry the actor whose re-entry spawn failed live")
	}
}

// Live: running at crash. Recover #1 spawn fails; a step; Recover #2 succeeds.
func TestRecover_RetriesASpawnThatFailedAtRecover(t *testing.T) {
	ctx := context.Background()
	m := probeMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("p2")
	good := map[string]state.ActorBehavior{"spawnChild": probeChild()}
	bad := map[string]state.ActorBehavior{"spawnChild": func(map[string]any) (state.ActorInstance, error) { return nil, errors.New("x") }}
	h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, probeCtx](good)).
		Start(ctx, id, probeCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Fire(ctx, "poke"); err != nil {
		t.Fatal(err)
	}
	r1, err := durable.Recover(ctx, m, store, id, durable.WithActorPalette[string, string, probeCtx](bad))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r1.Fire(ctx, "poke"); err != nil {
		t.Fatal(err)
	}
	r2, err := durable.Recover(ctx, m, store, id, durable.WithActorPalette[string, string, probeCtx](good))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r2.ActorRef(state.ActorID("probe", "working", 0)); !ok {
		t.Fatal("second Recover didn't retry")
	}
}

// Running at crash, recovered handle re-enters and its respawn fails, then Recover.
func TestRecover_RetriesAFailedReentrySpawnOnARecoveredHandle(t *testing.T) {
	ctx := context.Background()
	m := probeMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("p3")
	good := map[string]state.ActorBehavior{"spawnChild": probeChild()}
	var n int64
	once := map[string]state.ActorBehavior{"spawnChild": func(in map[string]any) (state.ActorInstance, error) {
		if atomic.AddInt64(&n, 1) > 1 {
			return nil, errors.New("can't start")
		}
		return probeChild()(in)
	}}
	h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, probeCtx](good)).
		Start(ctx, id, probeCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Fire(ctx, "poke"); err != nil {
		t.Fatal(err)
	}
	r1, err := durable.Recover(ctx, m, store, id, durable.WithActorPalette[string, string, probeCtx](once))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r1.Fire(ctx, "re"); err != nil {
		t.Fatal(err)
	}
	r2, err := durable.Recover(ctx, m, store, id, durable.WithActorPalette[string, string, probeCtx](good))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r2.ActorRef(state.ActorID("probe", "working", 0)); !ok {
		t.Fatal("Recover didn't retry")
	}
}

func TestRecover_RetriesASpawnThatFailedWhenASettleReentered(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[probeCtx]("probe2").
		Actor("spawnChild").
		State("working").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone")).
		Initial("working").
		Transition("working").On("childDone").GoTo("working").Reenter().
		Quench()
	store := durable.NewMemStore()
	id := durable.InstanceID("p4")
	var n int64
	pal := map[string]state.ActorBehavior{"spawnChild": func(in map[string]any) (state.ActorInstance, error) {
		if atomic.AddInt64(&n, 1) > 1 {
			return nil, errors.New("can't start")
		}
		return probeChild()(in)
	}}
	h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, probeCtx](pal)).
		Start(ctx, id, probeCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatal(err)
	}
	aid := state.ActorID("probe2", "working", 0)
	ref, _ := h.ActorRef(aid)
	if found, err := h.DeliverToActor(ctx, ref, "finish"); !found || err != nil {
		t.Fatalf("deliver: %v %v", found, err)
	}
	if _, ok := h.ActorRef(aid); ok {
		t.Fatal("setup: respawn should have failed")
	}
	good := map[string]state.ActorBehavior{"spawnChild": probeChild()}
	rec, err := durable.Recover(ctx, m, store, id, durable.WithActorPalette[string, string, probeCtx](good))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rec.ActorRef(aid); !ok {
		t.Fatal("Recover didn't retry the actor whose re-entry spawn (via settle) failed live")
	}
}

type rsCtx struct {
	N int `json:"n"`
}

func rsChild() state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		c := state.ForgeFor[rsCtx]("rschild").
			State("w").Initial("w").
			Transition("w").On("go").GoTo("w").SendParent("re").SendParent("re").
			Quench()
		return state.NewActor(c.Cast(rsCtx{}, state.WithInitialState[string]("w")), nil), nil
	}
}

func rsMachine() *state.Machine[string, string, rsCtx] {
	return state.ForgeFor[rsCtx]("rs").
		Actor("spawnChild").
		State("working").InvokeActor("spawnChild").
		Initial("working").
		Transition("working").On("re").GoTo("working").Reenter().
		Transition("working").On("poke").GoTo("working").
		Quench()
}

// In one delivery the state is re-entered twice: the first respawn starts, the
// second fails. Live ends with no actor; Recover should retry it.
func TestRecover_RetriesAnActorWhoseSecondReentrySpawnFailed(t *testing.T) {
	for _, poke := range []bool{false, true} {
		ctx := context.Background()
		m := rsMachine()
		store := durable.NewMemStore()
		id := durable.InstanceID("rs")
		var n int64
		pal := map[string]state.ActorBehavior{"spawnChild": func(in map[string]any) (state.ActorInstance, error) {
			if atomic.AddInt64(&n, 1) > 2 {
				return nil, errors.New("can't start")
			}
			return rsChild()(in)
		}}
		h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, rsCtx](pal)).
			Start(ctx, id, rsCtx{}, state.WithInitialState("working"))
		if err != nil {
			t.Fatal(err)
		}
		aid := state.ActorID("rs", "working", 0)
		ref, ok := h.ActorRef(aid)
		if !ok {
			t.Fatal("no child")
		}
		if _, err := h.DeliverToActor(ctx, ref, "go"); err != nil {
			t.Fatal(err)
		}
		if got := atomic.LoadInt64(&n); got != 3 {
			t.Fatalf("setup: spawns %d, want 3", got)
		}
		if _, ok := h.ActorRef(aid); ok {
			t.Fatal("setup: last respawn should have failed")
		}
		if poke {
			if _, err := h.Fire(ctx, "poke"); err != nil {
				t.Fatal(err)
			}
		}
		good := map[string]state.ActorBehavior{"spawnChild": rsChild()}
		rec, err := durable.Recover(ctx, m, store, id, durable.WithActorPalette[string, string, rsCtx](good))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := rec.ActorRef(aid); !ok {
			t.Errorf("poke=%v: Recover didn't retry the actor whose last respawn failed", poke)
		}
	}
}

func driver3() state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		child := state.ForgeFor[actorCtx]("driver").State("ready").Initial("ready").
			Transition("ready").On("go").GoTo("ready").SendParent("toZ").SendParent("kill").SendParent("re").
			Quench()
		return state.NewActor(child.Cast(actorCtx{}, state.WithInitialState[string]("ready")), nil), nil
	}
}

// Non-blocker probe: in one delivery the z child spawns, is stopped, and its re-entry
// spawn fails. Live: not running. Is it retried by Recover (the stated rule for a failed
// spawn), or recorded as ended?
func TestRecover_RetriesAnActorWhoseLatestSpawnInAStepFailed(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[actorCtx]("flip").
		Actor("spawnDriver").Actor("spawnChild").
		State("y").InvokeActor("spawnDriver").
		State("z").InvokeActor("spawnChild").
		Initial("y").
		Transition("y").On("toZ").GoTo("z").
		Transition("z").On("kill").GoTo("z").StopActor(state.ActorID("flip", "z", 0)).
		Transition("z").On("re").GoTo("z").Reenter().
		Quench()
	var n, runs int64
	pal := map[string]state.ActorBehavior{"spawnDriver": driver3(), "spawnChild": func(in map[string]any) (state.ActorInstance, error) {
		if atomic.AddInt64(&n, 1) > 1 {
			return nil, errors.New("no")
		}
		return counterChild(&runs)(in)
	}}
	store := durable.NewMemStore()
	h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, actorCtx](pal)).Start(ctx, "x", actorCtx{}, state.WithInitialState("y"))
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := h.ActorRef(state.ActorID("flip", "y", 0))
	if _, err := h.DeliverToActor(ctx, ref, "go"); err != nil {
		t.Fatal(err)
	}
	cid := state.ActorID("flip", "z", 0)
	good := map[string]state.ActorBehavior{"spawnDriver": driver3(), "spawnChild": counterChild(&runs)}
	rec, err := durable.Recover(ctx, m, store, "x", durable.WithActorPalette[string, string, actorCtx](good))
	if err != nil {
		t.Fatal(err)
	}
	if _, rr := rec.ActorRef(cid); !rr {
		t.Fatal("Recover didn't retry the actor whose latest spawn failed")
	}
}

// TestRecover_SpawnsASuperstatesActor proves an actor a compound state declares runs
// while one of its children is active: Start spawns it, and so does Recover after a
// step moved between the children.
func TestRecover_SpawnsASuperstatesActor(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[actorCtx]("nest").
		Actor("spawnChild").
		SuperState("working").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone")).
		SubState("a").
		SubState("b").
		Initial("a").
		EndSuperState().
		State("done").Final().
		Initial("working").
		Transition("a").On("next").GoTo("b").
		Transition("working").On("childDone").GoTo("done").
		Quench()
	var runs int64
	opts := []durable.Option[string, string, actorCtx]{durable.WithActorPalette[string, string, actorCtx](counterPalette(&runs))}
	store := durable.NewMemStore()
	id := durable.InstanceID("nest")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	childID := state.ActorID("nest", "working", 0)
	if _, ok := h.ActorRef(childID); !ok {
		t.Fatal("Start didn't spawn the superstate's actor")
	}
	if _, err = h.Fire(ctx, "next"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if found, _ := recovered.DeliverToActor(ctx, state.ActorRef{ID: childID}, "finish"); !found {
		t.Fatal("Recover didn't spawn the superstate's actor")
	}
	if got := recovered.Instance().Current(); got != "done" {
		t.Fatalf("state = %q, want done after the actor finished", got)
	}
}

// greeter is an actor that sends its parent "hello" as it spawns, then keeps running.
type greeter struct{ sent bool }

func (g *greeter) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (g *greeter) ChildEffects() []state.Effect {
	if g.sent {
		return nil
	}
	g.sent = true
	return []state.Effect{state.SendParent{Event: "hello"}}
}
func (g *greeter) Output() any { return nil }

// TestRecover_AppliesASpawnMessageOnce proves a message a running actor sends its parent
// as it spawns isn't journaled with the step that spawned it: Recover spawns the actor
// again, which sends it again, so journaling it too would apply it twice.
func TestRecover_AppliesASpawnMessageOnce(t *testing.T) {
	ctx := context.Background()
	note := func(s string) func(state.AssignCtx[actorCtx]) actorCtx {
		return func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			c.Notes = append(c.Notes, s)
			return c
		}
	}
	opts := []durable.Option[string, string, actorCtx]{durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
		"greeter": func(map[string]any) (state.ActorInstance, error) { return &greeter{}, nil },
	})}
	for _, initial := range []string{"idle", "greeting"} {
		t.Run("from "+initial, func(t *testing.T) {
			m := state.ForgeFor[actorCtx]("greet").
				Reducer("noteHello", note("hello")).
				Reducer("notePoke", note("poke")).
				Actor("greeter").
				State("idle").
				State("greeting").InvokeActor("greeter").
				Initial(initial).
				Transition("idle").On("go").GoTo("greeting").
				Transition("greeting").On("hello").GoTo("greeting").Assign("noteHello").
				Transition("greeting").On("poke").GoTo("greeting").Assign("notePoke").
				Quench()
			store, id := durable.NewMemStore(), durable.InstanceID("greet")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState(initial))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if initial == "idle" {
				if _, err = h.Fire(ctx, "go"); err != nil {
					t.Fatalf("Fire(go): %v", err)
				}
			}
			if _, err = h.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke): %v", err)
			}
			recovered, err := durable.Recover(ctx, m, store, id, opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			hellos := 0
			for _, n := range recovered.Instance().Entity().Notes {
				if n == "hello" {
					hellos++
				}
			}
			if hellos != 1 {
				t.Fatalf("recovered notes = %v, want the spawn's hello once", recovered.Instance().Entity().Notes)
			}
		})
	}
}
