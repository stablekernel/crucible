package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// ctxKey tags the contexts these tests pass, so a callback's Ctx can be traced back to
// the call that supplied it.
type ctxKey struct{}

// tagged returns a context carrying tag.
func tagged(tag string) context.Context {
	return context.WithValue(context.Background(), ctxKey{}, tag)
}

// tagOf reads the tag a callback's Ctx carries, or "" for none.
func tagOf(ctx context.Context) string {
	if ctx == nil {
		return "nil"
	}
	tag, _ := ctx.Value(ctxKey{}).(string)
	return tag
}

// ctxSeen records which tag each kind of callback saw.
type ctxSeen struct {
	Guard, Action, Assign, Entry string
}

// ctxMachine runs a guard, an action and a transition reducer on idle -go-> busy, and
// an entry reducer on entering idle, each recording its Ctx's tag.
func ctxMachine() *state.Machine[string, string, *ctxSeen] {
	return state.ForgeFor[*ctxSeen]("ctx").
		Guard("g", func(c state.GuardCtx[*ctxSeen]) bool {
			c.Entity.Guard = tagOf(c.Ctx)
			return true
		}).
		Action("a", func(c state.ActionCtx[*ctxSeen]) (state.Effect, error) {
			c.Entity.Action = tagOf(c.Ctx)
			return nil, nil
		}).
		Reducer("r", func(c state.AssignCtx[*ctxSeen]) *ctxSeen {
			c.Entity.Assign = tagOf(c.Ctx)
			return c.Entity
		}).
		Reducer("entry", func(c state.AssignCtx[*ctxSeen]) *ctxSeen {
			c.Entity.Entry = tagOf(c.Ctx)
			return c.Entity
		}).
		State("idle").OnEntryAssign("entry").
		State("busy").
		Initial("idle").
		Transition("idle").On("go").GoTo("busy").When("g").Do("a").Assign("r").
		Transition("busy").On("back").GoTo("idle").
		Quench()
}

// TestCallbacks_GetTheFiresContext proves a guard, an action and a reducer each get the
// context the Fire was given as their Ctx.
func TestCallbacks_GetTheFiresContext(t *testing.T) {
	seen := &ctxSeen{}
	inst := ctxMachine().Cast(seen, state.WithInitialState("idle"))
	if res := inst.Fire(tagged("fire-1"), "go"); res.Err != nil {
		t.Fatalf("Fire: %v", res.Err)
	}
	if *seen != (ctxSeen{Guard: "fire-1", Action: "fire-1", Assign: "fire-1", Entry: ""}) {
		t.Fatalf("callbacks saw %+v, want the guard, action and reducer to see fire-1", *seen)
	}
	if res := inst.Fire(tagged("fire-2"), "back"); res.Err != nil {
		t.Fatalf("Fire(back): %v", res.Err)
	}
	if seen.Entry != "fire-2" {
		t.Fatalf("the entry reducer saw %q, want fire-2", seen.Entry)
	}
}

// TestCallbacks_CastWithContext proves the reducers that settle a cast's initial state
// get WithContext's context, and a plain Cast gives them a non-nil background one.
func TestCallbacks_CastWithContext(t *testing.T) {
	seen := &ctxSeen{}
	ctxMachine().Cast(seen, state.WithInitialState("idle"), state.WithContext[string](tagged("cast")))
	if seen.Entry != "cast" {
		t.Fatalf("the entry reducer saw %q at Cast, want cast", seen.Entry)
	}
	plain := &ctxSeen{}
	ctxMachine().Cast(plain, state.WithInitialState("idle"))
	if plain.Entry != "" { // context.Background(): non-nil, no tag
		t.Fatalf("the entry reducer saw %q at a plain Cast, want a background context", plain.Entry)
	}
}

// TestCallbacks_PlanPathGetsABackgroundContext proves a guard evaluated outside any
// Fire, by PlanPath, gets a non-nil context.
func TestCallbacks_PlanPathGetsABackgroundContext(t *testing.T) {
	m := ctxMachine()
	seen := &ctxSeen{Guard: "unset"}
	if _, err := m.PlanPath("idle", "busy", seen); err != nil {
		t.Fatalf("PlanPath: %v", err)
	}
	if seen.Guard != "" {
		t.Fatalf("PlanPath's guard saw %q, want a background context", seen.Guard)
	}
}

// TestCallbacks_NestedFireRestoresTheOuterContext proves a Fire made from inside a
// callback on the same instance doesn't leave its context behind: the outer step's
// later callbacks still see the outer context.
func TestCallbacks_NestedFireRestoresTheOuterContext(t *testing.T) {
	type rec struct{ Assign string }
	var inst *state.Instance[string, string, *rec]
	m := state.ForgeFor[*rec]("nested").
		Guard("nest", func(c state.GuardCtx[*rec]) bool {
			inst.Fire(tagged("inner"), "noop")
			return true
		}).
		Reducer("r", func(c state.AssignCtx[*rec]) *rec {
			c.Entity.Assign = tagOf(c.Ctx)
			return c.Entity
		}).
		State("idle").
		State("busy").
		Initial("idle").
		Transition("idle").On("go").GoTo("busy").When("nest").Assign("r").
		Quench()
	seen := &rec{}
	inst = m.Cast(seen, state.WithInitialState("idle"))
	if res := inst.Fire(tagged("outer"), "go"); res.Err != nil {
		t.Fatalf("Fire: %v", res.Err)
	}
	if seen.Assign != "outer" {
		t.Fatalf("the reducer after a nested Fire saw %q, want outer", seen.Assign)
	}
}

// TestActorSystem_RegisterContext_GetsTheSpawnContext proves a behavior registered
// with RegisterContext gets the context of the call that absorbed the spawn, and can
// hand it to its child's Cast.
func TestActorSystem_RegisterContext_GetsTheSpawnContext(t *testing.T) {
	parent := ctxMachine().Cast(&ctxSeen{}, state.WithInitialState("idle"))
	child := &ctxSeen{}
	var spawnTag string
	sys := state.NewActorSystem(parent).
		RegisterContext("kid", func(ctx context.Context, _ map[string]any) (state.ActorInstance, error) {
			spawnTag = tagOf(ctx)
			inst := ctxMachine().Cast(child, state.WithInitialState("idle"), state.WithContext[string](ctx))
			return state.NewActor(inst, nil), nil
		})
	sys.Absorb(tagged("spawn"), []state.Effect{state.SpawnActor{ID: "kid-1", Src: state.Ref{Name: "kid"}}})
	if spawnTag != "spawn" || child.Entry != "spawn" {
		t.Fatalf("behavior saw %q and the child's entry reducer %q, want spawn for both", spawnTag, child.Entry)
	}
	ref, ok := sys.Ref("kid-1")
	if !ok {
		t.Fatal("the actor didn't spawn")
	}
	if !sys.Deliver(tagged("deliver"), ref, "go") {
		t.Fatal("Deliver returned false")
	}
	if child.Guard != "deliver" || child.Action != "deliver" || child.Assign != "deliver" {
		t.Fatalf("the child's callbacks saw %+v, want deliver", *child)
	}
}
