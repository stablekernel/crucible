package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// acct is a value-semantics context: assigns return a new acct, guards and
// actions receive a copy and cannot mutate the instance's context.
type acct struct {
	Balance int
	Notes   []string
}

// TestAssign_TransitionUpdatesContext asserts a transition Assign folds the
// returned context onto the instance.
func TestAssign_TransitionUpdatesContext(t *testing.T) {
	m := state.ForgeFor[acct]("acct").
		Reducer("credit", func(in state.AssignCtx[acct]) acct {
			c := in.Entity
			c.Balance += 100
			return c
		}).
		State("idle").
		State("paid").
		Transition("idle").On("pay").GoTo("paid").Assign("credit").
		Initial("idle").
		Quench()

	inst := m.Cast(acct{Balance: 1}, state.WithInitialState[string]("idle"))
	res := inst.Fire(context.Background(), "pay")
	if res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}
	if got := inst.Entity().Balance; got != 101 {
		t.Fatalf("balance = %d, want 101", got)
	}
}

// TestG1_GuardCannotMutateContext asserts a guard that writes its received
// context copy does not change the instance under value semantics.
func TestG1_GuardCannotMutateContext(t *testing.T) {
	m := state.ForgeFor[acct]("g1guard").
		Guard("mutating", func(c state.GuardCtx[acct]) bool {
			c.Entity.Balance = 9999 // write the copy
			c.Entity.Notes = append(c.Entity.Notes, "x")
			return true
		}).
		State("a").
		State("b").
		Transition("a").On("go").GoTo("b").When("mutating").
		Initial("a").
		Quench()

	inst := m.Cast(acct{Balance: 5}, state.WithInitialState[string]("a"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}
	if got := inst.Entity().Balance; got != 5 {
		t.Fatalf("guard mutated context: balance = %d, want 5", got)
	}
	if len(inst.Entity().Notes) != 0 {
		t.Fatalf("guard mutated context notes: %v", inst.Entity().Notes)
	}
}

// TestG1_ActionCannotMutateContext asserts an action that writes its received
// context copy is a structural no-op on the instance under value semantics; only
// an Assign writes context.
func TestG1_ActionCannotMutateContext(t *testing.T) {
	m := state.ForgeFor[acct]("g1action").
		Action("mutating", func(c state.ActionCtx[acct]) (state.Effect, error) {
			c.Entity.Balance = 9999
			c.Entity.Notes = append(c.Entity.Notes, "x")
			return "eff", nil
		}).
		State("a").
		State("b").
		Transition("a").On("go").GoTo("b").Do("mutating").
		Initial("a").
		Quench()

	inst := m.Cast(acct{Balance: 7}, state.WithInitialState[string]("a"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}
	if got := inst.Entity().Balance; got != 7 {
		t.Fatalf("action mutated context: balance = %d, want 7", got)
	}
	if len(res.Effects) != 1 {
		t.Fatalf("action effect not emitted: %v", res.Effects)
	}
}

// TestAssign_FoldOrder asserts assigns fold in exit -> transition -> entry order,
// declaration order within a phase, each seeing the prior result.
func TestAssign_FoldOrder(t *testing.T) {
	note := func(tag string) state.AssignFn[acct] {
		return func(in state.AssignCtx[acct]) acct {
			c := in.Entity
			c.Notes = append(c.Notes, tag)
			return c
		}
	}
	m := state.ForgeFor[acct]("fold").
		Reducer("exitA", note("exit")).
		Reducer("tr", note("tr")).
		Reducer("entryB", note("entry")).
		State("a").OnExitAssign("exitA").
		State("b").OnEntryAssign("entryB").
		Transition("a").On("go").GoTo("b").Assign("tr").
		Initial("a").
		Quench()

	inst := m.Cast(acct{}, state.WithInitialState[string]("a"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}
	got := inst.Entity().Notes
	want := []string{"exit", "tr", "entry"}
	if len(got) != len(want) {
		t.Fatalf("fold order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("fold order = %v, want %v", got, want)
		}
	}
}

// TestAssign_DeclarationOrderWithinPhase asserts two assigns in the same phase
// fold in declaration order, the second seeing the first's result.
func TestAssign_DeclarationOrderWithinPhase(t *testing.T) {
	first := func(in state.AssignCtx[acct]) acct {
		c := in.Entity
		c.Balance = 10
		return c
	}
	second := func(in state.AssignCtx[acct]) acct {
		c := in.Entity
		c.Balance += 5 // sees first's 10 -> 15
		return c
	}
	m := state.ForgeFor[acct]("declorder").
		Reducer("first", first).
		Reducer("second", second).
		State("a").
		State("b").
		Transition("a").On("go").GoTo("b").Assign("first").Assign("second").
		Initial("a").
		Quench()

	inst := m.Cast(acct{}, state.WithInitialState[string]("a"))
	if res := inst.Fire(context.Background(), "go"); res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}
	if got := inst.Entity().Balance; got != 15 {
		t.Fatalf("declaration-order fold = %d, want 15", got)
	}
}

// TestAssign_ActionsReadPreAssignContext asserts an action in a phase observes
// the context as it stood at phase entry, before that phase's assigns folded.
func TestAssign_ActionsReadPreAssignContext(t *testing.T) {
	var seenByAction int
	m := state.ForgeFor[acct]("preassign").
		Action("observe", func(c state.ActionCtx[acct]) (state.Effect, error) {
			seenByAction = c.Entity.Balance
			return nil, nil
		}).
		Reducer("bump", func(in state.AssignCtx[acct]) acct {
			c := in.Entity
			c.Balance += 50
			return c
		}).
		State("a").
		State("b").
		Transition("a").On("go").GoTo("b").Do("observe").Assign("bump").
		Initial("a").
		Quench()

	inst := m.Cast(acct{Balance: 3}, state.WithInitialState[string]("a"))
	if res := inst.Fire(context.Background(), "go"); res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}
	if seenByAction != 3 {
		t.Fatalf("action saw post-assign context: %d, want 3 (pre-assign)", seenByAction)
	}
	if got := inst.Entity().Balance; got != 53 {
		t.Fatalf("assign did not fold: %d, want 53", got)
	}
}

// TestAssign_ReadsEvent asserts an Assign reads the triggering event payload from
// AssignCtx.Event.
func TestAssign_ReadsEvent(t *testing.T) {
	m := state.ForgeFor[acct]("readsevent").
		Reducer("recordEvent", func(in state.AssignCtx[acct]) acct {
			c := in.Entity
			if ev, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "ev:"+ev)
			}
			return c
		}).
		State("a").
		State("b").
		Transition("a").On("ping").GoTo("b").Assign("recordEvent").
		Initial("a").
		Quench()

	inst := m.Cast(acct{}, state.WithInitialState[string]("a"))
	if res := inst.Fire(context.Background(), "ping"); res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}
	got := inst.Entity().Notes
	if len(got) != 1 || got[0] != "ev:ping" {
		t.Fatalf("assign did not read event: %v", got)
	}
}

// TestAssign_ParamsAvailable asserts an Assign reads its ref's static params.
func TestAssign_ParamsAvailable(t *testing.T) {
	m := state.ForgeFor[acct]("params").
		Reducer("addBy", func(in state.AssignCtx[acct]) acct {
			c := in.Entity
			if amt, ok := in.Params["amount"].(int); ok {
				c.Balance += amt
			}
			return c
		}).
		State("a").
		State("b").
		Transition("a").On("go").GoTo("b").Assign("addBy", map[string]any{"amount": 42}).
		Initial("a").
		Quench()

	inst := m.Cast(acct{}, state.WithInitialState[string]("a"))
	if res := inst.Fire(context.Background(), "go"); res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}
	if got := inst.Entity().Balance; got != 42 {
		t.Fatalf("assign params not threaded: %d, want 42", got)
	}
}

// TestAssign_ParallelRegionFolds asserts a transition inside a parallel region
// folds its assign onto the instance context, so assigns are not silently dropped
// on the orthogonal-region path.
func TestAssign_ParallelRegionFolds(t *testing.T) {
	m := state.ForgeFor[acct]("parassign").
		Reducer("bump", func(in state.AssignCtx[acct]) acct {
			c := in.Entity
			c.Balance += 4
			return c
		}).
		SuperState("live").
		Region("r1").
		Initial("r1a").
		SubState("r1a").On("tick").GoTo("r1b").Assign("bump").
		SubState("r1b").
		EndRegion().
		Region("r2").
		Initial("r2a").
		SubState("r2a").
		EndRegion().
		EndSuperState().
		Initial("live").
		Quench()

	inst := m.Cast(acct{Balance: 1}, state.WithInitialState[string]("live"))
	if res := inst.Fire(context.Background(), "tick"); res.Err != nil {
		t.Fatalf("fire: %v", res.Err)
	}
	if got := inst.Entity().Balance; got != 5 {
		t.Fatalf("region transition assign did not fold: balance = %d, want 5", got)
	}
}

// TestAssign_ParallelRegionPanicStopsCommit asserts a region reducer that panics
// surfaces as OutcomeAssignFailed / *AssignPanicError and stops the commit — it
// must not silently no-op, which was the prior behavior on the parallel path.
func TestAssign_ParallelRegionPanicStopsCommit(t *testing.T) {
	m := state.ForgeFor[acct]("par-assign-panic").
		Reducer("boom", func(in state.AssignCtx[acct]) acct {
			panic("region reducer blew up")
		}).
		SuperState("live").
		Region("r1").
		Initial("r1a").
		SubState("r1a").On("tick").GoTo("r1b").Assign("boom").
		SubState("r1b").
		EndRegion().
		Region("r2").
		Initial("r2a").
		SubState("r2a").
		EndRegion().
		EndSuperState().
		Initial("live").
		Quench()

	inst := m.Cast(acct{Balance: 7}, state.WithInitialState[string]("live"))
	res := inst.Fire(context.Background(), "tick")
	if res.Err == nil {
		t.Fatal("panicking region reducer should fail the fire")
	}
	var ap *state.AssignPanicError
	if !errors.As(res.Err, &ap) {
		t.Fatalf("error = %v, want *AssignPanicError", res.Err)
	}
	if ap.AssignName != "boom" {
		t.Fatalf("assign name = %q, want boom", ap.AssignName)
	}
	if res.Trace.Outcome != state.OutcomeAssignFailed {
		t.Fatalf("outcome = %v, want OutcomeAssignFailed", res.Trace.Outcome)
	}
	if got := inst.Entity().Balance; got != 7 {
		t.Fatalf("context changed after panic: balance = %d, want 7", got)
	}
}

// TestAssign_IRRoundTrip asserts the new assign IR fields (transition Assigns and
// state OnEntryAssign/OnExitAssign) survive a ToJSON -> LoadFromJSON -> Provide
// round-trip and the rehydrated machine still folds context.
func TestAssign_IRRoundTrip(t *testing.T) {
	reg := func() *state.Registry[acct] {
		return state.NewRegistry[acct]().
			Reducer("exitA", func(in state.AssignCtx[acct]) acct { c := in.Entity; c.Notes = append(c.Notes, "exit"); return c }).
			Reducer("tr", func(in state.AssignCtx[acct]) acct { c := in.Entity; c.Balance += 7; return c }).
			Reducer("entryB", func(in state.AssignCtx[acct]) acct { c := in.Entity; c.Notes = append(c.Notes, "entry"); return c })
	}
	m := state.ForgeFor[acct]("irrt").
		Reducer("exitA", func(in state.AssignCtx[acct]) acct { return in.Entity }).
		Reducer("tr", func(in state.AssignCtx[acct]) acct { return in.Entity }).
		Reducer("entryB", func(in state.AssignCtx[acct]) acct { return in.Entity }).
		State("a").OnExitAssign("exitA").
		State("b").OnEntryAssign("entryB").
		Transition("a").On("go").GoTo("b").Assign("tr").
		Initial("a").
		Quench()

	raw, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, acct](raw)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	m2 := ir.Provide(reg()).Quench()

	inst := m2.Cast(acct{Balance: 1}, state.WithInitialState[string]("a"))
	if res := inst.Fire(context.Background(), "go"); res.Err != nil {
		t.Fatalf("fire on rehydrated machine: %v", res.Err)
	}
	got := inst.Entity()
	if got.Balance != 8 {
		t.Fatalf("rehydrated transition assign did not fold: balance = %d, want 8", got.Balance)
	}
	if len(got.Notes) != 2 || got.Notes[0] != "exit" || got.Notes[1] != "entry" {
		t.Fatalf("rehydrated exit/entry assigns did not fold in order: %v", got.Notes)
	}
}

// TestAssign_PanicStopsCommit asserts a reducer that panics surfaces as a typed
// failure that stops the commit and leaves the instance's context unchanged — an
// assign is a total reducer, so a panic is a programmer error, not a routed error.
func TestAssign_PanicStopsCommit(t *testing.T) {
	m := state.ForgeFor[acct]("assignpanic").
		Reducer("boom", func(in state.AssignCtx[acct]) acct {
			panic("reducer blew up")
		}).
		State("a").
		State("b").
		Transition("a").On("go").GoTo("b").Assign("boom").
		Initial("a").
		Quench()

	inst := m.Cast(acct{Balance: 11}, state.WithInitialState[string]("a"))
	res := inst.Fire(context.Background(), "go")
	if res.Err == nil {
		t.Fatal("panicking reducer should fail the fire")
	}
	var ap *state.AssignPanicError
	if !errors.As(res.Err, &ap) {
		t.Fatalf("error = %v, want *AssignPanicError", res.Err)
	}
	if ap.AssignName != "boom" {
		t.Fatalf("assign name = %q, want boom", ap.AssignName)
	}
	if got := inst.Entity().Balance; got != 11 {
		t.Fatalf("context changed after panic: balance = %d, want 11", got)
	}
}

// TestAssign_UnboundRefFailsQuench asserts a transition that wires an unregistered
// assign fails Quench with the typed *UnboundRefError (Kind "assign"), exactly like
// an unbound guard, action, or service.
func TestAssign_UnboundRefFailsQuench(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Quench with an unbound assign ref should panic")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panic value is not an error: %T", r)
		}
		var ub *state.UnboundRefError
		if !errors.As(err, &ub) {
			t.Fatalf("panic = %v, want *UnboundRefError", err)
		}
		if ub.Kind != "assign" || ub.Name != "ghost" {
			t.Fatalf("unbound ref = {%q, %q}, want {assign, ghost}", ub.Kind, ub.Name)
		}
	}()

	state.ForgeFor[acct]("unboundassign").
		State("a").
		State("b").
		Transition("a").On("go").GoTo("b").Assign("ghost").
		Initial("a").
		Quench()
}

// TestAssign_Palette asserts a registered assign surfaces in the registry palette
// under KindAssign.
func TestAssign_Palette(t *testing.T) {
	reg := state.NewRegistry[acct]().
		Reducer("credit", func(in state.AssignCtx[acct]) acct { return in.Entity })
	found := false
	for _, d := range reg.Palette() {
		if d.Kind == state.KindAssign && d.Name == "credit" {
			found = true
		}
	}
	if !found {
		t.Fatalf("assign not in palette: %v", reg.Palette())
	}
}
