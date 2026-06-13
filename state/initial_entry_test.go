package state_test

import (
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// markEffect is a recordable effect emitted by a test action so the test can
// match it inside the buffered initial-effect slice.
type markEffect struct {
	tag string
}

// entryRec is the entity threaded through the initial-entry tests; OnEntryAssign
// reducers mutate it so the test can observe entry-assign application at Cast.
type entryRec struct {
	entered bool
}

// findMark reports whether effects contains a markEffect with the given tag.
func findMark(effects []state.Effect, tag string) bool {
	for _, e := range effects {
		if m, ok := e.(markEffect); ok && m.tag == tag {
			return true
		}
	}
	return false
}

// findSchedule reports whether effects contains a ScheduleAfter for the given
// source state.
func findSchedule(effects []state.Effect, srcState string) bool {
	for _, e := range effects {
		if s, ok := e.(state.ScheduleAfter); ok && s.State == srcState {
			return true
		}
	}
	return false
}

// TestInitial_EntrySemanticsRunAtCast asserts that entering the initial
// configuration at Cast runs OnEntry actions, applies OnEntryAssign reducers,
// and settles an enabled eventless ("always") transition, all before any Fire.
func TestInitial_EntrySemanticsRunAtCast(t *testing.T) {
	m := state.Forge[string, string, *entryRec]("boot").
		Action("markEntered", func(ctx state.ActionCtx[*entryRec]) (state.Effect, error) {
			return markEffect{tag: "entered"}, nil
		}).
		Reducer("setEntered", func(in state.AssignCtx[*entryRec]) *entryRec {
			in.Entity.entered = true
			return in.Entity
		}).
		State("start").OnEntry("markEntered").OnEntryAssign("setEntered").
		State("next").
		Initial("start").
		Transition("start").Always().GoTo("next").
		Quench()

	inst := m.Cast(&entryRec{}, state.WithInitialState("start"))

	if !findMark(inst.InitialEffects(), "entered") {
		t.Fatalf("OnEntry effect not emitted at Cast; effects=%v", inst.InitialEffects())
	}
	if !inst.Entity().entered {
		t.Fatal("OnEntryAssign did not apply at Cast")
	}
	if inst.Current() != "next" {
		t.Fatalf("initial Always did not advance at Cast; Current=%q want next", inst.Current())
	}
}

// TestInitial_AfterArmsAtCast asserts that an initial state declaring an `after`
// transition emits a ScheduleAfter effect at Cast.
func TestInitial_AfterArmsAtCast(t *testing.T) {
	m := state.Forge[string, string, *entryRec]("timed").
		State("waiting").After(50 * time.Millisecond).On("tick").GoTo("done").
		State("done").
		Initial("waiting").
		Quench()

	inst := m.Cast(&entryRec{}, state.WithInitialState("waiting"))

	if !findSchedule(inst.InitialEffects(), "waiting") {
		t.Fatalf("initial `after` did not arm a ScheduleAfter at Cast; effects=%v", inst.InitialEffects())
	}
}

// TestInitial_FinalChildRaisesOnDoneAtCast asserts that a compound whose initial
// child is final raises the compound's OnDone at Cast (initial descent settles
// done), proving the settleDone target choice for the initial configuration.
func TestInitial_FinalChildRaisesOnDoneAtCast(t *testing.T) {
	m := state.Forge[string, string, *entryRec]("flow").
		Action("markDone", func(ctx state.ActionCtx[*entryRec]) (state.Effect, error) {
			return markEffect{tag: "done"}, nil
		}).
		SuperState("group").
		Initial("inner").
		OnDone("markDone").
		SubState("inner").Final().
		EndSuperState().
		Initial("group").
		Quench()

	inst := m.Cast(&entryRec{}, state.WithInitialState("group"))

	if !findMark(inst.InitialEffects(), "done") {
		t.Fatalf("initial->final descent did not raise compound OnDone at Cast; effects=%v", inst.InitialEffects())
	}
}
