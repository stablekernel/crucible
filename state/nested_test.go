package state_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file exercises arbitrarily nested superstates authored entirely through
// the DSL: a SuperState block opened inside another SuperState block, to a depth
// of three. It covers the entry cascade descending to the deepest leaf, the exit
// cascade unwinding innermost-first across every level, child-first event
// resolution bubbling through multiple ancestors, onDone propagation when a
// nested compound completes, DSL-authored deep history restoring a nested leaf,
// and a lossless IR round-trip at depth.

type lvl int

const (
	l0     lvl = iota // top-level flat state
	a                 // L1 superstate
	b                 // L2 superstate (child of a)
	c                 // L3 superstate (child of b)
	c1                // leaf of c (initial)
	c2                // leaf of c
	cFinal            // final leaf of c
	aSib              // a leaf sibling of a's compound spine
	aHist             // deep-history pseudo-state under a
	parked            // flat state we exit to and resume from
)

func (s lvl) String() string {
	switch s {
	case l0:
		return "L0"
	case a:
		return "A"
	case b:
		return "B"
	case c:
		return "C"
	case c1:
		return "C1"
	case c2:
		return "C2"
	case cFinal:
		return "CFinal"
	case aSib:
		return "ASib"
	case aHist:
		return "AHist"
	case parked:
		return "Parked"
	default:
		return "lvl?"
	}
}

type lvlEvent int

const (
	enter   lvlEvent = iota // L0 -> A (descends to deepest leaf)
	step                    // C1 -> C2 (innermost leaf transition)
	finishC                 // C2 -> CFinal (drives nested onDone)
	crossA                  // cross-cutting on A: any active substate -> ASib
	park                    // exit the whole A spine -> Parked
	resume                  // Parked -> AHist (deep restore)
)

func (e lvlEvent) String() string {
	switch e {
	case enter:
		return "Enter"
	case step:
		return "Step"
	case finishC:
		return "FinishC"
	case crossA:
		return "CrossA"
	case park:
		return "Park"
	case resume:
		return "Resume"
	default:
		return "lvlEvent?"
	}
}

type box struct{ State lvl }

// note is a concrete effect recording one entry/exit/done cascade step.
type note struct {
	Label string
	State string
}

func recordLvl(label string) state.ActionFn[*box] {
	return func(ctx state.ActionCtx[*box]) (state.Effect, error) {
		name, _ := ctx.Params["state"].(string)
		return note{Label: label, State: name}, nil
	}
}

func lvlNotes(effects []state.Effect) []string {
	var out []string
	for _, e := range effects {
		if n, ok := e.(note); ok {
			out = append(out, n.Label+":"+n.State)
		}
	}
	return out
}

// buildNested forges a three-level nested machine via the DSL:
//
//	L0 --Enter--> A { B { C { C1, C2, CFinal } } }
//
// A is a superstate whose initial child is the superstate B; B's initial child
// is the superstate C; C's initial child is the leaf C1. Cross-cutting CrossA on
// A and Park on the top level exercise multi-level bubbling and exit. AHist is a
// deep-history pseudo-state under A so a DSL-authored deep restore can be tested.
func buildNested(t *testing.T) *state.Machine[lvl, lvlEvent, *box] {
	t.Helper()
	return forgeNested()
}

// forgeNested is the t-free constructor so the golden IR set can pin the
// three-level machine without a *testing.T.
func forgeNested() *state.Machine[lvl, lvlEvent, *box] {
	return state.Forge[lvl, lvlEvent, *box]("nested").
		Action("entry", recordLvl("entry")).
		Action("exit", recordLvl("exit")).
		Action("done", recordLvl("done")).
		State(l0).
		Transition(l0).On(enter).GoTo(a).
		SuperState(a).
		Initial(b).
		OnEntry("entry", state.P{"state": "A"}).
		OnExit("exit", state.P{"state": "A"}).
		OnDone("done", state.P{"state": "A"}).
		History(aHist, state.HistoryDeep).
		SuperState(b).
		Initial(c).
		OnEntry("entry", state.P{"state": "B"}).
		OnExit("exit", state.P{"state": "B"}).
		OnDone("done", state.P{"state": "B"}).
		SuperState(c).
		Initial(c1).
		OnEntry("entry", state.P{"state": "C"}).
		OnExit("exit", state.P{"state": "C"}).
		OnDone("done", state.P{"state": "C"}).
		SubState(c1).
		OnEntry("entry", state.P{"state": "C1"}).
		OnExit("exit", state.P{"state": "C1"}).
		On(step).GoTo(c2).
		SubState(c2).
		OnEntry("entry", state.P{"state": "C2"}).
		OnExit("exit", state.P{"state": "C2"}).
		On(finishC).GoTo(cFinal).
		SubState(cFinal).Final().
		EndSuperState(). // close C
		EndSuperState(). // close B
		// Cross-cutting on the L1 superstate: applies to any active substate at
		// any depth, resolved via the child-first bubble walking A's whole spine.
		Transition(a).On(crossA).GoTo(aSib).
		EndSuperState(). // close A
		State(aSib).
		State(parked).
		Transition(a).On(park).GoTo(parked).
		Transition(parked).On(resume).GoTo(aHist).
		Initial(l0).
		CurrentStateFn(func(x *box) lvl { return x.State }).
		Quench()
}

func fireLvl(t *testing.T, inst *state.Instance[lvl, lvlEvent, *box], ev lvlEvent) state.FireResult[lvl] {
	t.Helper()
	res := inst.Fire(context.Background(), ev)
	if res.Err != nil {
		t.Fatalf("Fire(%v) from %v: %v", ev, inst.Current(), res.Err)
	}
	return res
}

// TestNested_EntryCascadeToDeepestLeaf asserts entering the L1 superstate
// descends through every level (A -> B -> C) to the deepest initial leaf C1,
// running entry actions outermost-first across all three levels.
func TestNested_EntryCascadeToDeepestLeaf(t *testing.T) {
	m := buildNested(t)
	inst := m.Cast(&box{State: l0}, state.WithFullTrace[lvl]())
	res := fireLvl(t, inst, enter)

	if got := inst.Current(); got != c1 {
		t.Fatalf("Current() = %v, want C1 (deepest initial leaf)", got)
	}
	notes := lvlNotes(res.Effects)
	want := []string{"entry:A", "entry:B", "entry:C", "entry:C1"}
	if strings.Join(notes, ",") != strings.Join(want, ",") {
		t.Fatalf("entry cascade = %v, want %v", notes, want)
	}
	if ent := res.Trace.EnteredStates; strings.Join(ent, ",") != "A,B,C,C1" {
		t.Fatalf("EnteredStates = %v, want [A B C C1]", ent)
	}
}

// TestNested_ExitCascadeInnermostFirst asserts an event handled by an ancestor of
// the active leaf (Park, declared at the top level out of A) exits the whole
// spine innermost-first: C1, C, B, A.
func TestNested_ExitCascadeInnermostFirst(t *testing.T) {
	m := buildNested(t)
	inst := m.Cast(&box{State: c1}, state.WithFullTrace[lvl]())
	res := fireLvl(t, inst, park)

	if got := inst.Current(); got != parked {
		t.Fatalf("Current() = %v, want Parked", got)
	}
	notes := lvlNotes(res.Effects)
	want := []string{"exit:C1", "exit:C", "exit:B", "exit:A"}
	if strings.Join(notes, ",") != strings.Join(want, ",") {
		t.Fatalf("exit cascade = %v, want %v", notes, want)
	}
	if ex := res.Trace.ExitedStates; strings.Join(ex, ",") != "C1,C,B,A" {
		t.Fatalf("ExitedStates = %v, want [C1 C B A]", ex)
	}
}

// TestNested_ChildFirstResolution asserts the innermost active leaf's own
// transition wins when it matches: Step is declared on C1, so it fires there and
// MatchedAt names C1, not an ancestor.
func TestNested_ChildFirstResolution(t *testing.T) {
	m := buildNested(t)
	inst := m.Cast(&box{State: c1})
	res := fireLvl(t, inst, step)

	if res.NewState != c2 {
		t.Fatalf("NewState = %v, want C2", res.NewState)
	}
	if res.Trace.MatchedAt != "C1" {
		t.Fatalf("MatchedAt = %q, want C1", res.Trace.MatchedAt)
	}
}

// TestNested_BubblesAcrossMultipleAncestors asserts an event unhandled by the
// leaf bubbles up through C and B to the cross-cutting transition on the L1
// superstate A — proving the child-first resolver walks the full ancestor chain.
func TestNested_BubblesAcrossMultipleAncestors(t *testing.T) {
	m := buildNested(t)
	inst := m.Cast(&box{State: c1})
	res := fireLvl(t, inst, crossA)

	if res.NewState != aSib {
		t.Fatalf("NewState = %v, want ASib", res.NewState)
	}
	if res.Trace.MatchedAt != "A" {
		t.Fatalf("MatchedAt = %q, want A (cross-cutting at depth 1, leaf at depth 3)", res.Trace.MatchedAt)
	}
}

// TestNested_OnDonePropagatesUpward asserts that when the deepest leaf reaches a
// final state, the done event cascades up: C completes (its active leaf CFinal is
// final), then B completes (its active leaf descends to a final), then A — each
// ancestor's OnDone runs, innermost-first, recorded as done microsteps.
func TestNested_OnDonePropagatesUpward(t *testing.T) {
	m := buildNested(t)
	inst := m.Cast(&box{State: c2}, state.WithFullTrace[lvl]())
	res := fireLvl(t, inst, finishC)

	if got := inst.Current(); got != cFinal {
		t.Fatalf("Current() = %v, want CFinal", got)
	}
	notes := lvlNotes(res.Effects)
	// C2 exits (CFinal is a bare final leaf with no entry action), then OnDone
	// fires for C, B, A (innermost-first) as completion cascades up the spine.
	want := []string{"exit:C2", "done:C", "done:B", "done:A"}
	if strings.Join(notes, ",") != strings.Join(want, ",") {
		t.Fatalf("done cascade = %v, want %v", notes, want)
	}
	// The trace records a done microstep per completed level.
	var dones []string
	for _, ms := range res.Trace.Microsteps {
		if strings.HasPrefix(ms, "done.") {
			dones = append(dones, ms)
		}
	}
	if strings.Join(dones, ",") != "done.CFinal,done.C,done.B" {
		t.Fatalf("done microsteps = %v, want [done.CFinal done.C done.B]", dones)
	}
}

// TestNested_DeepHistoryFromDSL asserts a DSL-authored HistoryDeep pseudo-state
// in a compound with nested compounds restores the full nested leaf
// configuration. After descending to C2 (two levels below A), parking out of A,
// and resuming via the deep-history target, the machine restores C2 exactly —
// not A's default spine leaf C1. This is the capability the v1 DSL gate blocked.
func TestNested_DeepHistoryFromDSL(t *testing.T) {
	m := buildNested(t)
	inst := m.Cast(&box{State: l0}, state.WithFullTrace[lvl]())

	fireLvl(t, inst, enter) // L0 -> A -> B -> C -> C1
	if got := inst.Current(); got != c1 {
		t.Fatalf("after Enter: current=%v want C1", got)
	}
	fireLvl(t, inst, step) // C1 -> C2 (nested leaf two levels deep)
	if got := inst.Current(); got != c2 {
		t.Fatalf("after Step: current=%v want C2", got)
	}
	fireLvl(t, inst, park) // exit whole A spine -> Parked (records deep config C2)
	if got := inst.Current(); got != parked {
		t.Fatalf("after Park: current=%v want Parked", got)
	}
	res := fireLvl(t, inst, resume) // -> AHist -> deep restore C2
	if got := inst.Current(); got != c2 {
		t.Fatalf("deep history restore: current=%v want C2 (full nested leaf)", got)
	}
	// The restore re-enters the entire nested spine outermost-first.
	if ent := res.Trace.EnteredStates; strings.Join(ent, ",") != "A,B,C,C2" {
		t.Fatalf("deep restore EnteredStates = %v, want [A B C C2]", ent)
	}
}

// TestNested_IRRoundTripAtDepth asserts a DSL-authored three-level machine
// serializes and reloads losslessly: re-serializing the reloaded IR yields
// byte-identical JSON, and the reloaded machine still descends to the deepest
// leaf at runtime.
func TestNested_IRRoundTripAtDepth(t *testing.T) {
	m := buildNested(t)

	first, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[lvl, lvlEvent, *box](first)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	reg := state.NewRegistry[*box]().
		Action("entry", recordLvl("entry")).
		Action("exit", recordLvl("exit")).
		Action("done", recordLvl("done"))
	second, err := ir.Provide(reg).Quench().ToJSON()
	if err != nil {
		t.Fatalf("reserialize: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("nested IR not byte-stable under round-trip:\n first=%s\nsecond=%s", first, second)
	}

	// The reloaded machine still descends through all three levels at runtime.
	// A JSON-reloaded machine carries no CurrentStateFn, so Cast from an explicit
	// initial state.
	rm := ir.Provide(reg).Quench()
	inst := rm.Cast(&box{State: l0}, state.WithInitialState(l0))
	fireLvl(t, inst, enter)
	if got := inst.Current(); got != c1 {
		t.Fatalf("reloaded nested descent: current=%v want C1", got)
	}
}

// Region-with-nested-compound: a parallel region whose initial child is itself a
// compound (superstate). Entering the parallel state must descend each region to
// its leaf, including transitively through a region's nested compound.

type reg2 int

const (
	rcOff   reg2 = iota
	rcPar        // parallel state
	rcInner      // compound inside region "Work"
	rcLeaf       // initial leaf of rcInner
	tFlat        // flat leaf of region "Tel"
)

func (s reg2) String() string {
	switch s {
	case rcOff:
		return "POff"
	case rcPar:
		return "PPar"
	case rcInner:
		return "RCInner"
	case rcLeaf:
		return "RCLeaf"
	case tFlat:
		return "TFlat"
	default:
		return "reg2?"
	}
}

type reg2Event int

const (
	goPar reg2Event = iota
)

func (e reg2Event) String() string {
	if e == goPar {
		return "GoPar"
	}
	return "reg2Event?"
}

type cell struct{ State reg2 }

// TestNested_CompoundInsideParallelRegion asserts a parallel region may contain a
// nested compound: region "Work"'s initial child is the compound RCInner, whose
// own initial child is the leaf RCLeaf. Entering the parallel state descends the
// "Work" region through RCInner to RCLeaf while the "Tel" region enters its flat
// leaf — so the active configuration holds both region leaves.
func TestNested_CompoundInsideParallelRegion(t *testing.T) {
	m := state.Forge[reg2, reg2Event, *cell]("region-compound").
		State(rcOff).
		Transition(rcOff).On(goPar).GoTo(rcPar).
		SuperState(rcPar).
		Region("Work").
		Initial(rcInner).
		SuperState(rcInner).
		Initial(rcLeaf).
		SubState(rcLeaf).
		EndSuperState(). // close RCInner (nested compound inside the region)
		EndRegion().
		Region("Tel").
		Initial(tFlat).
		SubState(tFlat).
		EndRegion().
		EndSuperState(). // close PPar
		Initial(rcOff).
		CurrentStateFn(func(c *cell) reg2 { return c.State }).
		Quench()

	inst := m.Cast(&cell{State: rcOff})
	res := inst.Fire(context.Background(), goPar)
	if res.Err != nil {
		t.Fatalf("Fire(GoPar): %v", res.Err)
	}
	cfg := inst.Configuration()
	got := make(map[reg2]bool, len(cfg))
	for _, leaf := range cfg {
		got[leaf] = true
	}
	if !got[rcLeaf] || !got[tFlat] {
		t.Fatalf("Configuration() = %v, want both RCLeaf (via region's nested compound) and TFlat", cfg)
	}
}
