package state_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// trec records the cascade and effect notes emitted while exercising the
// transition-semantics features. Actions append a note tagged with their kind so
// a test can assert exactly which entry/exit/effect ran and in what order.
type trec struct {
	notes []string
}

func tag(p map[string]any) string {
	if p == nil {
		return ""
	}
	if v, ok := p["t"].(string); ok {
		return v
	}
	return ""
}

// noteAction returns an action that appends a "<kind>:<tag>" note to the bound
// *trec, so a test reads the entry/exit/effect cascade as ordered data.
func noteAction(kind string) state.ActionFn[*trec] {
	return func(c state.ActionCtx[*trec]) (state.Effect, error) {
		c.Entity.notes = append(c.Entity.notes, kind+":"+tag(c.Params))
		return nil, nil
	}
}

// noteReg builds a registry whose "entry", "exit", and "do" actions record
// param-tagged notes, used by the IR-rehydration path (Provide).
func noteReg() *state.Registry[*trec] {
	return state.NewRegistry[*trec]().
		Action("entry", noteAction("entry")).
		Action("exit", noteAction("exit")).
		Action("do", noteAction("do"))
}

// provide registers the note actions onto a builder and quenches it.
func provide(b *state.Builder[string, string, *trec]) *state.Machine[string, string, *trec] {
	return b.
		Action("entry", noteAction("entry")).
		Action("exit", noteAction("exit")).
		Action("do", noteAction("do")).
		Quench()
}

// ---------------------------------------------------------------------------
// Wildcard
// ---------------------------------------------------------------------------

// TestWildcard_CatchAllMatchesUnhandledEvent asserts a wildcard transition fires
// for any event no specific On-keyed transition handles.
func TestWildcard_CatchAllMatchesUnhandledEvent(t *testing.T) {
	m := provide(state.Forge[string, string, *trec]("wild").
		State("a").
		Transition("a").On("go").GoTo("b").
		Transition("a").OnAny().GoTo("fallback").
		State("b").
		State("fallback").
		Initial("a"))

	inst := m.Cast(&trec{}, state.WithInitialState("a"))
	res := inst.Fire(context.Background(), "surprise")
	if res.Err != nil {
		t.Fatalf("Fire(surprise) err = %v", res.Err)
	}
	if res.NewState != "fallback" {
		t.Fatalf("NewState = %q, want fallback (wildcard catch-all)", res.NewState)
	}
}

// TestWildcard_SpecificOutranksWildcard asserts a specific-event transition is
// chosen over the wildcard for the event it handles (priority).
func TestWildcard_SpecificOutranksWildcard(t *testing.T) {
	m := provide(state.Forge[string, string, *trec]("wild").
		State("a").
		Transition("a").On("go").GoTo("b").
		Transition("a").OnAny().GoTo("fallback").
		State("b").
		State("fallback").
		Initial("a"))

	inst := m.Cast(&trec{}, state.WithInitialState("a"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("Fire(go) err = %v", res.Err)
	}
	if res.NewState != "b" {
		t.Fatalf("NewState = %q, want b (specific outranks wildcard)", res.NewState)
	}
}

// ---------------------------------------------------------------------------
// Forbidden
// ---------------------------------------------------------------------------

// TestForbidden_ConsumesEventWithoutBubbling asserts a forbidden event is
// ignored at the state and does NOT bubble to an ancestor that would handle it,
// whereas an undeclared event does bubble.
func TestForbidden_ConsumesEventWithoutBubbling(t *testing.T) {
	// child forbids "stop"; parent has a cross-cutting "stop" -> halted. A
	// forbidden "stop" must be consumed at child (stay in child), while an
	// unrelated "kick" must bubble to the parent.
	m := provide(state.Forge[string, string, *trec]("forbid").
		State("idle").
		Transition("idle").On("start").GoTo("running").
		SuperState("running").
		Initial("work").
		Transition("running").On("stop").GoTo("halted").
		Transition("running").On("kick").GoTo("halted").
		SubState("work").
		Forbid("stop").
		EndSuperState().
		State("halted").
		Initial("idle"))

	inst := m.Cast(&trec{}, state.WithInitialState("running"))
	if got := inst.Current(); got != "work" {
		t.Fatalf("setup: Current() = %q, want work", got)
	}

	// Forbidden: consumed at "work", no bubble to parent's stop -> halted.
	res := inst.Fire(context.Background(), "stop")
	if res.Err != nil {
		t.Fatalf("Fire(stop) err = %v, want nil (forbidden is ignored)", res.Err)
	}
	if res.NewState == "halted" {
		t.Fatalf("forbidden event bubbled to ancestor: NewState = halted")
	}
	if inst.Current() != "work" {
		t.Fatalf("Current() = %q, want work (forbidden consumed in place)", inst.Current())
	}

	// Control: an event with no handler at the child DOES bubble to the parent.
	res = inst.Fire(context.Background(), "kick")
	if res.Err != nil {
		t.Fatalf("Fire(kick) err = %v", res.Err)
	}
	if res.NewState != "halted" {
		t.Fatalf("NewState = %q, want halted (unhandled event bubbles)", res.NewState)
	}
}

// ---------------------------------------------------------------------------
// Reenter / internal-by-default
// ---------------------------------------------------------------------------

// TestReenter_SelfTransitionRunsExitEntry asserts a self-transition marked
// Reenter runs exit then entry of the source, while the internal-by-default
// self-transition runs neither (only its effects).
func TestReenter_SelfTransitionRunsExitEntry(t *testing.T) {
	build := func(reenter bool) *state.Machine[string, string, *trec] {
		b := state.Forge[string, string, *trec]("reenter").
			State("s").
			OnEntry("entry", state.P{"t": "s"}).
			OnExit("exit", state.P{"t": "s"}).
			Transition("s").On("ping").GoTo("s").Do("do", state.P{"t": "ping"})
		if reenter {
			b = b.Reenter()
		}
		return provide(b.Initial("s"))
	}

	// Internal (default): effects run, no exit/entry of the source.
	mi := build(false)
	ei := &trec{}
	resi := mi.Cast(ei, state.WithInitialState("s")).Fire(context.Background(), "ping")
	if resi.Err != nil {
		t.Fatalf("internal Fire err = %v", resi.Err)
	}
	if got := strings.Join(ei.notes, ","); got != "do:ping" {
		t.Fatalf("internal self-transition notes = %q, want %q (no exit/entry)", got, "do:ping")
	}
	if len(resi.Trace.ExitedStates) != 0 || len(resi.Trace.EnteredStates) != 0 {
		t.Fatalf("internal self-transition cascaded: exited=%v entered=%v",
			resi.Trace.ExitedStates, resi.Trace.EnteredStates)
	}

	// Reenter: exit then entry of the source run around the effects.
	mr := build(true)
	er := &trec{}
	resr := mr.Cast(er, state.WithInitialState("s"), state.WithFullTrace[string]()).Fire(context.Background(), "ping")
	if resr.Err != nil {
		t.Fatalf("reenter Fire err = %v", resr.Err)
	}
	got := strings.Join(er.notes, ",")
	want := "exit:s,do:ping,entry:s"
	if got != want {
		t.Fatalf("reenter self-transition notes = %q, want %q", got, want)
	}
	if len(resr.Trace.ExitedStates) == 0 || len(resr.Trace.EnteredStates) == 0 {
		t.Fatalf("reenter self-transition did not cascade: exited=%v entered=%v",
			resr.Trace.ExitedStates, resr.Trace.EnteredStates)
	}
}

// ---------------------------------------------------------------------------
// Raise
// ---------------------------------------------------------------------------

// TestRaise_FeedsSameMacrostepRTC asserts a raised internal event is processed
// within the same Fire macrostep — chaining a -> b -> c on one external event —
// and that the raise/transition microsteps are recorded in the Trace.
func TestRaise_FeedsSameMacrostepRTC(t *testing.T) {
	m := provide(state.Forge[string, string, *trec]("raise").
		State("a").
		Transition("a").On("go").GoTo("b").Do("do", state.P{"t": "a->b"}).Raise("next").
		State("b").
		Transition("b").On("next").GoTo("c").Do("do", state.P{"t": "b->c"}).
		State("c").
		Initial("a"))

	rec := &trec{}
	inst := m.Cast(rec, state.WithInitialState("a"), state.WithFullTrace[string]())
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("Fire(go) err = %v", res.Err)
	}
	if res.NewState != "c" {
		t.Fatalf("NewState = %q, want c (raised event drove a->b->c in one macrostep)", res.NewState)
	}
	if got := strings.Join(rec.notes, ","); got != "do:a->b,do:b->c" {
		t.Fatalf("effect order = %q, want %q", got, "do:a->b,do:b->c")
	}
	// The Trace records the raised event and the second transition as microsteps
	// of the same macrostep.
	joined := strings.Join(res.Trace.Microsteps, ",")
	if !strings.Contains(joined, "raise.next") {
		t.Fatalf("Trace.Microsteps = %v, want a raise.next entry", res.Trace.Microsteps)
	}
	if !strings.Contains(joined, "next") {
		t.Fatalf("Trace.Microsteps = %v, want the raised 'next' sub-step", res.Trace.Microsteps)
	}
}

// TestRaise_UnhandledIsIgnored asserts a raised event with no enabled transition
// at the resulting configuration is ignored (does not fail the macrostep).
func TestRaise_UnhandledIsIgnored(t *testing.T) {
	m := provide(state.Forge[string, string, *trec]("raise").
		State("a").
		Transition("a").On("go").GoTo("b").Raise("nope").
		State("b").
		Initial("a"))

	inst := m.Cast(&trec{}, state.WithInitialState("a"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("Fire(go) err = %v, want nil (unhandled raised event is ignored)", res.Err)
	}
	if res.NewState != "b" {
		t.Fatalf("NewState = %q, want b", res.NewState)
	}
}

// TestRaise_CycleOverflowsTyped asserts a self-raising cycle fails fast with the
// typed run-to-completion overflow error instead of spinning forever.
func TestRaise_CycleOverflowsTyped(t *testing.T) {
	m := provide(state.Forge[string, string, *trec]("loop").
		State("a").
		Transition("a").On("go").GoTo("a").Raise("go").
		Initial("a"))

	inst := m.Cast(&trec{}, state.WithInitialState("a"))
	res := inst.Fire(context.Background(), "go")
	var overflow *state.ErrMicrostepOverflow
	if !errors.As(res.Err, &overflow) {
		t.Fatalf("err = %v, want *ErrMicrostepOverflow", res.Err)
	}
}

// ---------------------------------------------------------------------------
// Eventless via the run-to-completion loop
// ---------------------------------------------------------------------------

// TestAlways_AutoFiresInMacrostep asserts an eventless ("always") transition is
// auto-fired by the run-to-completion loop within the same macrostep.
func TestAlways_AutoFiresInMacrostep(t *testing.T) {
	m := provide(state.Forge[string, string, *trec]("always").
		State("a").
		Transition("a").On("go").GoTo("b").
		State("b").
		Transition("b").Always().GoTo("c").
		State("c").
		Initial("a"))

	inst := m.Cast(&trec{}, state.WithInitialState("a"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("Fire(go) err = %v", res.Err)
	}
	if res.NewState != "c" {
		t.Fatalf("NewState = %q, want c (eventless b->c auto-fired)", res.NewState)
	}
}

// ---------------------------------------------------------------------------
// IR round-trip of the new structural fields
// ---------------------------------------------------------------------------

// TestTransitionSemantics_IRRoundTrip asserts wildcard, forbidden, reenter, and
// raise survive a ToJSON -> LoadFromJSON -> Provide -> Quench round-trip and
// behave identically.
func TestTransitionSemantics_IRRoundTrip(t *testing.T) {
	m := provide(state.Forge[string, string, *trec]("rt").
		State("a").
		Transition("a").On("go").GoTo("b").Raise("auto").
		Transition("a").OnAny().GoTo("fallback").
		State("b").
		Transition("b").On("auto").GoTo("c").Reenter().
		Forbid("nope").
		State("c").
		State("fallback").
		Initial("a"))

	raw, err := m.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	if !strings.Contains(string(raw), "\"wildcard\":true") ||
		!strings.Contains(string(raw), "\"forbidden\":true") ||
		!strings.Contains(string(raw), "\"reenter\":true") ||
		!strings.Contains(string(raw), "\"raise\":") {
		t.Fatalf("serialized IR missing a structural marker:\n%s", raw)
	}

	ir, err := state.LoadFromJSON[string, string, *trec](raw)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	reg := noteReg()
	m2 := ir.Provide(reg).Quench()

	// Behavior parity: the rehydrated machine drives a -> b (raises auto) ->
	// (b on auto, reenter) c in one macrostep.
	inst := m2.Cast(&trec{}, state.WithInitialState("a"))
	res := inst.Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("rehydrated Fire(go) err = %v", res.Err)
	}
	if res.NewState != "c" {
		t.Fatalf("rehydrated NewState = %q, want c", res.NewState)
	}
}
