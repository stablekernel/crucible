package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// gctx is a tiny entity whose flags drive named guards in the combinator tests.
type gctx struct {
	a, b, c bool
	// calls records, in order, the name of every leaf guard the kernel evaluated,
	// so a test can assert short-circuit behavior by which guards did and did not
	// run.
	calls *[]string
}

// flagGuard returns a named guard reading one boolean off the entity and
// recording that it ran, so short-circuit tests observe evaluation order.
func flagGuard(name string, read func(gctx) bool) state.GuardFn[gctx] {
	return func(c state.GuardCtx[gctx]) bool {
		if c.Entity.calls != nil {
			*c.Entity.calls = append(*c.Entity.calls, name)
		}
		return read(c.Entity)
	}
}

// guardReg registers the a/b/c flag guards onto a builder.
func withGuards(b *state.Builder[string, string, gctx]) *state.Builder[string, string, gctx] {
	return b.
		Guard("a", flagGuard("a", func(e gctx) bool { return e.a })).
		Guard("b", flagGuard("b", func(e gctx) bool { return e.b })).
		Guard("c", flagGuard("c", func(e gctx) bool { return e.c }))
}

// fireExpr builds a one-edge machine whose "go" transition carries the given
// composite guard, fires "go" against the entity, and reports whether the
// transition was enabled (reached "to").
func fireExpr(t *testing.T, expr state.GuardNode[string], e gctx) (enabled bool, res state.FireResult[string]) {
	t.Helper()
	m := withGuards(state.Forge[string, string, gctx]("g").
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(expr).
		State("to").
		Initial("from")).
		Quench()
	inst := m.Cast(e, state.WithInitialState("from"))
	res = inst.Fire(context.Background(), "go")
	return inst.Current() == "to", res
}

// ---------------------------------------------------------------------------
// And / Or / Not truth tables
// ---------------------------------------------------------------------------

func TestAnd_TruthTable(t *testing.T) {
	cases := []struct {
		a, b, want bool
	}{
		{false, false, false},
		{true, false, false},
		{false, true, false},
		{true, true, true},
	}
	for _, tc := range cases {
		expr := state.And(state.Guard[string]("a"), state.Guard[string]("b"))
		got, _ := fireExpr(t, expr, gctx{a: tc.a, b: tc.b})
		if got != tc.want {
			t.Fatalf("And(a=%v,b=%v) enabled=%v want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestOr_TruthTable(t *testing.T) {
	cases := []struct {
		a, b, want bool
	}{
		{false, false, false},
		{true, false, true},
		{false, true, true},
		{true, true, true},
	}
	for _, tc := range cases {
		expr := state.Or(state.Guard[string]("a"), state.Guard[string]("b"))
		got, _ := fireExpr(t, expr, gctx{a: tc.a, b: tc.b})
		if got != tc.want {
			t.Fatalf("Or(a=%v,b=%v) enabled=%v want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestNot_TruthTable(t *testing.T) {
	for _, a := range []bool{false, true} {
		expr := state.Not(state.Guard[string]("a"))
		got, _ := fireExpr(t, expr, gctx{a: a})
		if got != !a {
			t.Fatalf("Not(a=%v) enabled=%v want %v", a, got, !a)
		}
	}
}

// ---------------------------------------------------------------------------
// Short-circuit
// ---------------------------------------------------------------------------

func TestAnd_ShortCircuitsAtFirstFalse(t *testing.T) {
	var calls []string
	expr := state.And(state.Guard[string]("a"), state.Guard[string]("b"), state.Guard[string]("c"))
	got, _ := fireExpr(t, expr, gctx{a: false, b: true, c: true, calls: &calls})
	if got {
		t.Fatalf("And enabled but a is false")
	}
	if strings.Join(calls, ",") != "a" {
		t.Fatalf("And short-circuit: evaluated %v, want only [a]", calls)
	}
}

func TestOr_ShortCircuitsAtFirstTrue(t *testing.T) {
	var calls []string
	expr := state.Or(state.Guard[string]("a"), state.Guard[string]("b"), state.Guard[string]("c"))
	got, _ := fireExpr(t, expr, gctx{a: true, b: false, c: false, calls: &calls})
	if !got {
		t.Fatalf("Or not enabled but a is true")
	}
	if strings.Join(calls, ",") != "a" {
		t.Fatalf("Or short-circuit: evaluated %v, want only [a]", calls)
	}
}

func TestOr_EvaluatesUntilFirstTrue(t *testing.T) {
	var calls []string
	expr := state.Or(state.Guard[string]("a"), state.Guard[string]("b"), state.Guard[string]("c"))
	got, _ := fireExpr(t, expr, gctx{a: false, b: true, c: false, calls: &calls})
	if !got {
		t.Fatalf("Or not enabled but b is true")
	}
	if strings.Join(calls, ",") != "a,b" {
		t.Fatalf("Or evaluation order: %v, want [a b]", calls)
	}
}

// ---------------------------------------------------------------------------
// Nested combinators
// ---------------------------------------------------------------------------

func TestNested_AndOrNot(t *testing.T) {
	// And(Or(a,b), Not(c))
	expr := state.And(
		state.Or(state.Guard[string]("a"), state.Guard[string]("b")),
		state.Not(state.Guard[string]("c")),
	)
	eval := func(a, b, c bool) bool { return (a || b) && !c }
	for _, a := range []bool{false, true} {
		for _, b := range []bool{false, true} {
			for _, c := range []bool{false, true} {
				got, _ := fireExpr(t, expr, gctx{a: a, b: b, c: c})
				if got != eval(a, b, c) {
					t.Fatalf("And(Or(a,b),Not(c)) a=%v b=%v c=%v: enabled=%v want %v", a, b, c, got, eval(a, b, c))
				}
			}
		}
	}
}

func TestNested_DeepNesting(t *testing.T) {
	// Not(And(a, Or(b, Not(c))))  ==  !(a && (b || !c))
	expr := state.Not(state.And(
		state.Guard[string]("a"),
		state.Or(state.Guard[string]("b"), state.Not(state.Guard[string]("c"))),
	))
	eval := func(a, b, c bool) bool {
		inner := a && (b || !c)
		return !inner
	}
	for _, a := range []bool{false, true} {
		for _, b := range []bool{false, true} {
			for _, c := range []bool{false, true} {
				got, _ := fireExpr(t, expr, gctx{a: a, b: b, c: c})
				if got != eval(a, b, c) {
					t.Fatalf("deep nest a=%v b=%v c=%v enabled=%v want %v", a, b, c, got, eval(a, b, c))
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Composite-guard failure semantics
// ---------------------------------------------------------------------------

func TestCompositeGuard_FailureReportsLeaf(t *testing.T) {
	expr := state.And(state.Guard[string]("a"), state.Guard[string]("b"))
	enabled, res := fireExpr(t, expr, gctx{a: true, b: false})
	if enabled {
		t.Fatalf("expected guard failure")
	}
	var gf *state.GuardFailedError
	if !errors.As(res.Err, &gf) {
		t.Fatalf("want *GuardFailedError, got %T: %v", res.Err, res.Err)
	}
	if !strings.Contains(gf.GuardName, "b") {
		t.Fatalf("failure should name leaf b, got %q", gf.GuardName)
	}
	if res.Trace.Outcome != state.OutcomeGuardFailed {
		t.Fatalf("outcome = %v, want OutcomeGuardFailed", res.Trace.Outcome)
	}
}

func TestCompositeGuard_PanicSurfacesTyped(t *testing.T) {
	m := state.Forge[string, string, gctx]("p").
		Guard("boom", func(state.GuardCtx[gctx]) bool { panic("kaboom") }).
		State("from").
		Transition("from").On("go").GoTo("to").
		WhenExpr(state.Not(state.Guard[string]("boom"))).
		State("to").
		Initial("from").
		Quench()
	inst := m.Cast(gctx{}, state.WithInitialState("from"))
	res := inst.Fire(context.Background(), "go")
	var gp *state.GuardPanicError
	if !errors.As(res.Err, &gp) {
		t.Fatalf("want *GuardPanicError, got %T: %v", res.Err, res.Err)
	}
}

// plainAndExprBothMustPass confirms a When guard and a WhenExpr on the same
// transition are AND-composed: the transition is enabled only when both pass.
func TestWhenAndWhenExpr_BothMustPass(t *testing.T) {
	build := func(e gctx) bool {
		m := withGuards(state.Forge[string, string, gctx]("both").
			State("from").
			Transition("from").On("go").GoTo("to").
			When("a").
			WhenExpr(state.Guard[string]("b")).
			State("to").
			Initial("from")).
			Quench()
		inst := m.Cast(e, state.WithInitialState("from"))
		inst.Fire(context.Background(), "go")
		return inst.Current() == "to"
	}
	if build(gctx{a: true, b: true}) != true {
		t.Fatalf("both true should enable")
	}
	if build(gctx{a: true, b: false}) != false {
		t.Fatalf("expr false should disable")
	}
	if build(gctx{a: false, b: true}) != false {
		t.Fatalf("when false should disable")
	}
}

// ---------------------------------------------------------------------------
// stateIn — atomic, compound, parallel
// ---------------------------------------------------------------------------

func TestStateIn_Atomic(t *testing.T) {
	m := state.Forge[string, string, gctx]("flat").
		State("a").Transition("a").On("go").GoTo("b").WhenExpr(state.StateIn("a")).
		State("b").Transition("b").On("go").GoTo("c").WhenExpr(state.StateIn("a")).
		State("c").
		Initial("a").
		Quench()
	inst := m.Cast(gctx{}, state.WithInitialState("a"))
	// In "a": stateIn("a") true, so a->b fires.
	inst.Fire(context.Background(), "go")
	if inst.Current() != "b" {
		t.Fatalf("stateIn(a) in a should enable a->b, got %q", inst.Current())
	}
	// In "b": stateIn("a") false, so b->c does not fire.
	inst.Fire(context.Background(), "go")
	if inst.Current() != "b" {
		t.Fatalf("stateIn(a) in b should block b->c, got %q", inst.Current())
	}
}

// compound machine: super "work" with children "draft"/"review"; stateIn("work")
// must be true while any child of work is active.
func TestStateIn_Compound(t *testing.T) {
	m := state.Forge[string, string, gctx]("hsm").
		SuperState("work").Initial("draft").
		SubState("draft").On("next").GoTo("review").
		SubState("review").
		EndSuperState().
		State("done").
		// A top-level edge enabled only while inside the "work" compound.
		Transition("draft").On("ping").GoTo("done").WhenExpr(state.StateIn("work")).
		Initial("work").
		Quench()
	inst := m.Cast(gctx{}, state.WithInitialState("work"))
	if inst.Current() != "draft" {
		t.Fatalf("initial leaf = %q, want draft", inst.Current())
	}
	// stateIn("work") is true because the active spine includes "work".
	inst.Fire(context.Background(), "ping")
	if inst.Current() != "done" {
		t.Fatalf("stateIn(work) should be true inside work; got %q", inst.Current())
	}
}

// parallel machine: a stateIn over a leaf in one region is true while that region
// holds the leaf, even though another region is concurrently active.
func TestStateIn_Parallel(t *testing.T) {
	m := state.Forge[string, string, gctx]("par").
		State("off").Transition("off").On("start").GoTo("root").
		SuperState("root").
		Region("r1").
		Initial("r1a").
		SubState("r1a").On("toB").GoTo("r1b").
		SubState("r1b").
		EndRegion().
		Region("r2").
		Initial("r2a").
		// Enabled only while region r1 is in r1b; r2 is concurrently active. The
		// edge lives in region r2 and targets a leaf in r2, so the cross-region
		// stateIn(r1b) reads the other region's live leaf.
		SubState("r2a").On("check").GoTo("r2done").WhenExpr(state.StateIn("r1b")).
		SubState("r2done").
		EndRegion().
		EndSuperState().
		Initial("off").
		Quench()
	inst := m.Cast(gctx{}, state.WithInitialState("off"))
	inst.Fire(context.Background(), "start")
	if !contains(inst.Configuration(), "r1a") || !contains(inst.Configuration(), "r2a") {
		t.Fatalf("after start, config = %v, want r1a and r2a active", inst.Configuration())
	}
	// Initially r1 is in r1a, so stateIn(r1b) is false: r2 stays in r2a.
	inst.Fire(context.Background(), "check")
	if contains(inst.Configuration(), "r2done") {
		t.Fatalf("stateIn(r1b) should be false before toB; config=%v", inst.Configuration())
	}
	// Move region r1 to r1b; r2 stays in r2a.
	inst.Fire(context.Background(), "toB")
	cfg := inst.Configuration()
	if !contains(cfg, "r1b") || !contains(cfg, "r2a") {
		t.Fatalf("parallel config = %v, want both r1b and r2a active", cfg)
	}
	// Now stateIn(r1b) is true even though r2 is concurrently active.
	inst.Fire(context.Background(), "check")
	if !contains(inst.Configuration(), "r2done") {
		t.Fatalf("stateIn(r1b) should be true after toB; config=%v", inst.Configuration())
	}
}

// stateIn composes with the boolean combinators.
func TestStateIn_ComposesWithCombinators(t *testing.T) {
	m := withGuards(state.Forge[string, string, gctx]("mix").
		State("a").
		Transition("a").On("go").GoTo("b").
		WhenExpr(state.And(state.StateIn("a"), state.Not(state.Guard[string]("c")))).
		State("b").
		Initial("a")).
		Quench()
	// stateIn(a) true, c false -> Not(c) true -> enabled.
	in1 := m.Cast(gctx{c: false}, state.WithInitialState("a"))
	in1.Fire(context.Background(), "go")
	if in1.Current() != "b" {
		t.Fatalf("And(stateIn(a),Not(c)) should enable with c=false, got %q", in1.Current())
	}
	// c true -> Not(c) false -> blocked.
	in2 := m.Cast(gctx{c: true}, state.WithInitialState("a"))
	in2.Fire(context.Background(), "go")
	if in2.Current() != "a" {
		t.Fatalf("And(stateIn(a),Not(c)) should block with c=true, got %q", in2.Current())
	}
}

// ---------------------------------------------------------------------------
// IR round-trip of a nested guard expression
// ---------------------------------------------------------------------------

func TestGuardExpr_IRRoundTrip(t *testing.T) {
	expr := state.And(
		state.Or(state.Guard[string]("a", map[string]any{"k": "v"}), state.StateIn("a")),
		state.Not(state.Guard[string]("c")),
	)
	m := withGuards(state.Forge[string, string, gctx]("rt").
		State("a").
		Transition("a").On("go").GoTo("b").WhenExpr(expr).
		State("b").
		Initial("a")).
		Quench()

	b, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	if !strings.Contains(string(b), "\"guardExpr\"") {
		t.Fatalf("serialized IR missing guardExpr: %s", b)
	}

	ir, err := state.LoadFromJSON[string, string, gctx](b)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}

	// Structural lossless round-trip: re-marshal and compare bytes.
	again, err := json.Marshal(struct {
		States []state.State[string, string, gctx] `json:"states"`
	}{States: ir.States})
	_ = again
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}

	// Rehydrate behavior via Provide against a registry and confirm the composite
	// guard binds and evaluates identically.
	reg := state.NewRegistry[gctx]().
		Guard("a", flagGuard("a", func(e gctx) bool { return e.a })).
		Guard("c", flagGuard("c", func(e gctx) bool { return e.c }))
	m2 := ir.Provide(reg).Quench()

	// expr = And(Or(a, stateIn(a)), Not(c)). In state "a", stateIn(a) is true, so
	// Or(...) short-circuits true; with c=false, Not(c) true -> enabled.
	inst := m2.Cast(gctx{a: false, c: false}, state.WithInitialState("a"))
	inst.Fire(context.Background(), "go")
	if inst.Current() != "b" {
		t.Fatalf("rehydrated composite guard should enable, got %q", inst.Current())
	}
	// With c=true the Not(c) leaf fails -> blocked.
	inst2 := m2.Cast(gctx{a: false, c: true}, state.WithInitialState("a"))
	inst2.Fire(context.Background(), "go")
	if inst2.Current() != "a" {
		t.Fatalf("rehydrated composite guard should block with c=true, got %q", inst2.Current())
	}
}

func TestGuardExpr_UnboundLeafPanicsAtQuench(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected Quench panic for unbound composite leaf")
		}
		var ub *state.UnboundRefError
		if !errors.As(r.(error), &ub) {
			t.Fatalf("want *UnboundRefError, got %T: %v", r, r)
		}
	}()
	state.Forge[string, string, gctx]("u").
		State("a").
		Transition("a").On("go").GoTo("b").WhenExpr(state.Guard[string]("missing")).
		State("b").
		Initial("a").
		Quench()
}

func TestGuardExpr_MalformedAndPanicsAtQuench(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected Quench panic for malformed (empty) And")
		}
	}()
	withGuards(state.Forge[string, string, gctx]("m").
		State("a").
		Transition("a").On("go").GoTo("b").WhenExpr(state.And[string]()).
		State("b").
		Initial("a")).
		Quench()
}

// TestEventlessGuard_PanicDoesNotEnableTransition asserts the eventless safety
// property guardsPass guarantees: a guard that panics on an Always (eventless)
// transition is treated as not-passing, so the run-to-completion loop never
// silently auto-fires the transition. The instance settles in the event-target
// state and never advances through the guarded eventless edge. A swallowed panic
// that enabled the transition would corrupt the macrostep, so the property is a
// correctness invariant, not cosmetics.
//
// This is distinct from an event-triggered guard panic (covered by
// TestCompositeGuard_PanicSurfacesTyped), which surfaces a typed
// *GuardPanicError; the eventless selector is deliberately quieter — it must not
// turn a faulty guard into an unguarded auto-transition.
func TestEventlessGuard_PanicDoesNotEnableTransition(t *testing.T) {
	tests := []struct {
		name  string
		guard func(*state.Builder[string, string, gctx]) *state.Builder[string, string, gctx]
	}{
		{
			name: "plain guard ref panics",
			guard: func(b *state.Builder[string, string, gctx]) *state.Builder[string, string, gctx] {
				return b.When("boom")
			},
		},
		{
			name: "composite guard expr panics",
			guard: func(b *state.Builder[string, string, gctx]) *state.Builder[string, string, gctx] {
				return b.WhenExpr(state.Not(state.Guard[string]("boom")))
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			m := tt.guard(
				state.Forge[string, string, gctx]("eventless-guard").
					Guard("boom", func(state.GuardCtx[gctx]) bool { panic("kaboom") }).
					State("from").
					Transition("from").On("go").GoTo("mid").
					State("mid").
					Always().GoTo("done"),
			).
				State("done").
				Initial("from").
				Quench()

			inst := m.Cast(gctx{}, state.WithInitialState("from"))
			res := inst.Fire(context.Background(), "go")

			// The panicking eventless guard is not-passing: the auto-transition to
			// "done" never fires, so the macrostep settles in the event-target.
			if got := inst.Current(); got != "mid" {
				t.Fatalf("current = %q, want mid (eventless edge must not auto-fire)", got)
			}
			// The swallowed panic does not poison the macrostep: the triggering
			// event settled cleanly.
			if res.Err != nil {
				t.Fatalf("Fire err = %v, want nil (eventless guard panic is swallowed, not surfaced)", res.Err)
			}
			if res.Trace.Outcome != state.OutcomeSuccess {
				t.Fatalf("outcome = %v, want OutcomeSuccess", res.Trace.Outcome)
			}
		})
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
