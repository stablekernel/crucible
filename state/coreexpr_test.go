package state_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// corder is the sample context the Core-expression tests evaluate against. Its
// JSON tags match the dotted paths the field-refs resolve, so the reflective path
// reader and the ContextSchema agree on field naming.
type corder struct {
	Status   string         `json:"status"`
	Total    float64        `json:"total"`
	Quantity int            `json:"quantity"`
	Rush     bool           `json:"rush"`
	Window   time.Duration  `json:"window"`
	Customer customer       `json:"customer"`
	Tags     map[string]int `json:"tags"`
}

type customer struct {
	Tier string `json:"tier"`
}

// sampleCorder is a representative context value used across the eval tests.
func sampleCorder() corder {
	return corder{
		Status:   "paid",
		Total:    42.50,
		Quantity: 3,
		Rush:     true,
		Window:   90 * time.Minute,
		Customer: customer{Tier: "gold"},
		Tags:     map[string]int{"vip": 1},
	}
}

// evalCoreGuard builds a one-edge machine whose transition carries the Core guard
// expression, fires it against the entity, and reports whether the transition was
// enabled. No ContextSchema is attached, so evaluation is purely dynamic.
func evalCoreGuard(t *testing.T, expr state.GuardNode[string], e corder) bool {
	t.Helper()
	m := state.ForgeFor[corder]("co").
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(expr).
		State("to").
		Initial("from").
		Quench()
	inst := m.Cast(e, state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	return inst.Current() == "to"
}

// ---------------------------------------------------------------------------
// Compare op truth tables
// ---------------------------------------------------------------------------

func TestCoreExpr_Compare_Numeric(t *testing.T) {
	e := sampleCorder()
	cases := []struct {
		name string
		expr state.GuardNode[string]
		want bool
	}{
		{"total gt", state.Field[string]("total").Gt(state.Float[string](40)), true},
		{"total gt false", state.Field[string]("total").Gt(state.Float[string](50)), false},
		{"total ge equal", state.Field[string]("total").Ge(state.Float[string](42.50)), true},
		{"total lt", state.Field[string]("total").Lt(state.Float[string](50)), true},
		{"total le equal", state.Field[string]("total").Le(state.Float[string](42.50)), true},
		{"qty eq int", state.Field[string]("quantity").Eq(state.Int[string](3)), true},
		{"qty ne", state.Field[string]("quantity").Ne(state.Int[string](4)), true},
		// int field compared to float literal coerces numerically.
		{"qty gt float", state.Field[string]("quantity").Gt(state.Float[string](2.5)), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := evalCoreGuard(t, tc.expr, e); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCoreExpr_Compare_StringBoolDuration(t *testing.T) {
	e := sampleCorder()
	cases := []struct {
		name string
		expr state.GuardNode[string]
		want bool
	}{
		{"status eq", state.Field[string]("status").Eq(state.Str[string]("paid")), true},
		{"status ne", state.Field[string]("status").Ne(state.Str[string]("draft")), true},
		{"status lt lexical", state.Field[string]("status").Lt(state.Str[string]("z")), true},
		{"rush eq true", state.Field[string]("rush").Eq(state.Bool[string](true)), true},
		{"rush ne false", state.Field[string]("rush").Ne(state.Bool[string](false)), true},
		{"window ge", state.Field[string]("window").Ge(state.Dur[string](time.Hour)), true},
		{"window lt", state.Field[string]("window").Lt(state.Dur[string](time.Hour)), false},
		{"nested field", state.Field[string]("customer.tier").Eq(state.Str[string]("gold")), true},
		{"map index", state.Field[string]("tags.vip").Eq(state.Int[string](1)), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := evalCoreGuard(t, tc.expr, e); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCoreExpr_CrossCategoryEqualityIsFalse(t *testing.T) {
	// status (string) compared for equality against an int literal is simply
	// false, not an error — a differently typed comparison does not enable.
	e := sampleCorder()
	if evalCoreGuard(t, state.Field[string]("status").Eq(state.Int[string](1)), e) {
		t.Fatal("cross-category equality unexpectedly passed")
	}
}

// ---------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------

func TestCoreExpr_Membership(t *testing.T) {
	e := sampleCorder()
	in := state.Field[string]("status").In(
		state.Str[string]("paid"), state.Str[string]("settled"))
	if !evalCoreGuard(t, in, e) {
		t.Fatal("expected status in {paid,settled} to pass")
	}
	notIn := state.Field[string]("status").In(
		state.Str[string]("draft"), state.Str[string]("void"))
	if evalCoreGuard(t, notIn, e) {
		t.Fatal("expected status in {draft,void} to fail")
	}
	// Enum (Param) literals compare as strings against the string field.
	enumIn := state.Field[string]("status").In(state.Param[string]("paid"))
	if !evalCoreGuard(t, enumIn, e) {
		t.Fatal("expected enum membership to pass")
	}
}

// ---------------------------------------------------------------------------
// Boolean composition with stateIn and Core leaves mixed
// ---------------------------------------------------------------------------

func TestCoreExpr_BooleanCompositionWithStateIn(t *testing.T) {
	e := sampleCorder()
	// and( total >= 40, or( status==paid, rush==true ) ) — all true.
	expr := state.And(
		state.Field[string]("total").Ge(state.Float[string](40)),
		state.Or(
			state.Field[string]("status").Eq(state.Str[string]("paid")),
			state.Field[string]("rush").Eq(state.Bool[string](true)),
		),
	)
	if !evalCoreGuard(t, expr, e) {
		t.Fatal("expected composed Core expression to pass")
	}

	// not( quantity > 10 ) — quantity is 3, so the negation passes.
	if !evalCoreGuard(t, state.Not(state.Field[string]("quantity").Gt(state.Int[string](10))), e) {
		t.Fatal("expected not(quantity>10) to pass")
	}
}

func TestCoreExpr_StateInMixedWithCore(t *testing.T) {
	// A machine that mixes the stateIn built-in with a Core compare in one
	// expression, driving a real transition.
	m := state.ForgeFor[corder]("mix").
		State("from").
		Transition("from").On("go").GoTo("to").
		WhenExpr(state.And(
			state.StateIn("from"),
			state.Field[string]("total").Gt(state.Float[string](40)),
		)).
		State("to").
		Initial("from").
		Quench()
	inst := m.Cast(sampleCorder(), state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	if inst.Current() != "to" {
		t.Fatalf("expected stateIn+core guard to enable transition, at %q", inst.Current())
	}
}

// ---------------------------------------------------------------------------
// Field-ref against a comparison operand (field vs field)
// ---------------------------------------------------------------------------

func TestCoreExpr_FieldVsField(t *testing.T) {
	type pair struct {
		A int `json:"a"`
		B int `json:"b"`
	}
	m := state.ForgeFor[pair]("ff").
		State("from").
		Transition("from").On("go").GoTo("to").
		WhenExpr(state.Field[string]("a").Lt(state.FieldOp(state.Field[string]("b")))).
		State("to").
		Initial("from").
		Quench()
	inst := m.Cast(pair{A: 1, B: 2}, state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	if inst.Current() != "to" {
		t.Fatalf("expected a<b field-vs-field guard to enable, at %q", inst.Current())
	}
}

// ---------------------------------------------------------------------------
// Build-time type-check against an attached ContextSchema
// ---------------------------------------------------------------------------

func coreExprBuilder(expr state.GuardNode[string]) *state.Builder[string, string, corder] {
	return state.ForgeFor[corder]("tc").
		WithContextSchema(state.SchemaOf[corder]()).
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(expr).
		State("to").
		Initial("from")
}

// hasErrorContaining reports whether any error diagnostic's message contains sub.
func hasErrorContaining(diags []state.Diagnostic, sub string) bool {
	for _, d := range diags {
		if d.Severity == "error" && strings.Contains(d.Message, sub) {
			return true
		}
	}
	return false
}

func TestCoreExpr_TypeCheck_UnknownField(t *testing.T) {
	b := coreExprBuilder(state.Field[string]("nope").Eq(state.Str[string]("x")))
	diags := b.Temper()
	if !hasErrorContaining(diags, `unknown field "nope"`) {
		t.Fatalf("expected unknown-field error, got %+v", diags)
	}
}

func TestCoreExpr_TypeCheck_TypeMismatch(t *testing.T) {
	// status is a string field; comparing it against an int literal must fail the
	// build-time type-check.
	b := coreExprBuilder(state.Field[string]("status").Eq(state.Int[string](1)))
	diags := b.Temper()
	if !hasErrorContaining(diags, "cannot compare") {
		t.Fatalf("expected type-mismatch error, got %+v", diags)
	}
}

func TestCoreExpr_TypeCheck_MembershipMismatch(t *testing.T) {
	// quantity is int; a string membership value is not comparable.
	b := coreExprBuilder(state.Field[string]("quantity").In(state.Str[string]("a")))
	diags := b.Temper()
	if !hasErrorContaining(diags, "not comparable") {
		t.Fatalf("expected membership-mismatch error, got %+v", diags)
	}
}

func TestCoreExpr_TypeCheck_NestedFieldResolves(t *testing.T) {
	// A valid nested-field comparison against the schema must NOT produce errors.
	b := coreExprBuilder(state.Field[string]("customer.tier").Eq(state.Str[string]("gold")))
	for _, d := range b.Temper() {
		if d.Severity == "error" {
			t.Fatalf("unexpected error for valid nested field: %q", d.Message)
		}
	}
}

func TestCoreExpr_TypeCheck_NumericCoercionOK(t *testing.T) {
	// quantity is int; comparing against a float literal is allowed (numeric
	// coercion), so the build must not error.
	b := coreExprBuilder(state.Field[string]("quantity").Gt(state.Float[string](2.5)))
	for _, d := range b.Temper() {
		if d.Severity == "error" {
			t.Fatalf("unexpected error for numeric coercion: %q", d.Message)
		}
	}
}

func TestCoreExpr_NoSchema_SkipsTypeCheckButStillEvals(t *testing.T) {
	// With no schema attached a type-mismatch is not caught at build time, but the
	// expression still evaluates dynamically (here cross-category equality => false).
	if evalCoreGuard(t, state.Field[string]("status").Eq(state.Int[string](1)), sampleCorder()) {
		t.Fatal("cross-category equality should be false at eval")
	}
}

// ---------------------------------------------------------------------------
// IR round-trip
// ---------------------------------------------------------------------------

func TestCoreExpr_IRRoundTrip(t *testing.T) {
	expr := state.And(
		state.Field[string]("total").Ge(state.Float[string](40)),
		state.Or(
			state.Field[string]("status").In(state.Str[string]("paid"), state.Str[string]("settled")),
			state.Field[string]("window").Lt(state.Dur[string](time.Hour)),
		),
	)
	m := state.ForgeFor[corder]("rt").
		WithContextSchema(state.SchemaOf[corder]()).
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(expr).
		State("to").
		Initial("from").
		Quench()

	jsonBytes, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, corder](jsonBytes)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	// Re-serialize the rehydrated IR and assert the bytes match: the Core nodes
	// (kind, op, path, literal, set) round-trip losslessly.
	m2 := ir.Provide(state.NewRegistry[corder]()).Quench()
	jsonBytes2, err := m2.ToJSON()
	if err != nil {
		t.Fatalf("second ToJSON err = %v", err)
	}
	if string(jsonBytes) != string(jsonBytes2) {
		t.Fatalf("round-trip mismatch:\n first: %s\nsecond: %s", jsonBytes, jsonBytes2)
	}

	// The rehydrated machine evaluates the Core guard identically.
	inst := m2.Cast(sampleCorder(), state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	if inst.Current() != "to" {
		t.Fatalf("rehydrated Core guard did not enable transition, at %q", inst.Current())
	}
}

func TestCoreExpr_UnknownKindPreserved(t *testing.T) {
	// A guardExpr node carrying a kind this build does not recognize, plus an
	// unknown sidecar key, must survive a load -> save cycle verbatim (closed-enum
	// extension policy extended to the nested guard node).
	raw := []byte(`{
	  "name":"future",
	  "schemaVersion":"1.0.0",
	  "initial":"from",
	  "states":[
	    {"name":"from","transitions":[
	      {"from":"from","to":"to","on":"go",
	       "guardExpr":{"op":"someFutureOp","kind":"plasma","futureField":42}}
	    ]},
	    {"name":"to"}
	  ]
	}`)
	ir, err := state.LoadFromJSON[string, string, corder](raw)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	out, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("marshal IR err = %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal err = %v", err)
	}
	states := got["states"].([]any)
	from := states[0].(map[string]any)
	tr := from["transitions"].([]any)[0].(map[string]any)
	ge := tr["guardExpr"].(map[string]any)
	if ge["op"] != "someFutureOp" || ge["kind"] != "plasma" {
		t.Fatalf("unknown op/kind not preserved: %+v", ge)
	}
	if ge["futureField"] != float64(42) {
		t.Fatalf("unknown sidecar key not preserved: %+v", ge)
	}
}

// ---------------------------------------------------------------------------
// Eval-time error surfacing
// ---------------------------------------------------------------------------

func TestCoreExpr_UnresolvableFieldErrorsAtFire(t *testing.T) {
	// With no schema, an unknown field passes the build but surfaces a typed guard
	// error at Fire rather than silently passing the transition.
	m := state.ForgeFor[corder]("err").
		State("from").
		Transition("from").On("go").GoTo("to").
		WhenExpr(state.Field[string]("ghost").Eq(state.Str[string]("x"))).
		State("to").
		Initial("from").
		Quench()
	inst := m.Cast(sampleCorder(), state.WithInitialState("from"))
	res := inst.Fire(context.Background(), "go")
	if inst.Current() == "to" {
		t.Fatal("unresolvable field unexpectedly enabled the transition")
	}
	if res.Err == nil {
		t.Fatal("expected a guard error surfaced for the unresolvable field")
	}
}
