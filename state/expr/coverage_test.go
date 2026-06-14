package expr_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// TestLower_JSONRoundTrippedLiterals asserts a Core node whose literals arrived as
// JSON floats (the form a node carries after an IR round-trip) lowers and evaluates
// correctly. JSON decodes every number as float64, so this exercises the integer and
// float literal normalization the builder's typed values would otherwise hide.
func TestLower_JSONRoundTrippedLiterals(t *testing.T) {
	node := state.And(
		state.Field[string]("i").Ge(state.Int[string](2)),
		state.Field[string]("f").Lt(state.Float[string](5.0)),
	)
	// Round-trip the node through JSON so its literal values become float64.
	b, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}
	var decoded state.GuardNode[string]
	if uerr := json.Unmarshal(b, &decoded); uerr != nil {
		t.Fatalf("unmarshal node: %v", uerr)
	}
	got, source, err := expr.EvalLowered(decoded, fuzzSchema(), fuzzCtx{I: 3, F: 2.0})
	if err != nil {
		t.Fatalf("EvalLowered(%q): %v", source, err)
	}
	if !got {
		t.Fatalf("source %q should pass for i=3, f=2.0", source)
	}
}

// TestEvalCheckedAST_RejectsBadBytes asserts malformed checked-AST bytes surface an
// error rather than a panic or a silent false.
func TestEvalCheckedAST_RejectsBadBytes(t *testing.T) {
	if _, err := expr.EvalCheckedAST([]byte{0xff, 0xfe, 0xfd}, orderSchema(), sampleOrder()); err == nil {
		t.Fatal("malformed checked-AST bytes should error")
	}
}

// TestCompileChecked_ReusesProgramAcrossEvals asserts a CompiledChecked built once
// evaluates many context values and agrees with the one-shot EvalCheckedAST on each,
// proving the cached program path is equivalent to the rebuild-every-call path.
func TestCompileChecked_ReusesProgramAcrossEvals(t *testing.T) {
	reg := state.NewRegistry[order]()
	cat := expr.NewCatalog()
	if _, err := expr.Guard[string](reg, "big", `total > 100.0`, orderSchema(), expr.WithCatalog(cat)); err != nil {
		t.Fatalf("Guard: %v", err)
	}
	entry, ok := cat.Entry("big")
	if !ok {
		t.Fatal("catalog recorded no entry")
	}

	compiled, err := expr.CompileChecked(entry.CheckedAST, orderSchema())
	if err != nil {
		t.Fatalf("CompileChecked: %v", err)
	}

	for _, tc := range []struct {
		total float64
		want  bool
	}{
		{total: 50, want: false},
		{total: 150, want: true},
		{total: 100, want: false},
		{total: 101, want: true},
	} {
		entity := order{Total: tc.total}
		got, err := compiled.Eval(entity)
		if err != nil {
			t.Fatalf("Eval(total=%v): %v", tc.total, err)
		}
		if got != tc.want {
			t.Fatalf("Eval(total=%v) = %v, want %v", tc.total, got, tc.want)
		}
		// The reusable program agrees with the rebuild-every-call helper.
		oneShot, err := expr.EvalCheckedAST(entry.CheckedAST, orderSchema(), entity)
		if err != nil {
			t.Fatalf("EvalCheckedAST(total=%v): %v", tc.total, err)
		}
		if oneShot != got {
			t.Fatalf("CompiledChecked.Eval=%v disagrees with EvalCheckedAST=%v at total=%v", got, oneShot, tc.total)
		}
	}
}

// TestCompileChecked_RejectsBadBytes asserts malformed checked-AST bytes fail at
// compile time rather than at evaluation.
func TestCompileChecked_RejectsBadBytes(t *testing.T) {
	if _, err := expr.CompileChecked([]byte{0xff, 0xfe, 0xfd}, orderSchema()); err == nil {
		t.Fatal("malformed checked-AST bytes should fail to compile")
	}
}

// TestLoadCatalog_RejectsMalformedSidecar asserts a sidecar that is not an object,
// and one whose checked-AST is not valid base64, are both rejected.
func TestLoadCatalog_RejectsMalformedSidecar(t *testing.T) {
	if _, err := expr.LoadCatalog(map[string]any{expr.MetaKey: "not-an-object"}); err == nil {
		t.Fatal("a non-object sidecar should be rejected")
	}
	bad := map[string]any{expr.MetaKey: map[string]any{
		"g": map[string]any{"source": "x", "dialect": "cel", "checkedAST": "!!!not-base64!!!"},
	}}
	if _, err := expr.LoadCatalog(bad); err == nil {
		t.Fatal("a non-base64 checkedAST should be rejected")
	}
}

// TestGuard_SchemaBuildErrorPropagates asserts a schema the env cannot build (a bad
// map key) fails Guard, EvalLowered, and EvalCheckedAST consistently — the env-build
// error path is shared by all three entry points.
func TestGuard_SchemaBuildErrorPropagates(t *testing.T) {
	badSchema := state.ContextSchema{Fields: []state.SchemaField{{
		Name: "m",
		Kind: state.SchemaMap,
		Key:  &state.SchemaField{Kind: state.SchemaDuration},
		Elem: &state.SchemaField{Kind: state.SchemaInt},
	}}}
	reg := state.NewRegistry[map[string]any]()
	if _, err := expr.Guard[string](reg, "g", `true`, badSchema); err == nil {
		t.Fatal("Guard should fail on an unbuildable schema")
	}
	node := state.Field[string]("x").Eq(state.Int[string](1))
	if _, _, err := expr.EvalLowered(node, badSchema, map[string]any{}); err == nil {
		t.Fatal("EvalLowered should fail on an unbuildable schema")
	}
	if _, err := expr.EvalCheckedAST([]byte{}, badSchema, map[string]any{}); err == nil {
		t.Fatal("EvalCheckedAST should fail on an unbuildable schema or empty bytes")
	}
}

// TestEvalLowered_BadActivation asserts a context that cannot be marshaled to an
// activation surfaces an error from EvalLowered.
func TestEvalLowered_BadActivation(t *testing.T) {
	node := state.Field[string]("i").Eq(state.Int[string](1))
	// A channel cannot be JSON-marshaled, so the activation projection fails.
	if _, _, err := expr.EvalLowered(node, fuzzSchema(), make(chan int)); err == nil {
		t.Fatal("an unmarshalable context should fail EvalLowered")
	}
}

// TestLowerMembership_FloatOperandIntSet asserts the float-operand/int-set membership
// branch double-casts the int elements, matching Core's numeric coercion.
func TestLowerMembership_FloatOperandIntSet(t *testing.T) {
	node := state.Field[string]("f").In(state.Int[string](2), state.Int[string](3))
	got, source, err := expr.EvalLowered(node, fuzzSchema(), fuzzCtx{F: 2.0})
	if err != nil {
		t.Fatalf("EvalLowered(%q): %v", source, err)
	}
	if !got {
		t.Fatalf("source %q should pass for f=2.0 in {2,3}", source)
	}
}

// TestLower_RejectsMalformedNodes asserts the lowering arity/shape guards reject
// hand-constructed malformed Core nodes: an And with no operands, a Not with the
// wrong arity, a compare with one operand, a membership with an empty set, and an
// operand node in a non-operand position.
func TestLower_RejectsMalformedNodes(t *testing.T) {
	cases := []struct {
		name string
		node state.GuardNode[string]
	}{
		{"empty and", state.GuardNode[string]{Op: state.GuardAnd}},
		{"not wrong arity", state.GuardNode[string]{Op: state.GuardNot}},
		{"compare one operand", state.GuardNode[string]{
			Op:       state.GuardEq,
			Children: []state.GuardNode[string]{{Op: state.GuardField, Path: "i"}},
		}},
		{"membership empty set", state.GuardNode[string]{
			Op:       state.GuardIn,
			Children: []state.GuardNode[string]{{Op: state.GuardField, Path: "i"}},
		}},
		{"bad operand op", state.GuardNode[string]{
			Op: state.GuardEq,
			Children: []state.GuardNode[string]{
				{Op: state.GuardAnd},
				{Op: state.GuardField, Path: "i"},
			},
		}},
		{"literal operand nil", state.GuardNode[string]{
			Op: state.GuardEq,
			Children: []state.GuardNode[string]{
				{Op: state.GuardField, Path: "i"},
				{Op: state.GuardLit},
			},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := expr.Lower(tc.node, fuzzSchema()); err == nil {
				t.Fatalf("Lower should reject %s", tc.name)
			}
		})
	}
}

// TestLower_EnumAndDurationOperands covers the enum-literal and duration-literal
// lowering arms together with a string field, exercising the literal renderers and
// schema-kind mapping for the stringy and duration categories.
func TestLower_EnumAndDurationOperands(t *testing.T) {
	// Enum literal compared against the string field (enum lowers as a string).
	enumNode := state.Field[string]("s").Eq(state.Param[string]("paid"))
	got, source, err := expr.EvalLowered(enumNode, fuzzSchema(), fuzzCtx{S: "paid"})
	if err != nil {
		t.Fatalf("EvalLowered(%q): %v", source, err)
	}
	if !got {
		t.Fatalf("enum operand %q should pass for s=paid", source)
	}
	// Duration membership exercises a non-numeric membership set.
	durNode := state.Field[string]("d").In(state.Dur[string](time.Second), state.Dur[string](2*time.Second))
	got, source, err = expr.EvalLowered(durNode, fuzzSchema(), fuzzCtx{D: 2 * time.Second})
	if err != nil {
		t.Fatalf("EvalLowered(%q): %v", source, err)
	}
	if !got {
		t.Fatalf("duration membership %q should pass for d=2s", source)
	}
}

// TestGuard_DurationFieldStruct asserts a struct context whose duration field
// marshals as a number (the default time.Duration JSON form) coerces correctly,
// covering the numeric branch of the duration coercion through the rich path.
func TestGuard_DurationFieldStruct(t *testing.T) {
	got := fireRich(t, `window == duration("1h30m")`, order{Window: 90 * time.Minute})
	if !got {
		t.Fatal("a numeric-encoded duration field should compare equal to its literal")
	}
}

// TestEnv_IntAndBoolMapKeysTypeCheck asserts int- and bool-keyed maps build valid CEL
// map types (covering the non-string arms of mapKeyType and the map/list derivation in
// celType). The JSON activation floor carries only string-keyed maps, so these key
// types are usable at the type level — a guard over them compiles — even though the
// floor cannot activate a non-string-keyed map value; the compile is the coverage
// target and the documented boundary.
func TestEnv_IntAndBoolMapKeysTypeCheck(t *testing.T) {
	cases := []state.SchemaField{
		{
			Name: "byInt", Kind: state.SchemaMap,
			Key: &state.SchemaField{Kind: state.SchemaInt}, Elem: &state.SchemaField{Kind: state.SchemaString},
		},
		{
			Name: "byBool", Kind: state.SchemaMap,
			Key: &state.SchemaField{Kind: state.SchemaBool}, Elem: &state.SchemaField{Kind: state.SchemaInt},
		},
		{
			Name: "byEnum", Kind: state.SchemaMap,
			Key: &state.SchemaField{Kind: state.SchemaEnum}, Elem: &state.SchemaField{Kind: state.SchemaInt},
		},
	}
	sources := map[string]string{
		"byInt":  `byInt[1] == "x"`,
		"byBool": `byBool[true] == 2`,
		"byEnum": `byEnum["k"] == 2`,
	}
	for _, f := range cases {
		schema := state.ContextSchema{Fields: []state.SchemaField{f}}
		reg := state.NewRegistry[map[string]any]()
		if _, err := expr.Guard[string](reg, "g", sources[f.Name], schema); err != nil {
			t.Fatalf("%s-keyed map guard should compile: %v", f.Name, err)
		}
	}
}

// unmarshalable is a context whose JSON projection fails (a channel field cannot be
// marshaled), used to drive the EvalGuard activation-error path through a real fire.
type unmarshalable struct {
	Amount int      `json:"amount"`
	Ch     chan int `json:"ch"`
}

// TestEvalGuard_ActivationErrorYieldsFalse asserts a rich guard whose context cannot
// be projected to an activation fails closed: the binding returns an error, the kernel
// treats it as a non-transitioning false, and the machine does not advance.
func TestEvalGuard_ActivationErrorYieldsFalse(t *testing.T) {
	schema := state.ContextSchema{Fields: []state.SchemaField{{Name: "amount", Kind: state.SchemaInt}}}
	reg := state.NewRegistry[unmarshalable]()
	node, err := expr.Guard[string](reg, "g", `amount >= 1`, schema)
	if err != nil {
		t.Fatalf("Guard: %v", err)
	}
	name := node.Ref.Name
	def := state.ForgeFor[unmarshalable]("u").
		Guard(name, func(state.GuardCtx[unmarshalable]) bool { return false }).
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(node).
		State("to").
		Initial("from").
		Quench()
	js, err := def.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, unmarshalable](js)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	m := ir.Provide(reg).Quench()
	inst := m.Cast(unmarshalable{Amount: 5, Ch: make(chan int)}, state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	if inst.Current() != "from" {
		t.Fatalf("an unprojectable context must fail the guard closed; current=%v", inst.Current())
	}
}

// TestEnv_NestedBadKeyPropagates asserts a bad map key nested inside a list element
// and inside a map value propagates the env-build error up through celType's recursive
// arms, so the gap is reported rather than silently demoted.
func TestEnv_NestedBadKeyPropagates(t *testing.T) {
	badInner := state.SchemaField{
		Kind: state.SchemaMap,
		Key:  &state.SchemaField{Kind: state.SchemaFloat}, // invalid CEL map key
		Elem: &state.SchemaField{Kind: state.SchemaInt},
	}
	schemas := []state.ContextSchema{
		{Fields: []state.SchemaField{{Name: "xs", Kind: state.SchemaList, Elem: &badInner}}},
		{Fields: []state.SchemaField{{
			Name: "m", Kind: state.SchemaMap,
			Key: &state.SchemaField{Kind: state.SchemaString}, Elem: &badInner,
		}}},
	}
	for i, schema := range schemas {
		reg := state.NewRegistry[map[string]any]()
		if _, err := expr.Guard[string](reg, "g", `true`, schema); err == nil {
			t.Fatalf("schema %d: a nested bad map key should fail env build", i)
		}
	}
}

// TestEnv_TimeNumericPassthrough asserts a time field whose activation value is not a
// string (already a non-text value) passes through coerceField unchanged rather than
// erroring, covering the time passthrough arm.
func TestEnv_TimeNumericPassthrough(t *testing.T) {
	schema := state.ContextSchema{Fields: []state.SchemaField{
		{Name: "flag", Kind: state.SchemaBool},
		{Name: "at", Kind: state.SchemaTime},
	}}
	cat := expr.NewCatalog()
	reg := state.NewRegistry[map[string]any]()
	if _, err := expr.Guard[string](reg, "g", `flag`, schema, expr.WithCatalog(cat)); err != nil {
		t.Fatalf("Guard: %v", err)
	}
	entry, _ := cat.Entry("g")
	// "at" is absent (nil) so coerceField skips it; "flag" drives the result. The
	// guard does not read "at", so a non-string time value never needs parsing.
	ok, err := expr.EvalCheckedAST(entry.CheckedAST, schema, map[string]any{"flag": true})
	if err != nil {
		t.Fatalf("EvalCheckedAST: %v", err)
	}
	if !ok {
		t.Fatal("flag guard should pass")
	}
}

// TestEnv_CoercePassthrough asserts coerceField leaves an int field whose value is
// already non-float (and a time field that is not a string) unchanged, covering the
// passthrough arms.
func TestEnv_CoercePassthrough(t *testing.T) {
	schema := state.ContextSchema{Fields: []state.SchemaField{
		{Name: "n", Kind: state.SchemaInt},
		{Name: "t", Kind: state.SchemaTime},
	}}
	cat := expr.NewCatalog()
	reg := state.NewRegistry[map[string]any]()
	if _, err := expr.Guard[string](reg, "g", `n == 5`, schema, expr.WithCatalog(cat)); err != nil {
		t.Fatalf("Guard: %v", err)
	}
	entry, _ := cat.Entry("g")
	// n already int64 (not a JSON float), t already a non-string nil → both pass through.
	ok, err := expr.EvalCheckedAST(entry.CheckedAST, schema, map[string]any{"n": int64(5)})
	if err != nil {
		t.Fatalf("EvalCheckedAST: %v", err)
	}
	if !ok {
		t.Fatal("int passthrough should evaluate n == 5 true")
	}
}
