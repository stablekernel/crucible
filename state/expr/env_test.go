package expr_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// richCtx exercises every schema kind the env maps: scalars, time, duration, a
// nested object, a list, and a string-keyed map. Its JSON tags are the CEL variable
// names.
type richCtx struct {
	Name    string         `json:"name"`
	Count   int            `json:"count"`
	Ratio   float64        `json:"ratio"`
	Active  bool           `json:"active"`
	Wait    time.Duration  `json:"wait"`
	At      time.Time      `json:"at"`
	Nested  nested         `json:"nested"`
	Tags    []string       `json:"tags"`
	Lengths map[string]int `json:"lengths"`
}

type nested struct {
	Inner string `json:"inner"`
}

func richSchema() state.ContextSchema { return state.SchemaOf[richCtx]() }

func sampleRich() richCtx {
	return richCtx{
		Name:    "widget",
		Count:   4,
		Ratio:   1.5,
		Active:  true,
		Wait:    2 * time.Second,
		At:      time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Nested:  nested{Inner: "deep"},
		Tags:    []string{"x", "y"},
		Lengths: map[string]int{"a": 1},
	}
}

// TestEnv_AllKinds compiles and evaluates a guard touching every schema kind, so the
// scalar, time, duration, object (map<string,dyn>), list, and map type mappings and
// their activation coercions are all exercised end to end.
func TestEnv_AllKinds(t *testing.T) {
	cases := []struct {
		name   string
		source string
		want   bool
	}{
		{"string", `name == "widget"`, true},
		{"int", `count == 4`, true},
		{"float", `ratio > 1.0`, true},
		{"bool", `active`, true},
		{"duration", `wait >= duration("2s")`, true},
		{"time getter", `at.getFullYear() == 2026`, true},
		{"nested object", `nested.inner == "deep"`, true},
		{"list index", `tags[0] == "x"`, true},
		{"list size", `size(tags) == 2`, true},
		{"map index", `lengths["a"] == 1`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := state.NewRegistry[richCtx]()
			node, err := expr.Guard[string](reg, "g", tc.source, richSchema())
			if err != nil {
				t.Fatalf("Guard(%q): %v", tc.source, err)
			}
			got := fireRichCtx(t, reg, node, sampleRich())
			if got != tc.want {
				t.Fatalf("source %q: enabled=%v, want %v", tc.source, got, tc.want)
			}
		})
	}
}

// TestEnv_RejectsBadMapKey asserts a map whose key type CEL cannot express (a float
// key) is reported at env build rather than silently demoted.
func TestEnv_RejectsBadMapKey(t *testing.T) {
	schema := state.ContextSchema{Fields: []state.SchemaField{{
		Name: "weird",
		Kind: state.SchemaMap,
		Key:  &state.SchemaField{Kind: state.SchemaFloat},
		Elem: &state.SchemaField{Kind: state.SchemaInt},
	}}}
	reg := state.NewRegistry[map[string]any]()
	_, err := expr.Guard[string](reg, "g", `true`, schema)
	if err == nil || !strings.Contains(err.Error(), "map key") {
		t.Fatalf("expected a map-key error, got %v", err)
	}
}

// TestEnv_EnumIsConstrainedString asserts an authored enum field types as a CEL
// string, matching Core's "enum is a constrained string" stance.
func TestEnv_EnumIsConstrainedString(t *testing.T) {
	schema := state.ContextSchema{Fields: []state.SchemaField{{
		Name: "status",
		Kind: state.SchemaEnum,
		Enum: []string{"open", "paid"},
	}}}
	reg := state.NewRegistry[map[string]any]()
	if _, err := expr.Guard[string](reg, "g", `status == "paid"`, schema); err != nil {
		t.Fatalf("enum-as-string guard should compile: %v", err)
	}
}

// TestEnv_TimeAndDurationStringForms asserts a context that marshals time and
// duration as their string wire forms (RFC 3339 and a Go duration string) is coerced
// correctly into the activation. A map context marshals these as strings, exercising
// the string branches of coerceField that a time.Duration/time.Time struct field does
// not.
func TestEnv_TimeAndDurationStringForms(t *testing.T) {
	schema := state.ContextSchema{Fields: []state.SchemaField{
		{Name: "wait", Kind: state.SchemaDuration},
		{Name: "at", Kind: state.SchemaTime},
	}}
	entity := map[string]any{"wait": "1500ms", "at": "2030-06-01T00:00:00Z"}
	// Evaluate through the checked-AST helper, which uses the same activation
	// marshaling a fired rich guard does, so the string-form coercions are exercised.
	cat := expr.NewCatalog()
	reg := state.NewRegistry[map[string]any]()
	if _, err := expr.Guard[string](reg, "g", `wait >= duration("1s") && at.getFullYear() == 2030`, schema, expr.WithCatalog(cat)); err != nil {
		t.Fatalf("Guard(catalog): %v", err)
	}
	entry, _ := cat.Entry("g")
	ok, err := expr.EvalCheckedAST(entry.CheckedAST, schema, entity)
	if err != nil {
		t.Fatalf("EvalCheckedAST: %v", err)
	}
	if !ok {
		t.Fatal("string-form time/duration context should satisfy the guard")
	}
}

// TestEnv_BadTimeStringErrors asserts a malformed time/duration string surfaces an
// error from the activation rather than a silent miss.
func TestEnv_BadTimeStringErrors(t *testing.T) {
	schema := state.ContextSchema{Fields: []state.SchemaField{{Name: "at", Kind: state.SchemaTime}}}
	cat := expr.NewCatalog()
	reg := state.NewRegistry[map[string]any]()
	if _, err := expr.Guard[string](reg, "g", `at.getFullYear() == 2030`, schema, expr.WithCatalog(cat)); err != nil {
		t.Fatalf("Guard: %v", err)
	}
	entry, _ := cat.Entry("g")
	if _, err := expr.EvalCheckedAST(entry.CheckedAST, schema, map[string]any{"at": "not-a-time"}); err == nil {
		t.Fatal("a malformed time string should error")
	}
}

// fireRichCtx fires a one-edge machine carrying node against a richCtx context and
// reports whether the transition was enabled, binding the rich guard via Provide.
func fireRichCtx(t *testing.T, reg *state.Registry[richCtx], node state.GuardNode[string], e richCtx) bool {
	t.Helper()
	name := node.Ref.Name
	def := state.Forge[string, string, richCtx]("rc").
		Guard(name, func(state.GuardCtx[richCtx]) bool { return false }).
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(node).
		State("to").
		Initial("from").
		Quench()
	js, err := def.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, richCtx](js)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	m := ir.Provide(reg).Quench()
	inst := m.Cast(e, state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	return inst.Current() == "to"
}
