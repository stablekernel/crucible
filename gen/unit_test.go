package gen

import (
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// irWith builds a minimal IR value directly (no DSL) so a single state can carry
// arbitrary behavior refs for focused walk/dedup/collision tests.
func irWith(schema *state.ContextSchema, states ...state.State[string, string, order]) state.IR[string, string, order] {
	return state.IR[string, string, order]{
		Name:    "t",
		Context: schema,
		States:  states,
	}
}

func TestEject_NilAndEmptySchemaAlias(t *testing.T) {
	cases := []struct {
		name   string
		schema *state.ContextSchema
	}{
		{"nil", nil},
		{"empty", &state.ContextSchema{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src, err := Eject(irWith(tc.schema))
			if err != nil {
				t.Fatalf("Eject: %v", err)
			}
			got := string(src)
			if !strings.Contains(got, "type Context = map[string]any") {
				t.Errorf("expected map alias, got:\n%s", got)
			}
			if strings.Contains(got, "type Context struct") {
				t.Errorf("expected alias, not struct:\n%s", got)
			}
		})
	}
}

func TestSchemaFieldGoType(t *testing.T) {
	cases := []struct {
		kind state.SchemaKind
		want string
	}{
		{state.SchemaString, "string"},
		{state.SchemaEnum, "string"},
		{state.SchemaInt, "int64"},
		{state.SchemaFloat, "float64"},
		{state.SchemaBool, "bool"},
		{state.SchemaDuration, "time.Duration"},
		{state.SchemaTime, "time.Time"},
		{state.SchemaObject, "map[string]any"},
		{state.SchemaMap, "map[string]any"},
		{state.SchemaList, "[]any"},
		{state.SchemaAny, "any"},
		{state.SchemaKind("unknownfuturekind"), "any"},
	}
	for _, tc := range cases {
		t.Run(string(tc.kind), func(t *testing.T) {
			got := schemaFieldGoType(state.SchemaField{Kind: tc.kind}, newImportSet())
			if got != tc.want {
				t.Errorf("kind %q: got %q, want %q", tc.kind, got, tc.want)
			}
		})
	}
}

func TestSanitizeIdent(t *testing.T) {
	cases := map[string]string{
		"order_total": "OrderTotal",
		"has-items":   "HasItems",
		"order.total": "OrderTotal",
		"isPaid":      "IsPaid",
		"3way":        "X3way",
		"":            "Behavior",
		"@@@":         "Behavior",
	}
	for in, want := range cases {
		if got := sanitizeIdent(in); got != want {
			t.Errorf("sanitizeIdent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEject_DedupAndSort(t *testing.T) {
	// One state, two transitions naming the same guard "g" twice and actions out
	// of alphabetical order. Expect a single g stub and z-before-a sorted output.
	st := state.State[string, string, order]{
		Name: "s",
		Transitions: []state.Transition[string, string, order]{
			{From: "s", To: "s", On: "e1", Guards: []state.Ref{{Name: "g"}}, Effects: []state.Ref{{Name: "zAct"}}},
			{From: "s", To: "s", On: "e2", Guards: []state.Ref{{Name: "g"}}, Effects: []state.Ref{{Name: "aAct"}}},
		},
	}
	src, err := Eject(irWith(nil, st))
	if err != nil {
		t.Fatalf("Eject: %v", err)
	}
	got := string(src)
	if n := strings.Count(got, "func g(ctx state.GuardCtx"); n != 1 {
		t.Errorf("expected exactly one guard stub for g, got %d\n%s", n, got)
	}
	ai := strings.Index(got, "func aAct(")
	zi := strings.Index(got, "func zAct(")
	if ai < 0 || zi < 0 || ai > zi {
		t.Errorf("actions not sorted ascending (aAct before zAct): aAct@%d zAct@%d\n%s", ai, zi, got)
	}
}

func TestEject_CollisionAcrossKinds(t *testing.T) {
	// The same bare name "process" appears as a guard, an action, an assign, and a
	// service. Identifiers must be unique; registration strings stay the original.
	st := state.State[string, string, order]{
		Name:          "s",
		OnEntry:       []state.Ref{{Name: "process"}}, // action
		OnEntryAssign: []state.Ref{{Name: "process"}}, // assign
		Invoke:        []state.Invocation[string, string, order]{{Src: state.Ref{Name: "process"}}},
		Transitions: []state.Transition[string, string, order]{
			{From: "s", To: "s", On: "e", Guards: []state.Ref{{Name: "process"}}}, // guard
		},
	}
	src, err := Eject(irWith(nil, st))
	if err != nil {
		t.Fatalf("Eject: %v", err)
	}
	got := string(src)
	for _, want := range []string{
		"func processGuard(ctx state.GuardCtx[Context]) bool",
		"func processAction(ctx state.ActionCtx[Context]) (state.Effect, error)",
		"func processAssign(in state.AssignCtx[Context]) Context",
		"func processService(ctx context.Context, in state.ServiceCtx[Context]) (any, error)",
		`reg.Guard("process", processGuard)`,
		`reg.Action("process", processAction)`,
		`reg.Reducer("process", processAssign)`,
		`reg.Service("process", processService)`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("collision handling missing %q\n---\n%s", want, got)
		}
	}
}
