package state_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// fieldShape is one exported field of a serialized struct, captured as its Go
// field NAME and its full `json:` struct tag. Together these are the wire
// contract a recorded document depends on, so the frozen expectation pins both.
type fieldShape struct {
	Name    string
	JSONTag string
}

// reflectShape walks the exported fields of a struct type and returns each
// field's name and its `json:` tag, in declaration order. Unexported fields
// (e.g. the `extra` round-trip buffer) carry no wire shape and are skipped, so
// the frozen expectation pins exactly the bytes that cross the wire.
func reflectShape(t *testing.T, v any) []fieldShape {
	t.Helper()
	rt := reflect.TypeOf(v)
	if rt.Kind() != reflect.Struct {
		t.Fatalf("reflectShape: %T is not a struct", v)
	}
	var out []fieldShape
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if f.PkgPath != "" { // unexported: no wire shape
			continue
		}
		out = append(out, fieldShape{Name: f.Name, JSONTag: f.Tag.Get("json")})
	}
	return out
}

// assertShape compares a serialized type's reflected exported-field shape against
// the frozen expectation, field by field, and fails on any rename, removal,
// reorder, retag, or unexpected addition.
func assertShape(t *testing.T, name string, got, want []fieldShape) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s field count = %d, want %d (frozen v1 wire shape)\n got: %+v\nwant: %+v",
			name, len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s field[%d] = {Name:%q JSONTag:%q}, want {Name:%q JSONTag:%q} (frozen v1 wire shape)",
				name, i, got[i].Name, got[i].JSONTag, want[i].Name, want[i].JSONTag)
		}
	}
}

// TestWireShape_Frozen is the v1.0 WIRE FREEZE guard for the serialized IR and
// palette structs. It reflects over each serialized type and pins the exact set,
// order, NAMES, and `json:` tags (including the omitempty flag) of its exported
// fields. A recorded document is parsed by these names and tags, so renaming,
// removing, reordering, or retagging any field is a breaking wire change and must
// break this test.
//
// HOW TO UPDATE (deliberately, additive only): appending a new optional field
// with an `omitempty` tag is a backward-compatible (minor) change — add the new
// {Name, JSONTag} entry to the END of the relevant want slice in the same order
// the struct declares it. Any rename/removal/reorder/retag is a breaking (major)
// change and the test SHOULD fail until the wire-format version is bumped
// deliberately. The `extra` unexported round-trip buffer carries no wire shape
// and is intentionally not pinned here.
func TestWireShape_Frozen(t *testing.T) {
	t.Parallel()

	// Generic IR types are instantiated at concrete string/string/any params; the
	// type params do not change a field's name or json tag, only the element type.
	type S = string
	type E = string
	type C = any

	cases := []struct {
		name string
		zero any
		want []fieldShape
	}{
		{
			name: "IR",
			zero: state.IR[S, E, C]{},
			want: []fieldShape{
				{"SchemaVersion", "schemaVersion,omitempty"},
				{"ID", "id,omitempty"},
				{"Name", "name"},
				{"Version", "version,omitempty"},
				{"Input", "input,omitempty"},
				{"Output", "output,omitempty"},
				{"Context", "context,omitempty"},
				{"States", "states,omitempty"},
				{"Initial", "initial"},
				{"HasInitial", "hasInitial"},
				{"Meta", "meta,omitempty"},
			},
		},
		{
			name: "State",
			zero: state.State[S, E, C]{},
			want: []fieldShape{
				{"Name", "name"},
				{"OwnedBy", "ownedBy,omitempty"},
				{"Transitions", "transitions,omitempty"},
				{"OnEntry", "onEntry,omitempty"},
				{"OnExit", "onExit,omitempty"},
				{"IsFinal", "isFinal,omitempty"},
				{"OnDone", "onDone,omitempty"},
				{"OnEntryAssign", "onEntryAssign,omitempty"},
				{"OnExitAssign", "onExitAssign,omitempty"},
				{"Children", "children,omitempty"},
				{"InitialChild", "initialChild,omitempty"},
				{"Regions", "regions,omitempty"},
				{"HistoryType", "historyType,omitempty"},
				{"HistoryDefault", "historyDefault,omitempty"},
				{"Invoke", "invoke,omitempty"},
				{"Parent", "-"},
				{"Meta", "meta,omitempty"},
			},
		},
		{
			name: "Transition",
			zero: state.Transition[S, E, C]{},
			want: []fieldShape{
				{"From", "from"},
				{"To", "to"},
				{"On", "on"},
				{"Guards", "guards,omitempty"},
				{"Effects", "effects,omitempty"},
				{"WaitMode", "waitMode,omitempty"},
				{"Assigns", "assigns,omitempty"},
				{"GuardExpr", "guardExpr,omitempty"},
				{"Internal", "internal,omitempty"},
				{"EventLess", "eventLess,omitempty"},
				{"After", "after,omitempty"},
				{"Wildcard", "wildcard,omitempty"},
				{"Forbidden", "forbidden,omitempty"},
				{"Reenter", "reenter,omitempty"},
				{"Raise", "raise,omitempty"},
				{"SrcFile", "srcFile,omitempty"},
				{"SrcLine", "srcLine,omitempty"},
				{"Meta", "meta,omitempty"},
			},
		},
		{
			name: "Region",
			zero: state.Region[S, E, C]{},
			want: []fieldShape{
				{"Name", "name"},
				{"States", "states,omitempty"},
				{"InitialChild", "initialChild,omitempty"},
				{"Meta", "meta,omitempty"},
			},
		},
		{
			name: "Invocation",
			zero: state.Invocation[S, E, C]{},
			want: []fieldShape{
				{"ID", "id,omitempty"},
				{"Src", "src"},
				{"Input", "input,omitempty"},
				{"OnDone", "onDone"},
				{"OnError", "onError"},
				{"Kind", "kind,omitempty"},
				{"SystemID", "systemId,omitempty"},
				{"Meta", "meta,omitempty"},
			},
		},
		{
			name: "Ref",
			zero: state.Ref{},
			want: []fieldShape{
				{"Name", "name"},
				{"Params", "params,omitempty"},
				{"Meta", "meta,omitempty"},
			},
		},
		{
			name: "ContextSchema",
			zero: state.ContextSchema{},
			want: []fieldShape{
				{"Fields", "fields,omitempty"},
				{"Meta", "meta,omitempty"},
			},
		},
		{
			name: "Descriptor",
			zero: state.Descriptor{},
			want: []fieldShape{
				{"Kind", "kind"},
				{"Name", "name"},
				{"Description", "description,omitempty"},
				{"Category", "category,omitempty"},
				{"Examples", "examples,omitempty"},
				{"Params", "params,omitempty"},
				{"Reads", "reads,omitempty"},
				{"Writes", "writes,omitempty"},
				{"Binding", "binding,omitempty"},
			},
		},
		{
			name: "ParamSpec",
			zero: state.ParamSpec{},
			want: []fieldShape{
				{"Name", "name"},
				{"Type", "type"},
				{"Required", "required,omitempty"},
				{"Description", "description,omitempty"},
				{"Default", "default,omitempty"},
				{"Enum", "enum,omitempty"},
				{"Examples", "examples,omitempty"},
			},
		},
		{
			name: "BindingSpec",
			zero: state.BindingSpec{},
			want: []fieldShape{
				{"Transport", "transport,omitempty"},
				{"Meta", "meta,omitempty"},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertShape(t, tc.name, reflectShape(t, tc.zero), tc.want)
		})
	}
}

// TestEnumWireValues_Frozen pins the numeric wire value of every closed-int enum
// that serializes as a bare integer (WaitMode, HistoryType, ActorKind). These
// integers are part of the frozen v1.0 wire contract: a recorded document encodes
// them by value, so a drift here is a silent wire-format break. The integers may
// only be appended to, never reordered or repurposed.
func TestEnumWireValues_Frozen(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		val  any
		want int
	}{
		{"SyncReply", state.SyncReply, 0},
		{"FireAndForget", state.FireAndForget, 1},
		{"ValidatePoll", state.ValidatePoll, 2},
		{"HistoryNone", state.HistoryNone, 0},
		{"HistoryShallow", state.HistoryShallow, 1},
		{"HistoryDeep", state.HistoryDeep, 2},
		{"ActorKindService", state.ActorKindService, 0},
		{"ActorKindMachine", state.ActorKindMachine, 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tc.val)
			if err != nil {
				t.Fatalf("marshal %s: %v", tc.name, err)
			}
			var got int
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("unmarshal %s wire value %q: %v", tc.name, b, err)
			}
			if got != tc.want {
				t.Fatalf("%s wire value = %d, want %d (frozen)", tc.name, got, tc.want)
			}
		})
	}
}

// TestSchemaOf_UnreflectableKindsAreHonest asserts that SchemaOf maps a Go type it
// cannot reflect to a narrower schema (interface, func, chan, complex) to the
// honest SchemaAny kind rather than silently coercing it to SchemaString — which
// would give a freeze-time type-check false confidence about the field's shape.
func TestSchemaOf_UnreflectableKindsAreHonest(t *testing.T) {
	t.Parallel()
	type withIface struct {
		V any `json:"v"`
	}
	type withFunc struct {
		F func() `json:"f"`
	}
	type withChan struct {
		C chan int `json:"c"`
	}
	type withComplex struct {
		Z complex128 `json:"z"`
	}

	check := func(t *testing.T, schema state.ContextSchema, field string) {
		t.Helper()
		for _, f := range schema.Fields {
			if f.Name == field {
				if f.Kind != state.SchemaAny {
					t.Fatalf("field %q kind = %q, want %q (honest, not coerced to string)", field, f.Kind, state.SchemaAny)
				}
				return
			}
		}
		t.Fatalf("field %q not found in derived schema", field)
	}

	check(t, state.SchemaOf[withIface](), "v")
	check(t, state.SchemaOf[withFunc](), "f")
	check(t, state.SchemaOf[withChan](), "c")
	check(t, state.SchemaOf[withComplex](), "z")
}

// TestJournalRandom_RidesPayload documents and locks the v1.0 decision that a
// JournalRandom entry carries its recorded randomness draw on the shared
// JournalEntry.Payload field (the same channel as a service result), so the
// variant needs no dedicated backing field. A randomness entry round-trips its
// Kind and Payload like any other journal entry.
func TestJournalRandom_RidesPayload(t *testing.T) {
	t.Parallel()
	entry := state.JournalEntry{
		Step:    3,
		Kind:    state.JournalRandom,
		Payload: json.RawMessage(`{"draw":0.42}`),
	}
	b, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal JournalEntry: %v", err)
	}
	var got state.JournalEntry
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal JournalEntry: %v", err)
	}
	if got.Kind != state.JournalRandom {
		t.Fatalf("Kind = %q, want %q", got.Kind, state.JournalRandom)
	}
	if string(got.Payload) != `{"draw":0.42}` {
		t.Fatalf("Payload = %s, want the recorded randomness draw", got.Payload)
	}
}
