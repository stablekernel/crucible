package state_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// schemaSample is a struct exercising every reflection mapping SchemaOf supports:
// scalars, time/duration special cases, a nested object, a slice, a map, a
// pointer (nullable), a json-tag rename, and a json:"-" skip.
type schemaSample struct {
	Name      string                   `json:"name"`
	Count     int                      `json:"count"`
	Ratio     float64                  `json:"ratio"`
	Active    bool                     `json:"active"`
	CreatedAt time.Time                `json:"createdAt"`
	Timeout   time.Duration            `json:"timeout"`
	Address   schemaAddress            `json:"address"`
	Tags      []string                 `json:"tags"`
	Labels    map[string]int           `json:"labels"`
	Owner     *string                  `json:"owner"`
	Secret    string                   `json:"-"`
	renamed   string                   //nolint:unused // unexported, must be skipped
	Headers   map[string]schemaAddress `json:"headers"`
}

type schemaAddress struct {
	Street string `json:"street"`
	Zip    string `json:"zip"`
}

// fieldByName finds a top-level field in a schema by name, failing the test when
// it is absent.
func fieldByName(t *testing.T, s state.ContextSchema, name string) state.SchemaField {
	t.Helper()
	for _, f := range s.Fields {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("field %q not found in schema", name)
	return state.SchemaField{}
}

func TestSchemaOf_MapsEachVocabularyCase(t *testing.T) {
	s := state.SchemaOf[schemaSample]()

	tests := []struct {
		name string
		kind state.SchemaKind
	}{
		{"name", state.SchemaString},
		{"count", state.SchemaInt},
		{"ratio", state.SchemaFloat},
		{"active", state.SchemaBool},
		{"createdAt", state.SchemaTime},
		{"timeout", state.SchemaDuration},
		{"address", state.SchemaObject},
		{"tags", state.SchemaList},
		{"labels", state.SchemaMap},
		{"owner", state.SchemaString},
		{"headers", state.SchemaMap},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := fieldByName(t, s, tc.name)
			if f.Kind != tc.kind {
				t.Fatalf("field %q kind = %q, want %q", tc.name, f.Kind, tc.kind)
			}
		})
	}
}

func TestSchemaOf_SkipsUnexportedAndDashTaggedFields(t *testing.T) {
	s := state.SchemaOf[schemaSample]()
	for _, f := range s.Fields {
		if f.Name == "Secret" || f.Name == "-" || f.Name == "renamed" {
			t.Fatalf("field %q should have been skipped", f.Name)
		}
	}
}

func TestSchemaOf_NestedObjectFields(t *testing.T) {
	s := state.SchemaOf[schemaSample]()
	addr := fieldByName(t, s, "address")
	if addr.Kind != state.SchemaObject {
		t.Fatalf("address kind = %q, want object", addr.Kind)
	}
	if len(addr.Fields) != 2 {
		t.Fatalf("address fields = %d, want 2", len(addr.Fields))
	}
	if addr.Fields[0].Name != "street" || addr.Fields[0].Kind != state.SchemaString {
		t.Fatalf("address.street = %+v, want string", addr.Fields[0])
	}
}

func TestSchemaOf_SliceElementType(t *testing.T) {
	s := state.SchemaOf[schemaSample]()
	tags := fieldByName(t, s, "tags")
	if tags.Kind != state.SchemaList {
		t.Fatalf("tags kind = %q, want list", tags.Kind)
	}
	if tags.Elem == nil || tags.Elem.Kind != state.SchemaString {
		t.Fatalf("tags elem = %+v, want string element", tags.Elem)
	}
	if !tags.Nullable {
		t.Fatal("a slice field should be nullable")
	}
}

func TestSchemaOf_MapKeyAndValueTypes(t *testing.T) {
	s := state.SchemaOf[schemaSample]()
	labels := fieldByName(t, s, "labels")
	if labels.Key == nil || labels.Key.Kind != state.SchemaString {
		t.Fatalf("labels key = %+v, want string", labels.Key)
	}
	if labels.Elem == nil || labels.Elem.Kind != state.SchemaInt {
		t.Fatalf("labels value = %+v, want int", labels.Elem)
	}

	headers := fieldByName(t, s, "headers")
	if headers.Elem == nil || headers.Elem.Kind != state.SchemaObject {
		t.Fatalf("headers value = %+v, want object", headers.Elem)
	}
	if len(headers.Elem.Fields) != 2 {
		t.Fatalf("headers value object fields = %d, want 2", len(headers.Elem.Fields))
	}
}

func TestSchemaOf_PointerIsNullableAndUnwrapped(t *testing.T) {
	s := state.SchemaOf[schemaSample]()
	owner := fieldByName(t, s, "owner")
	if owner.Kind != state.SchemaString {
		t.Fatalf("owner kind = %q, want string (unwrapped)", owner.Kind)
	}
	if !owner.Nullable {
		t.Fatal("a pointer field should be nullable")
	}
}

func TestSchemaOf_PointerContextUnwrapsToObject(t *testing.T) {
	s := state.SchemaOf[*schemaAddress]()
	if len(s.Fields) != 2 {
		t.Fatalf("*schemaAddress fields = %d, want 2", len(s.Fields))
	}
}

func TestSchemaOf_FieldNameFallsBackToGoName(t *testing.T) {
	type noTag struct {
		Plain string
	}
	s := state.SchemaOf[noTag]()
	if len(s.Fields) != 1 || s.Fields[0].Name != "Plain" {
		t.Fatalf("expected field named Plain, got %+v", s.Fields)
	}
}

func TestSchemaOf_FlattensEmbeddedStruct(t *testing.T) {
	type embedded struct {
		schemaAddress
		Extra string `json:"extra"`
	}
	s := state.SchemaOf[embedded]()
	if _, ok := s.FieldAt("street"); !ok {
		t.Fatal("embedded struct field street should be promoted to the parent object")
	}
	if _, ok := s.FieldAt("extra"); !ok {
		t.Fatal("extra field missing")
	}
}

func TestFieldAt_ResolvesNestedPaths(t *testing.T) {
	s := state.SchemaOf[schemaSample]()

	tests := []struct {
		path string
		kind state.SchemaKind
		ok   bool
	}{
		{"name", state.SchemaString, true},
		{"address", state.SchemaObject, true},
		{"address.street", state.SchemaString, true},
		{"address.zip", state.SchemaString, true},
		{"tags", state.SchemaList, true},
		{"labels", state.SchemaMap, true},
		{"headers.street", state.SchemaString, true}, // descends through map value
		{"address.missing", "", false},
		{"missing", "", false},
		{"name.deeper", "", false}, // scalar has no children
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			f, ok := s.FieldAt(tc.path)
			if ok != tc.ok {
				t.Fatalf("FieldAt(%q) ok = %v, want %v", tc.path, ok, tc.ok)
			}
			if ok && f.Kind != tc.kind {
				t.Fatalf("FieldAt(%q) kind = %q, want %q", tc.path, f.Kind, tc.kind)
			}
		})
	}
}

func TestFieldAt_EmptyPathReturnsRootObject(t *testing.T) {
	s := state.SchemaOf[schemaSample]()
	root, ok := s.FieldAt("")
	if !ok {
		t.Fatal("empty path should resolve to the root object")
	}
	if root.Kind != state.SchemaObject {
		t.Fatalf("root kind = %q, want object", root.Kind)
	}
	if len(root.Fields) != len(s.Fields) {
		t.Fatalf("root fields = %d, want %d", len(root.Fields), len(s.Fields))
	}
}

// docSchema is a hand-authored schema exercising an explicit enum field, which
// reflection cannot derive.
func docSchema() state.ContextSchema {
	return state.ContextSchema{
		Fields: []state.SchemaField{
			{Name: "status", Kind: state.SchemaEnum, Enum: []string{"draft", "review", "published"}},
			{Name: "title", Kind: state.SchemaString},
		},
	}
}

func TestContextSchema_RoundTripLossless(t *testing.T) {
	s := docSchema()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal err = %v", err)
	}
	var got state.ContextSchema
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal err = %v", err)
	}
	status, ok := got.FieldAt("status")
	if !ok || status.Kind != state.SchemaEnum {
		t.Fatalf("status field lost on round-trip: %+v ok=%v", status, ok)
	}
	if len(status.Enum) != 3 {
		t.Fatalf("enum values = %v, want 3", status.Enum)
	}
}

func TestContextSchema_DeterministicEncoding(t *testing.T) {
	s := state.SchemaOf[schemaSample]()
	first, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal err = %v", err)
	}
	for i := 0; i < 20; i++ {
		again, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("Marshal err = %v", err)
		}
		if string(again) != string(first) {
			t.Fatalf("encoding not deterministic:\n%s\n%s", first, again)
		}
	}
}

func TestContextSchema_PreservesUnknownKind(t *testing.T) {
	// A newer producer emits a field kind and sidecar key this build does not model.
	raw := `{"fields":[{"name":"geo","kind":"geopoint","srid":4326}]}`
	var s state.ContextSchema
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("Unmarshal err = %v", err)
	}
	if len(s.Fields) != 1 {
		t.Fatalf("fields = %d, want 1", len(s.Fields))
	}
	if s.Fields[0].Kind != "geopoint" {
		t.Fatalf("kind = %q, want geopoint preserved", s.Fields[0].Kind)
	}
	out, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal err = %v", err)
	}
	// The unknown sidecar key survives the round-trip.
	var reparsed map[string]any
	if err := json.Unmarshal(out, &reparsed); err != nil {
		t.Fatalf("reparse err = %v", err)
	}
	fields, _ := reparsed["fields"].([]any)
	if len(fields) != 1 {
		t.Fatalf("reparsed fields = %v", reparsed["fields"])
	}
	field0, _ := fields[0].(map[string]any)
	if field0["srid"] != float64(4326) {
		t.Fatalf("unknown key srid not preserved: %+v", field0)
	}
}

// schemaTestIR builds a minimal machine and serializes it, optionally attaching a
// context schema, returning the JSON bytes.
func schemaTestIR(t *testing.T, withSchema bool) []byte {
	t.Helper()
	b := state.ForgeFor[*schemaSample]("ctx-machine")
	if withSchema {
		b = b.WithContextSchema(state.SchemaOf[schemaSample]())
	}
	m := b.State("idle").Initial("idle").Quench()
	out, err := m.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	return out
}

func TestIR_WithContextSchema_RoundTripLossless(t *testing.T) {
	out := schemaTestIR(t, true)

	ir, err := state.LoadFromJSON[string, string, *schemaSample](out)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	if ir.Context == nil {
		t.Fatal("IR.Context lost on load")
	}
	if _, ok := ir.Context.FieldAt("address.street"); !ok {
		t.Fatal("nested schema field lost on IR round-trip")
	}

	// Re-emit through Provide -> Quench -> ToJSON; the schema must survive.
	m2 := ir.Provide(state.NewRegistry[*schemaSample]()).Quench()
	out2, err := m2.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("re-ToJSON err = %v", err)
	}
	if string(out2) != string(out) {
		t.Fatalf("IR with schema not lossless across Provide:\n%s\n%s", out, out2)
	}
}

func TestIR_WithContextSchema_Deterministic(t *testing.T) {
	first := schemaTestIR(t, true)
	for i := 0; i < 10; i++ {
		if again := schemaTestIR(t, true); string(again) != string(first) {
			t.Fatalf("IR encoding not deterministic:\n%s\n%s", first, again)
		}
	}
}

func TestIR_AbsentSchema_RoundTripsUnchanged(t *testing.T) {
	out := schemaTestIR(t, false)
	// No "context" key should appear when no schema is attached.
	var doc map[string]any
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("Unmarshal err = %v", err)
	}
	if _, present := doc["context"]; present {
		t.Fatal("absent schema should omit the context key (omitempty)")
	}

	ir, err := state.LoadFromJSON[string, string, *schemaSample](out)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	if ir.Context != nil {
		t.Fatal("IR.Context should be nil when no schema was attached")
	}
	m2 := ir.Provide(state.NewRegistry[*schemaSample]()).Quench()
	out2, err := m2.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("re-ToJSON err = %v", err)
	}
	if string(out2) != string(out) {
		t.Fatalf("absent-schema IR not lossless:\n%s\n%s", out, out2)
	}
}

func TestIR_PreservesUnknownSchemaKind_AcrossIR(t *testing.T) {
	out := schemaTestIR(t, true)
	// Splice an unknown-kind field into the serialized context schema, simulating a
	// newer producer, then confirm it survives load -> save at the IR level.
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("Unmarshal err = %v", err)
	}
	doc["context"] = json.RawMessage(`{"fields":[{"name":"geo","kind":"geopoint","srid":4326}]}`)
	spliced, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal err = %v", err)
	}

	ir, err := state.LoadFromJSON[string, string, *schemaSample](spliced)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	m2 := ir.Provide(state.NewRegistry[*schemaSample]()).Quench()
	out2, err := m2.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("re-ToJSON err = %v", err)
	}
	var reparsed map[string]any
	if err := json.Unmarshal(out2, &reparsed); err != nil {
		t.Fatalf("reparse err = %v", err)
	}
	ctx, _ := reparsed["context"].(map[string]any)
	fields, _ := ctx["fields"].([]any)
	if len(fields) != 1 {
		t.Fatalf("context fields lost: %+v", ctx)
	}
	field0, _ := fields[0].(map[string]any)
	if field0["kind"] != "geopoint" || field0["srid"] != float64(4326) {
		t.Fatalf("unknown schema kind/key not preserved across IR: %+v", field0)
	}
}

func TestSchemaOf_NonStructContextYieldsEmptyObject(t *testing.T) {
	// A map context is not an object-shaped struct; SchemaOf yields an empty field
	// set rather than guessing a shape.
	s := state.SchemaOf[map[string]int]()
	if len(s.Fields) != 0 {
		t.Fatalf("non-struct context fields = %d, want 0", len(s.Fields))
	}
}

func TestSchemaOf_InterfaceFieldIsAny(t *testing.T) {
	// An interface field cannot be reflected to a narrower schema; SchemaOf reports
	// the honest SchemaAny kind rather than dishonestly coercing it to a string,
	// which would give a freeze-time type-check false confidence about its shape.
	type withAny struct {
		Payload any `json:"payload"`
	}
	s := state.SchemaOf[withAny]()
	f := fieldByName(t, s, "payload")
	if f.Kind != state.SchemaAny {
		t.Fatalf("interface field kind = %q, want %q", f.Kind, state.SchemaAny)
	}
}

func TestSchemaOf_JSONTagOmitemptyKeepsName(t *testing.T) {
	type tagged struct {
		Kept    string `json:"kept,omitempty"`
		Renamed string `json:",omitempty"` // empty name -> Go field name
	}
	s := state.SchemaOf[tagged]()
	if _, ok := s.FieldAt("kept"); !ok {
		t.Fatal("json:\"kept,omitempty\" should keep the name kept")
	}
	if _, ok := s.FieldAt("Renamed"); !ok {
		t.Fatal("json:\",omitempty\" with empty name should fall back to the Go field name")
	}
}

func TestWithContextSchema_DoesNotAliasCaller(t *testing.T) {
	schema := state.SchemaOf[schemaSample]()
	b := state.ForgeFor[*schemaSample]("m").WithContextSchema(schema)
	// Mutate the caller's copy after attaching; the builder must hold a clone.
	schema.Fields[0].Name = "mutated"
	m := b.State("idle").Initial("idle").Quench()
	out, err := m.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, *schemaSample](out)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	if _, ok := ir.Context.FieldAt("name"); !ok {
		t.Fatal("builder schema aliased the caller's slice (saw mutated field name)")
	}
}
