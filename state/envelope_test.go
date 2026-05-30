package state_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// versionedIR builds a small IR document carrying every new envelope field —
// schema version, identity, definition version, input/output slots, and Meta at
// machine, state, transition, and ref granularity — so the round-trip and
// determinism tests exercise the whole envelope surface.
func versionedIR() state.IR[DocState, DocEvent, *Document] {
	return state.IR[DocState, DocEvent, *Document]{
		SchemaVersion: state.CurrentSchemaVersion,
		ID:            "doc.machine.v1",
		Name:          "document",
		Version:       "2.3.1",
		Input: &state.IOSpec{
			Description: "the document to drive",
			Schema:      map[string]any{"type": "object"},
		},
		Output: &state.IOSpec{
			Description: "the published document",
		},
		Meta: map[string]any{
			"crucible.binding": map[string]any{"kind": "go"},
			"studio.layout":    map[string]any{"zoom": 1.0},
		},
		Initial:    Draft,
		HasInitial: true,
		States: []state.State[DocState, DocEvent, *Document]{
			{
				Name: Draft,
				Meta: map[string]any{"doc.description": "the draft state"},
				Transitions: []state.Transition[DocState, DocEvent, *Document]{
					{
						From: Draft,
						To:   Submitted,
						On:   Submit,
						Meta: map[string]any{"studio.waypoints": []any{1.0, 2.0}},
						Effects: []state.Ref{
							{
								Name:   "emit",
								Params: map[string]any{"event": "submitted"},
								Meta:   map[string]any{"crucible.binding": map[string]any{"kind": "go"}},
							},
						},
					},
				},
			},
			{Name: Submitted},
		},
	}
}

// TestEnvelope_RoundTrip_MetaAndVersionSurvive asserts every new envelope field —
// SchemaVersion, ID, Version, Input, Output, and Meta at all four granularities —
// survives a Marshal -> LoadFromJSON cycle unchanged.
func TestEnvelope_RoundTrip_MetaAndVersionSurvive(t *testing.T) {
	src := versionedIR()
	b, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("Marshal err = %v", err)
	}

	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](b)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}

	if ir.SchemaVersion != src.SchemaVersion {
		t.Errorf("SchemaVersion = %q, want %q", ir.SchemaVersion, src.SchemaVersion)
	}
	if ir.ID != src.ID {
		t.Errorf("ID = %q, want %q", ir.ID, src.ID)
	}
	if ir.Version != src.Version {
		t.Errorf("Version = %q, want %q", ir.Version, src.Version)
	}
	if ir.Input == nil || ir.Input.Description != "the document to drive" {
		t.Errorf("Input not preserved: %+v", ir.Input)
	}
	if ir.Output == nil || ir.Output.Description != "the published document" {
		t.Errorf("Output not preserved: %+v", ir.Output)
	}
	if _, ok := ir.Meta["crucible.binding"]; !ok {
		t.Errorf("machine Meta crucible.binding missing: %v", ir.Meta)
	}
	if len(ir.States) != 2 {
		t.Fatalf("states len = %d, want 2", len(ir.States))
	}
	if _, ok := ir.States[0].Meta["doc.description"]; !ok {
		t.Errorf("state Meta missing: %v", ir.States[0].Meta)
	}
	if len(ir.States[0].Transitions) != 1 {
		t.Fatalf("transitions len = %d, want 1", len(ir.States[0].Transitions))
	}
	tr := ir.States[0].Transitions[0]
	if _, ok := tr.Meta["studio.waypoints"]; !ok {
		t.Errorf("transition Meta missing: %v", tr.Meta)
	}
	if len(tr.Effects) != 1 {
		t.Fatalf("effects len = %d, want 1", len(tr.Effects))
	}
	if _, ok := tr.Effects[0].Meta["crucible.binding"]; !ok {
		t.Errorf("ref Meta missing: %v", tr.Effects[0].Meta)
	}
}

// TestEnvelope_DeterministicEncoding asserts the IR encodes to byte-identical
// output across repeated marshals — the canonical-encoding guarantee golden
// diffing and (future) digesting depend on. Meta maps with multiple keys are the
// adversarial case: map iteration order must not leak into the bytes.
func TestEnvelope_DeterministicEncoding(t *testing.T) {
	src := versionedIR()

	first, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("Marshal err = %v", err)
	}
	for i := 0; i < 50; i++ {
		next, err := json.Marshal(src)
		if err != nil {
			t.Fatalf("Marshal err = %v", err)
		}
		if !bytes.Equal(first, next) {
			t.Fatalf("encoding not deterministic on iteration %d:\n first=%s\n next =%s", i, first, next)
		}
	}
}

// TestEnvelope_PreserveUnknownFields asserts a v1 loader round-trips unknown
// top-level and node-level JSON keys verbatim (forward-compat): a future producer
// may emit fields a v1.0 consumer does not model, and they must survive a
// load -> save cycle rather than being silently dropped.
func TestEnvelope_PreserveUnknownFields(t *testing.T) {
	doc := []byte(`{` +
		`"schemaVersion":"1.0",` +
		`"name":"document",` +
		`"futureTopLevel":{"k":"v"},` +
		`"initial":0,` +
		`"hasInitial":true,` +
		`"states":[{"name":0,"futureStateField":42,` +
		`"transitions":[{"from":0,"to":1,"on":0,"futureEdgeField":true,` +
		`"effects":[{"name":"emit","futureRefField":"x"}]}]}]` +
		`}`)

	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](doc)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}

	out, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("Marshal err = %v", err)
	}

	for _, key := range []string{"futureTopLevel", "futureStateField", "futureEdgeField", "futureRefField"} {
		if !strings.Contains(string(out), key) {
			t.Errorf("unknown key %q dropped on round-trip:\n%s", key, out)
		}
	}
}

// TestEnvelope_PreserveUnknownVariants asserts unknown enum/node variants survive
// a round-trip. A guard expression carrying an op a v1 kernel does not recognize
// must be preserved verbatim on load -> save (forward-compat), then rejected at
// evaluation rather than silently dropped or mis-evaluated.
func TestEnvelope_PreserveUnknownVariants(t *testing.T) {
	doc := []byte(`{` +
		`"schemaVersion":"1.0","name":"document","initial":0,"hasInitial":true,` +
		`"states":[{"name":0,"transitions":[{"from":0,"to":1,"on":0,` +
		`"guardExpr":{"op":"futureOp","children":[{"op":"leaf","ref":{"name":"hasReviewer"}}]}}]},` +
		`{"name":1}]}`)

	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](doc)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	out, err := json.Marshal(ir)
	if err != nil {
		t.Fatalf("Marshal err = %v", err)
	}
	if !strings.Contains(string(out), "futureOp") {
		t.Errorf("unknown guard op dropped on round-trip:\n%s", out)
	}
}

// TestLoadFromJSON_RejectHigherMajor asserts the reject-higher-major load policy:
// a document declaring a schema major greater than the loader's is refused, while
// a higher minor (still the same major) loads, preserving forward-compat within a
// major line.
func TestLoadFromJSON_RejectHigherMajor(t *testing.T) {
	tests := []struct {
		name      string
		version   string
		wantError bool
	}{
		{name: "current loads", version: "1.0", wantError: false},
		{name: "higher minor loads", version: "1.9", wantError: false},
		{name: "missing version loads", version: "", wantError: false},
		{name: "higher major rejected", version: "2.0", wantError: true},
		{name: "far higher major rejected", version: "7.3", wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ver := tc.version
			doc := `{"name":"document","initial":0,"hasInitial":true}`
			if ver != "" {
				doc = `{"schemaVersion":"` + ver + `","name":"document","initial":0,"hasInitial":true}`
			}
			_, err := state.LoadFromJSON[DocState, DocEvent, *Document]([]byte(doc))
			if tc.wantError && err == nil {
				t.Fatalf("LoadFromJSON(%q) err = nil, want rejection", ver)
			}
			if !tc.wantError && err != nil {
				t.Fatalf("LoadFromJSON(%q) err = %v, want nil", ver, err)
			}
			if tc.wantError {
				var ue *state.ErrUnsupportedSchema
				if !errors.As(err, &ue) {
					t.Fatalf("err = %v, want *ErrUnsupportedSchema", err)
				}
			}
		})
	}
}

// TestEnvelope_UnknownVariantRejectedOnEvaluation asserts the closed-enum
// extension policy: an unknown guard op that round-trips verbatim (preserved for
// forward-compat) is REJECTED when the machine is bound and built, rather than
// silently dropped or mis-evaluated. Preservation and rejection are the two halves
// of the policy — this locks the rejection half.
func TestEnvelope_UnknownVariantRejectedOnEvaluation(t *testing.T) {
	doc := []byte(`{` +
		`"schemaVersion":"1.0","name":"document","initial":0,"hasInitial":true,` +
		`"states":[{"name":0,"transitions":[{"from":0,"to":1,"on":0,` +
		`"guardExpr":{"op":"futureOp","children":[{"op":"leaf","ref":{"name":"hasReviewer"}}]}}]},` +
		`{"name":1}]}`)

	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](doc)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected Quench to reject the unknown guard variant")
		}
		if !strings.Contains(strings.ToLower(fmtRecover(r)), "guard") {
			t.Fatalf("rejection should name the malformed guard, got: %v", r)
		}
	}()
	_ = ir.Provide(docRegistry()).Quench()
}

// fmtRecover renders a recovered panic value for assertion messages.
func fmtRecover(r any) string {
	if err, ok := r.(error); ok {
		return err.Error()
	}
	if s, ok := r.(string); ok {
		return s
	}
	return ""
}

// TestEnvelope_ToJSON_StampsSchemaVersion asserts a machine serialized via ToJSON
// carries the current schema version, so every emitted document is self-describing
// without the author setting the field by hand.
func TestEnvelope_ToJSON_StampsSchemaVersion(t *testing.T) {
	m := buildDocMachine()
	b, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	if !strings.Contains(string(b), `"schemaVersion":"`+state.CurrentSchemaVersion+`"`) {
		t.Errorf("ToJSON did not stamp schemaVersion:\n%s", b)
	}
}
