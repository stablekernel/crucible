package state_test

import (
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestRegion_UnknownFields_ByteIdenticalRoundTrip asserts that a Region preserves
// unknown/future JSON keys and its Meta map verbatim across a load -> save cycle,
// so a newer producer's fields survive an older consumer round-tripping the node.
// The input JSON is in canonical (alphabetically sorted) key order so the
// preserve-unknown re-encoding — which sorts keys — is byte-identical to it.
func TestRegion_UnknownFields_ByteIdenticalRoundTrip(t *testing.T) {
	t.Parallel()
	// Keys in sorted order: futureField, initialChild, meta, name, states.
	in := []byte(`{"futureField":{"a":1,"b":["x","y"]},"initialChild":1,` +
		`"meta":{"doc.description":"region note"},"name":"r1",` +
		`"states":[{"name":1},{"name":2}]}`)

	var r state.Region[int, int, *struct{}]
	if err := json.Unmarshal(in, &r); err != nil {
		t.Fatalf("unmarshal Region: %v", err)
	}
	if r.Name != "r1" {
		t.Fatalf("Name = %q, want %q", r.Name, "r1")
	}
	if got, ok := r.Meta["doc.description"]; !ok || got != "region note" {
		t.Fatalf("Meta round-trip lost value: got %v ok=%v", got, ok)
	}

	out, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal Region: %v", err)
	}
	if string(out) != string(in) {
		t.Fatalf("Region round-trip not byte-identical:\n in=%s\nout=%s", in, out)
	}
}

// TestInvocation_UnknownFields_ByteIdenticalRoundTrip asserts that an Invocation
// preserves unknown/future JSON keys and its Meta map verbatim across a load ->
// save cycle, honoring the lossless round-trip its doc claims.
func TestInvocation_UnknownFields_ByteIdenticalRoundTrip(t *testing.T) {
	t.Parallel()
	// Keys in sorted order: futureField, id, input, meta, onDone, onError, src.
	in := []byte(`{"futureField":{"k":42},"id":"inv1",` +
		`"input":{"x":1},"meta":{"doc":"note"},"onDone":7,"onError":8,` +
		`"src":{"name":"svc"}}`)

	var inv state.Invocation[int, int, *struct{}]
	if err := json.Unmarshal(in, &inv); err != nil {
		t.Fatalf("unmarshal Invocation: %v", err)
	}
	if inv.ID != "inv1" || inv.Src.Name != "svc" {
		t.Fatalf("Invocation core fields lost: id=%q src=%q", inv.ID, inv.Src.Name)
	}
	if got, ok := inv.Meta["doc"]; !ok || got != "note" {
		t.Fatalf("Meta round-trip lost value: got %v ok=%v", got, ok)
	}

	out, err := json.Marshal(inv)
	if err != nil {
		t.Fatalf("marshal Invocation: %v", err)
	}
	if string(out) != string(in) {
		t.Fatalf("Invocation round-trip not byte-identical:\n in=%s\nout=%s", in, out)
	}
}

// TestIOSpec_UnknownFields_ByteIdenticalRoundTrip asserts that an IOSpec preserves
// unknown/future JSON keys and its Meta map verbatim across a load -> save cycle —
// the reserved input/output typing slot must not drop a newer producer's fields.
func TestIOSpec_UnknownFields_ByteIdenticalRoundTrip(t *testing.T) {
	t.Parallel()
	// Keys in sorted order: description, futureField, meta, schema.
	in := []byte(`{"description":"the input","futureField":[1,2,3],` +
		`"meta":{"owner":"team-a"},"schema":{"type":"object"}}`)

	var spec state.IOSpec
	if err := json.Unmarshal(in, &spec); err != nil {
		t.Fatalf("unmarshal IOSpec: %v", err)
	}
	if spec.Description != "the input" {
		t.Fatalf("Description lost: %q", spec.Description)
	}
	if got, ok := spec.Meta["owner"]; !ok || got != "team-a" {
		t.Fatalf("Meta round-trip lost value: got %v ok=%v", got, ok)
	}

	out, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal IOSpec: %v", err)
	}
	if string(out) != string(in) {
		t.Fatalf("IOSpec round-trip not byte-identical:\n in=%s\nout=%s", in, out)
	}
}
