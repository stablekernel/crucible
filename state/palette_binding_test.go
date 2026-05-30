package state

import (
	"encoding/json"
	"testing"
)

// TestBindingSpec_Default_InProcess asserts the reserved binding spec defaults to
// the in-process transport when none is set, both via the helper and on the wire.
func TestBindingSpec_Default_InProcess(t *testing.T) {
	var zero BindingSpec
	if got := zero.transport(); got != TransportInProcess {
		t.Fatalf("zero.transport() = %q, want %q", got, TransportInProcess)
	}
	explicit := BindingSpec{Transport: TransportInProcess}
	if explicit.transport() != TransportInProcess {
		t.Fatalf("explicit in-process transport mismatch")
	}
}

// TestDescriptorBinding_RoundTrip_AbsentDefaultsInProcess asserts a Descriptor
// with no Binding round-trips with the field absent (omitempty) and that a
// decoded absent binding is read as the in-process default.
func TestDescriptorBinding_RoundTrip_AbsentDefaultsInProcess(t *testing.T) {
	d := Descriptor{Kind: KindGuard, Name: "g"}

	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if containsKey(t, raw, "binding") {
		t.Fatalf("absent Binding must be omitted, got %s", raw)
	}

	var back Descriptor
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Binding != nil {
		t.Fatalf("Binding = %+v, want nil", back.Binding)
	}
	// The documented default for an absent binding is in-process.
	if BindingTransportOf(back) != TransportInProcess {
		t.Fatalf("default transport = %q, want %q", BindingTransportOf(back), TransportInProcess)
	}
}

// TestDescriptorBinding_RoundTrip_Explicit asserts an explicit binding spec
// survives a marshal/unmarshal cycle, including its Meta namespace.
func TestDescriptorBinding_RoundTrip_Explicit(t *testing.T) {
	d := Descriptor{
		Kind:    KindGuard,
		Name:    "g",
		Binding: &BindingSpec{Transport: TransportInProcess, Meta: map[string]any{"note": "x"}},
	}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back Descriptor
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Binding == nil || back.Binding.Transport != TransportInProcess {
		t.Fatalf("Binding = %+v, want in-process", back.Binding)
	}
	if back.Binding.Meta["note"] != "x" {
		t.Fatalf("Binding.Meta lost: %+v", back.Binding.Meta)
	}
}

// TestBindingTransportOf_ExplicitTransport reads back a non-default transport off
// a descriptor's reserved binding (the future out-of-process case), confirming the
// reader returns the declared transport rather than the in-process default.
func TestBindingTransportOf_ExplicitTransport(t *testing.T) {
	d := Descriptor{Kind: KindGuard, Name: "g", Binding: &BindingSpec{Transport: "wasm"}}
	if got := BindingTransportOf(d); got != "wasm" {
		t.Fatalf("BindingTransportOf = %q, want wasm", got)
	}
	// An explicit-but-empty transport still falls back to in-process.
	d2 := Descriptor{Kind: KindGuard, Name: "g", Binding: &BindingSpec{}}
	if got := BindingTransportOf(d2); got != TransportInProcess {
		t.Fatalf("empty transport = %q, want %q", got, TransportInProcess)
	}
}

// TestBindingSpec_UnknownTransport_Preserved asserts the closed-enum extension
// policy: an unknown transport (a future out-of-process binding a newer producer
// emitted) round-trips verbatim rather than being dropped or rejected at decode.
func TestBindingSpec_UnknownTransport_Preserved(t *testing.T) {
	src := []byte(`{"transport":"wasm","epoch":42}`)
	var spec BindingSpec
	if err := json.Unmarshal(src, &spec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if spec.Transport != "wasm" {
		t.Fatalf("Transport = %q, want wasm", spec.Transport)
	}
	out, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !containsKey(t, out, "epoch") {
		t.Fatalf("unknown key epoch dropped: %s", out)
	}
}

// containsKey reports whether the JSON object has the given top-level key.
func containsKey(t *testing.T, raw []byte, key string) bool {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("containsKey unmarshal: %v", err)
	}
	_, ok := m[key]
	return ok
}
