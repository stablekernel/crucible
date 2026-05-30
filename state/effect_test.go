package state

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// builtinEffectKinds enumerates every kernel-emitted effect and its stable Kind
// discriminant, so the round-trip and registry tests stay in lockstep with the
// built-in vocabulary: adding a built-in without registering it fails here.
var builtinEffectKinds = []struct {
	kind string
	eff  KindedEffect
}{
	{EffectKindSpawnActor, SpawnActor{ID: "a1", Src: Ref{Name: "worker"}, Input: map[string]any{"n": float64(3)}, State: "Running", SystemID: "sys"}},
	{EffectKindStopActor, StopActor{ID: "a1"}},
	{EffectKindStartService, StartService{ID: "s1", Src: Ref{Name: "fetch"}, Input: map[string]any{"url": "x"}, State: "Loading"}},
	{EffectKindStopService, StopService{ID: "s1"}},
	{EffectKindScheduleAfter, ScheduleAfter{ID: "t1", Delay: 5 * time.Second, State: "Waiting"}},
	{EffectKindCancelScheduled, CancelScheduled{ID: "t1"}},
	{EffectKindSendTo, SendTo{TargetID: "a2", SystemID: "named"}},
	{EffectKindSendParent, SendParent{}},
	{EffectKindRespondToSender, RespondToSender{}},
	{EffectKindForwardEvent, ForwardEvent{TargetID: "a3"}},
}

// TestKindedEffect_Builtins_HaveStableKind asserts every built-in effect reports
// a stable Kind without a Go type assertion, and that the kinds are unique.
func TestKindedEffect_Builtins_HaveStableKind(t *testing.T) {
	seen := map[string]bool{}
	for _, tc := range builtinEffectKinds {
		if got := tc.eff.Kind(); got != tc.kind {
			t.Errorf("%T.Kind() = %q, want %q", tc.eff, got, tc.kind)
		}
		if seen[tc.kind] {
			t.Errorf("duplicate effect kind %q", tc.kind)
		}
		seen[tc.kind] = true
	}
}

// TestEffectEnvelope_RoundTrip_Builtins serializes each built-in effect to an
// envelope, marshals it to JSON, reloads it, and routes it back to a concrete
// effect by kind (never by Go type assertion on the wire form).
func TestEffectEnvelope_RoundTrip_Builtins(t *testing.T) {
	reg := NewEffectRegistry()
	for _, tc := range builtinEffectKinds {
		tc := tc
		t.Run(tc.kind, func(t *testing.T) {
			env, err := MarshalEffect(tc.eff)
			if err != nil {
				t.Fatalf("MarshalEffect(%T): %v", tc.eff, err)
			}
			if env.Kind != tc.kind {
				t.Fatalf("envelope kind = %q, want %q", env.Kind, tc.kind)
			}
			b, err := json.Marshal(env)
			if err != nil {
				t.Fatalf("marshal envelope: %v", err)
			}
			var back EffectEnvelope
			if uerr := json.Unmarshal(b, &back); uerr != nil {
				t.Fatalf("unmarshal envelope: %v", uerr)
			}
			eff, err := reg.Unmarshal(back)
			if err != nil {
				t.Fatalf("registry Unmarshal(%q): %v", tc.kind, err)
			}
			ke, ok := eff.(KindedEffect)
			if !ok {
				t.Fatalf("decoded effect %T does not implement KindedEffect", eff)
			}
			if ke.Kind() != tc.kind {
				t.Fatalf("decoded kind = %q, want %q", ke.Kind(), tc.kind)
			}
		})
	}
}

// customEffect is a host-defined effect registered through the functional-option
// Register seam, proving user effects round-trip identically to built-ins.
type customEffect struct {
	Note  string `json:"note"`
	Count int    `json:"count"`
}

func (customEffect) Kind() string { return "example.custom" }

// TestEffectEnvelope_RoundTrip_CustomRegistered registers a user effect and
// asserts it round-trips through the envelope and back to the concrete type.
func TestEffectEnvelope_RoundTrip_CustomRegistered(t *testing.T) {
	reg := NewEffectRegistry(RegisterEffect("example.custom", func() Effect { return &customEffect{} }))

	orig := customEffect{Note: "hello", Count: 7}
	env, err := MarshalEffect(orig)
	if err != nil {
		t.Fatalf("MarshalEffect: %v", err)
	}
	if env.Kind != "example.custom" {
		t.Fatalf("kind = %q", env.Kind)
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back EffectEnvelope
	if uerr := json.Unmarshal(b, &back); uerr != nil {
		t.Fatalf("unmarshal: %v", uerr)
	}
	eff, err := reg.Unmarshal(back)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	got, ok := eff.(*customEffect)
	if !ok {
		t.Fatalf("decoded %T, want *customEffect", eff)
	}
	if got.Note != orig.Note || got.Count != orig.Count {
		t.Fatalf("decoded = %+v, want %+v", *got, orig)
	}
}

// TestEffectEnvelope_UnknownKind_PreservedOnRoundTrip asserts an unknown effect
// kind survives serialize -> load -> reserialize byte-for-byte (forward-compat),
// rather than being silently dropped.
func TestEffectEnvelope_UnknownKind_PreservedOnRoundTrip(t *testing.T) {
	reg := NewEffectRegistry()
	raw := `{"kind":"vendor.future","payload":{"a":1,"b":"two"},"meta":{"src":"x"}}`

	var env EffectEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	eff, err := reg.Unmarshal(env)
	if err != nil {
		t.Fatalf("Unmarshal of unknown kind must not error: %v", err)
	}
	unk, ok := eff.(UnknownEffect)
	if !ok {
		t.Fatalf("unknown kind decoded to %T, want UnknownEffect", eff)
	}
	if unk.Kind() != "vendor.future" {
		t.Fatalf("preserved kind = %q", unk.Kind())
	}

	// Re-marshal must reproduce the same payload bytes.
	reEnv, err := MarshalEffect(unk)
	if err != nil {
		t.Fatalf("re-marshal unknown: %v", err)
	}
	var got, want map[string]any
	if err := json.Unmarshal(reEnv.Payload, &got); err != nil {
		t.Fatalf("payload not preserved: %v", err)
	}
	_ = json.Unmarshal(env.Payload, &want)
	if got["a"] != want["a"] || got["b"] != want["b"] {
		t.Fatalf("payload changed: got %v want %v", got, want)
	}
}

// TestEffectRegistry_UnknownKind_RejectedOnDispatch asserts the closed-enum
// extension policy: an unknown effect kind is preserved on load (above) but
// rejected with a typed error at dispatch/execution, never silently applied.
func TestEffectRegistry_UnknownKind_RejectedOnDispatch(t *testing.T) {
	reg := NewEffectRegistry()
	env := EffectEnvelope{Kind: "vendor.future", Payload: json.RawMessage(`{}`)}

	eff, err := reg.Unmarshal(env)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if derr := reg.Dispatchable(eff); derr == nil {
		t.Fatal("Dispatchable(unknown) = nil, want typed rejection")
	} else {
		var ue *ErrUnknownEffectKind
		if !errors.As(derr, &ue) {
			t.Fatalf("Dispatchable error = %T, want *ErrUnknownEffectKind", derr)
		}
		if ue.Kind != "vendor.future" {
			t.Fatalf("rejected kind = %q", ue.Kind)
		}
	}

	// A known built-in is dispatchable.
	if derr := reg.Dispatchable(StopActor{ID: "x"}); derr != nil {
		t.Fatalf("Dispatchable(StopActor) = %v, want nil", derr)
	}
}

// TestEffectEnvelope_EffectID_Reserved documents that the envelope reserves an
// effectID slot that is present in the shape but not yet populated/stable: a
// freshly marshaled built-in envelope carries an empty EffectID, and an inbound
// effectID round-trips verbatim without the kernel assigning meaning to it.
func TestEffectEnvelope_EffectID_Reserved(t *testing.T) {
	env, err := MarshalEffect(StopActor{ID: "a1"})
	if err != nil {
		t.Fatalf("MarshalEffect: %v", err)
	}
	if env.EffectID != "" {
		t.Fatalf("EffectID should be unset (reserved, not yet stable), got %q", env.EffectID)
	}

	// An inbound envelope that carries effectID preserves it on round-trip.
	raw := `{"kind":"crucible.stopActor","payload":{"id":"a1"},"effectId":"reserved-123"}`
	var in EffectEnvelope
	if uerr := json.Unmarshal([]byte(raw), &in); uerr != nil {
		t.Fatalf("unmarshal: %v", uerr)
	}
	if in.EffectID != "reserved-123" {
		t.Fatalf("EffectID = %q, want reserved-123", in.EffectID)
	}
	out, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var rt EffectEnvelope
	if err := json.Unmarshal(out, &rt); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if rt.EffectID != "reserved-123" {
		t.Fatalf("EffectID not preserved: %q", rt.EffectID)
	}
}

// TestEffectEnvelope_UnknownFields_Preserved asserts the envelope itself is
// forward-compatible: unknown top-level keys survive a load -> save cycle, the
// same preserve-unknown contract the IR envelope holds.
func TestEffectEnvelope_UnknownFields_Preserved(t *testing.T) {
	raw := `{"kind":"crucible.stopActor","payload":{"id":"a1"},"futureField":{"x":1}}`
	var env EffectEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	out, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back map[string]json.RawMessage
	if err := json.Unmarshal(out, &back); err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	if _, ok := back["futureField"]; !ok {
		t.Fatalf("unknown field futureField dropped on round-trip: %s", out)
	}
}

// TestEffectLabel_KindOverType asserts a journaled trace label uses the stable
// Kind for a KindedEffect and falls back to the Go type for a bare effect.
func TestEffectLabel_KindOverType(t *testing.T) {
	if got := effectLabel(StopActor{ID: "x"}); got != EffectKindStopActor {
		t.Errorf("effectLabel(StopActor) = %q, want %q", got, EffectKindStopActor)
	}
	type bareEffect struct{ N int }
	if got := effectLabel(bareEffect{N: 1}); got == EffectKindStopActor || got == "" {
		t.Errorf("effectLabel(bare) = %q, want a Go type name", got)
	}
	if got := effectLabel(customEffect{}); got != "example.custom" {
		t.Errorf("effectLabel(customEffect) = %q, want example.custom", got)
	}
}

// TestMarshalEffect_NonSerializable surfaces a typed error when an effect's
// payload cannot be marshaled, rather than panicking or emitting a partial
// envelope.
func TestMarshalEffect_NonSerializable(t *testing.T) {
	if _, err := MarshalEffect(unmarshalableEffect{}); err == nil {
		t.Fatal("MarshalEffect of a non-serializable effect = nil error, want failure")
	}
}

// unmarshalableEffect is a KindedEffect whose payload fails json.Marshal (a chan
// field is not serializable), exercising the marshal error path.
type unmarshalableEffect struct {
	Bad chan int `json:"bad"`
}

func (unmarshalableEffect) Kind() string { return "example.bad" }

// TestEffectRegistry_Unmarshal_BadPayload asserts a recognized kind with a
// malformed payload returns a typed unmarshal error rather than a zero value.
func TestEffectRegistry_Unmarshal_BadPayload(t *testing.T) {
	reg := NewEffectRegistry()
	_, err := reg.Unmarshal(EffectEnvelope{Kind: EffectKindScheduleAfter, Payload: json.RawMessage(`{"delay":"not-a-number"}`)})
	if err == nil {
		t.Fatal("Unmarshal with bad payload = nil error, want failure")
	}
}

// TestEffectRegistry_Dispatchable_BareEffect asserts a non-kinded domain effect
// is never gated by the registry — the kernel never owned it.
func TestEffectRegistry_Dispatchable_BareEffect(t *testing.T) {
	reg := NewEffectRegistry()
	type bareEffect struct{ N int }
	if err := reg.Dispatchable(bareEffect{N: 1}); err != nil {
		t.Fatalf("Dispatchable(bare) = %v, want nil", err)
	}
}

// TestRegisterEffect_DuplicateBuiltin documents that registering a kind already
// claimed by a built-in overrides it (last-writer-wins), letting a host swap a
// decoder while keeping the kernel's pre-registration the default.
func TestRegisterEffect_Override(t *testing.T) {
	reg := NewEffectRegistry(RegisterEffect(EffectKindStopActor, func() Effect { return &customEffect{} }))
	eff, err := reg.Unmarshal(EffectEnvelope{Kind: EffectKindStopActor, Payload: json.RawMessage(`{"note":"x","count":1}`)})
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if _, ok := eff.(*customEffect); !ok {
		t.Fatalf("override decoder not used, got %T", eff)
	}
}
