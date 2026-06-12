package state_test

import (
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/state"
)

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
