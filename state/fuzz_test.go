package state_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
)

// FuzzLoadFromJSON feeds arbitrary bytes to the IR parser. The parser must never
// panic: malformed input returns an error, well-formed input returns an IR. This
// guards the JSON front-end against crashing on hostile or corrupt definitions.
func FuzzLoadFromJSON(f *testing.F) {
	// Seed with a valid machine IR plus a few degenerate shapes.
	m := buildDocMachine()
	if data, err := m.ToJSON(); err == nil {
		f.Add(data)
	}
	f.Add([]byte(`{"name":"x","initial":0,"hasInitial":true}`))
	f.Add([]byte(`{"name":"x","states":[]}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(``))
	f.Add([]byte(`{"name":`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// A parse either errors or yields a usable IR; it must not panic.
		ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](data)
		if err != nil {
			return
		}
		if ir == nil {
			t.Fatal("LoadFromJSON returned nil IR with nil error")
		}
	})
}

// FuzzRoundTrip fuzzes the serialize -> load -> serialize identity: for any
// machine that survives a round-trip, the reserialized bytes must equal the
// first serialization. The fuzzer mutates the seed JSON; every input that loads
// and rebinds is held to byte-stability.
func FuzzRoundTrip(f *testing.F) {
	m := buildDocMachine()
	seed, err := m.ToJSON()
	if err != nil {
		f.Fatalf("seed ToJSON: %v", err)
	}
	f.Add(seed)

	f.Fuzz(func(t *testing.T, data []byte) {
		ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](data)
		if err != nil {
			return
		}
		built := safeProvide(ir)
		if built == nil {
			return // unbound refs or invalid structure: not a round-trip candidate
		}
		first, err := built.ToJSON()
		if err != nil {
			return
		}
		ir2, err := state.LoadFromJSON[DocState, DocEvent, *Document](first)
		if err != nil {
			t.Fatalf("reload of serialized IR failed: %v", err)
		}
		built2 := safeProvide(ir2)
		if built2 == nil {
			t.Fatal("re-provide of serialized IR failed")
		}
		second, err := built2.ToJSON()
		if err != nil {
			t.Fatalf("reserialize: %v", err)
		}
		if string(first) != string(second) {
			t.Fatalf("round-trip not byte-stable:\n first=%s\nsecond=%s", first, second)
		}
	})
}

// safeProvide binds an IR against the document registry, recovering the Quench
// panic that an unbindable or malformed IR raises so the fuzzer can treat it as
// "not a round-trip candidate" rather than a crash.
func safeProvide(ir *state.IR[DocState, DocEvent, *Document]) (m *state.Machine[DocState, DocEvent, *Document]) {
	defer func() { _ = recover() }()
	m = ir.Provide(docRegistry()).Quench()
	return m
}
