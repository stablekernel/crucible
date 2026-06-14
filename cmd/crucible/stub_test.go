package main

import (
	"os"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestStubRegistry_EnumeratesEveryBehavior loads the clean fixture (which
// references two guards, two actions, one reducer, and one service) and confirms
// Provide(stubRegistry).Quench succeeds, proving every referenced name was
// stubbed. An unstubbed name would panic Quench with an *UnboundRefError.
func TestStubRegistry_EnumeratesEveryBehavior(t *testing.T) {
	b, err := os.ReadFile("testdata/clean.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, any](b)
	if err != nil {
		t.Fatalf("load IR: %v", err)
	}

	m, err := quench(ir)
	if err != nil {
		t.Fatalf("quench with stubs: %v", err)
	}
	if m == nil {
		t.Fatal("quench returned a nil machine")
	}
}
