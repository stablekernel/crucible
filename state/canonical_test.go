package state_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
)

// snapRegistry binds the host behavior the representative machines reference, so
// a rehydrated IR re-Quenches without unbound refs. It mirrors what the DSL
// registers for the flat machine ("bump"); the hierarchical and parallel
// machines carry no refs and are unaffected.
func snapRegistry() *state.Registry[*snapCtx] {
	return state.NewRegistry[*snapCtx]().
		Action("bump", func(c state.ActionCtx[*snapCtx]) (state.Effect, error) {
			c.Entity.Count++
			c.Entity.Notes = append(c.Entity.Notes, "bumped")
			return nil, nil
		})
}

// TestCanonicalForm_WithoutSrcPosIsReDecodeIdempotent asserts the canonical-form
// invariant that evolution.DiffMachines relies on: ToJSON(WithoutSrcPos) is
// stable across a full re-decode. Serializing a machine, loading it back,
// re-providing host behavior, re-Quenching, and serializing again must yield
// byte-identical canonical bytes. A divergence would make DiffMachines report a
// phantom change between a machine and its own round-trip, so the property is a
// correctness invariant of the diff/evolution pipeline.
//
// This is a deterministic property test over representative machine shapes
// (flat-with-action, hierarchical, parallel) rather than a go test -fuzz
// harness: the generic Provide/Quench API needs concrete S/E/C types and a
// matching registry, which a byte-level fuzzer cannot synthesize for arbitrary
// machines. The byte-level fuzzing of the JSON front-end lives in FuzzRoundTrip;
// this test pins the WithoutSrcPos canonical form specifically.
func TestCanonicalForm_WithoutSrcPosIsReDecodeIdempotent(t *testing.T) {
	reg := snapRegistry()
	tests := []struct {
		name    string
		machine func() *state.Machine[string, string, *snapCtx]
	}{
		{name: "flat with action", machine: flatSnapMachine},
		{name: "hierarchical", machine: hsmSnapMachine},
		{name: "parallel", machine: parallelSnapMachine},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := tt.machine()

			first, err := m.ToJSON(state.WithoutSrcPos())
			if err != nil {
				t.Fatalf("first ToJSON err = %v", err)
			}
			if len(first) == 0 {
				t.Fatal("first ToJSON produced empty bytes")
			}

			ir, err := state.LoadFromJSON[string, string, *snapCtx](first)
			if err != nil {
				t.Fatalf("LoadFromJSON err = %v", err)
			}
			m2 := ir.Provide(reg).Quench()
			if m2 == nil {
				t.Fatal("Provide().Quench() returned nil")
			}

			second, err := m2.ToJSON(state.WithoutSrcPos())
			if err != nil {
				t.Fatalf("second ToJSON err = %v", err)
			}

			if string(first) != string(second) {
				t.Fatalf("canonical form not re-decode idempotent:\n first=%s\nsecond=%s", first, second)
			}

			// A second full re-decode must reproduce the same bytes again: the
			// canonical form is a true fixed point, not merely stable for one hop.
			ir2, err := state.LoadFromJSON[string, string, *snapCtx](second)
			if err != nil {
				t.Fatalf("second LoadFromJSON err = %v", err)
			}
			m3 := ir2.Provide(reg).Quench()
			third, err := m3.ToJSON(state.WithoutSrcPos())
			if err != nil {
				t.Fatalf("third ToJSON err = %v", err)
			}
			if string(second) != string(third) {
				t.Fatalf("canonical form not a fixed point:\nsecond=%s\n third=%s", second, third)
			}
		})
	}
}
