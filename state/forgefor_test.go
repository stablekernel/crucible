package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// ForgeForCtx is a tiny value context for the ForgeFor equivalence test.
type forgeForCtx struct {
	Count int
}

// buildToggle builds a two-state toggle on the supplied builder, exercising a
// state, an initial, a transition, and an assign reducer so the resulting Machine
// behavior is non-trivial. It returns the quenched Machine.
func buildToggle(b *state.Builder[string, string, forgeForCtx]) *state.Machine[string, string, forgeForCtx] {
	return b.
		Reducer("bump", func(in state.AssignCtx[forgeForCtx]) forgeForCtx {
			c := in.Entity
			c.Count++
			return c
		}).
		State("off").
		State("on").
		Initial("off").
		Transition("off").On("flip").GoTo("on").
		Assign("bump", nil).
		Transition("on").On("flip").GoTo("off").
		Assign("bump", nil).
		Quench()
}

// TestForgeFor_EquivalentToForge asserts that a machine opened with ForgeFor[C]
// behaves identically to one opened with the fully-spelled Forge[string, string, C]:
// same name, same initial state, same transition outcome, and same context writes.
func TestForgeFor_EquivalentToForge(t *testing.T) {
	t.Parallel()

	viaForge := buildToggle(state.Forge[string, string, forgeForCtx]("toggle"))
	viaForgeFor := buildToggle(state.ForgeFor[forgeForCtx]("toggle"))

	if viaForge.Name() != viaForgeFor.Name() {
		t.Fatalf("name mismatch: Forge=%q ForgeFor=%q", viaForge.Name(), viaForgeFor.Name())
	}

	ctx := context.Background()

	a := viaForge.Cast(forgeForCtx{}, state.WithInitialState[string]("off"))
	b := viaForgeFor.Cast(forgeForCtx{}, state.WithInitialState[string]("off"))

	if a.Current() != b.Current() {
		t.Fatalf("initial state mismatch: Forge=%q ForgeFor=%q", a.Current(), b.Current())
	}

	a.Fire(ctx, "flip")
	b.Fire(ctx, "flip")

	if a.Current() != b.Current() {
		t.Fatalf("post-fire state mismatch: Forge=%q ForgeFor=%q", a.Current(), b.Current())
	}
	if a.Entity().Count != b.Entity().Count {
		t.Fatalf("context write mismatch: Forge=%d ForgeFor=%d", a.Entity().Count, b.Entity().Count)
	}
	if b.Current() != "on" || b.Entity().Count != 1 {
		t.Fatalf("unexpected ForgeFor result: state=%q count=%d", b.Current(), b.Entity().Count)
	}
}
