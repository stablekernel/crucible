package state

import "testing"

// TestLifecycleExits_DoesNotAliasCaller verifies that lifecycleExits returns a
// slice backed by its own array rather than the caller's exits slice. The
// function grows the result with orthogonal-region leaves, so aliasing the
// caller's backing array would let an append clobber the caller's data. The
// guard configuration here has more than one active leaf (so the early-return
// fast path is skipped) but no leaf descends from an exited state (so no extra
// leaf is appended), exercising the copy on its own.
func TestLifecycleExits_DoesNotAliasCaller(t *testing.T) {
	t.Parallel()

	inst := &Instance[string, string, struct{}]{
		machine: &Machine[string, string, struct{}]{},
		config:  []string{"a", "b"},
	}

	exits := []string{"a"}
	out := inst.lifecycleExits(exits)

	if len(out) != 1 || out[0] != "a" {
		t.Fatalf("lifecycleExits content = %v, want [a]", out)
	}
	if len(out) > 0 && len(exits) > 0 && &out[0] == &exits[0] {
		t.Fatal("lifecycleExits aliased the caller's slice; want a defensive copy")
	}

	// Growing the result must not be observable through the caller's slice.
	grown := append(out, "x")
	if len(grown) != 2 {
		t.Fatalf("grown result len = %d, want 2", len(grown))
	}
	if len(exits) != 1 || exits[0] != "a" {
		t.Fatalf("caller's exits mutated to %v after appending to result", exits)
	}
}
