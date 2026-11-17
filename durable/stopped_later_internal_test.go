package durable

import (
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestStoppedLater_SkipsOnlyAnActivationThatEndsInTheStep pins which spawns
// stoppedLater reports: a state's spawn a later effect stops, in its own call's effects
// or a later call's, and not the spawn for the activation that follows that stop, nor a
// dynamic spawn, nor another actor's.
func TestStoppedLater_SkipsOnlyAnActivationThatEndsInTheStep(t *testing.T) {
	x := state.SpawnActor{ID: "x", State: "S"}
	stopX := state.StopActor{ID: "x"}
	dynamic := state.SpawnActor{ID: "x"}
	for _, tc := range []struct {
		name        string
		sp          state.SpawnActor
		rest, later []state.Effect
		want        bool
	}{
		{"stopped later in the call", x, []state.Effect{stopX}, nil, true},
		{"stopped in a later call", x, nil, []state.Effect{stopX}, true},
		{"the activation after the stop", x, nil, nil, false},
		{"another actor stopped", x, []state.Effect{state.StopActor{ID: "y"}}, nil, false},
		{"a dynamic spawn", dynamic, []state.Effect{stopX}, nil, false},
	} {
		if got := stoppedLater(tc.sp, tc.rest, tc.later); got != tc.want {
			t.Errorf("%s: stoppedLater = %v, want %v", tc.name, got, tc.want)
		}
	}
}
