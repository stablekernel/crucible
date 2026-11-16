package state_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// TestResumeEffects_DoesNotRearmASuperstatesTimer proves ResumeEffects re-arms an
// active leaf's `after` timer but not a compound state's: here the compound's timer
// already fired into one of its children, so re-arming it after Restore would fire it
// a second time.
func TestResumeEffects_DoesNotRearmASuperstatesTimer(t *testing.T) {
	m := state.ForgeFor[*trec]("nestTimer").
		SuperState("outer").
		SubState("a").
		SubState("b").
		Initial("a").
		EndSuperState().
		State("done").
		Initial("outer").
		Transition("outer").After(time.Second).On("elapsed").GoTo("b").
		Transition("b").After(time.Minute).On("stale").GoTo("done").
		Quench()
	inst := m.Cast(&trec{}, state.WithInitialState("a"))
	if res := inst.Fire(context.Background(), "elapsed"); res.Err != nil {
		t.Fatalf("Fire(elapsed): %v", res.Err)
	}
	if got := inst.Current(); got != "b" {
		t.Fatalf("state = %q, want b", got)
	}
	restored, err := m.Restore(inst.Snapshot())
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	var timers []any
	for _, eff := range restored.ResumeEffects() {
		if e, ok := eff.(state.ScheduleAfter); ok {
			timers = append(timers, e.Event)
		}
	}
	if len(timers) != 1 || timers[0] != "stale" {
		t.Fatalf("ResumeEffects timers = %v, want only b's [stale]", timers)
	}
}
