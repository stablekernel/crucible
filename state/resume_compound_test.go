package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestResumeEffects_DoesNotRestartACompoundServiceThatSettledIntoAChild pins that a host
// that restores a snapshot and absorbs ResumeEffects doesn't start a compound state's
// service again when, before the snapshot, it settled and moved the instance into one of
// the state's children.
func TestResumeEffects_DoesNotRestartACompoundServiceThatSettledIntoAChild(t *testing.T) {
	m := state.ForgeFor[*trec]("resumeCompound").
		Service("authorize", func(context.Context, state.ServiceCtx[*trec]) (any, error) { return nil, nil }).
		SuperState("ordering").Invoke("authorize", state.WithInvokeOnDone("authorized")).
		SubState("pending").
		SubState("paid").
		Initial("pending").
		EndSuperState().
		Initial("ordering").
		Transition("pending").On("authorized").GoTo("paid").
		Quench()
	inst := m.Cast(&trec{}, state.WithInitialState("pending"))
	if res := inst.Fire(context.Background(), "authorized"); res.Err != nil {
		t.Fatalf("Fire(authorized): %v", res.Err)
	}
	snap, err := state.MarshalSnapshot(inst.Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	restoredSnap, err := state.UnmarshalSnapshot[string, string, *trec](snap)
	if err != nil {
		t.Fatalf("UnmarshalSnapshot: %v", err)
	}
	restored, err := m.Restore(restoredSnap)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	for _, eff := range restored.ResumeEffects() {
		if sv, ok := eff.(state.StartService); ok {
			t.Fatalf("ResumeEffects after the service settled into paid starts %s again", sv.ID)
		}
	}
}
