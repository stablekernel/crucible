package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestFireSeq_FullTraceMergesPerStepDiagnostics drives a guarded, effect-emitting
// machine through FireSeq in full-trace mode and asserts the merged Trace
// accumulates each step's guards and effects, exercising the full-mode merge
// branch of mergeTrace (which is a no-op in lite mode).
func TestFireSeq_FullTraceMergesPerStepDiagnostics(t *testing.T) {
	m := state.Forge[string, string, *trec]("seq").
		Guard("ok", func(state.GuardCtx[*trec]) bool { return true }).
		Action("note", noteAction("do")).
		State("a").
		Transition("a").On("go").GoTo("b").When("ok").Do("note", state.P{"t": "first"}).
		State("b").
		Transition("b").On("go").GoTo("c").When("ok").Do("note", state.P{"t": "second"}).
		State("c").Final().
		Initial("a").
		Quench()

	inst := m.Cast(&trec{}, state.WithInitialState("a"), state.WithFullTrace[string]())
	br := inst.FireSeq(context.Background(), []string{"go", "go"})

	if br.Err != nil {
		t.Fatalf("FireSeq errored: %v", br.Err)
	}
	if len(br.Steps) != 2 {
		t.Fatalf("steps = %d, want 2", len(br.Steps))
	}
	if br.Trace.Outcome != state.OutcomeSuccess {
		t.Fatalf("merged outcome = %v, want success", br.Trace.Outcome)
	}
	// Both steps evaluated the "ok" guard, so the merged trace carries both.
	if len(br.Trace.GuardsEvaluated) != 2 {
		t.Fatalf("merged GuardsEvaluated = %v, want two entries", br.Trace.GuardsEvaluated)
	}
	// Both steps emitted the "note" effect, merged in order.
	if len(br.Trace.EffectsEmitted) != 2 {
		t.Fatalf("merged EffectsEmitted = %v, want two entries", br.Trace.EffectsEmitted)
	}
}

// TestFireSeq_LiteTraceDoesNotMerge confirms that in the default lite mode the
// merged trace stays empty of the rich per-step fields: mergeTrace short-circuits.
func TestFireSeq_LiteTraceDoesNotMerge(t *testing.T) {
	m := state.Forge[string, string, *trec]("seq-lite").
		Guard("ok", func(state.GuardCtx[*trec]) bool { return true }).
		Action("note", noteAction("do")).
		State("a").
		Transition("a").On("go").GoTo("b").When("ok").Do("note", state.P{"t": "first"}).
		State("b").Final().
		Initial("a").
		Quench()

	inst := m.Cast(&trec{}, state.WithInitialState("a")) // lite by default
	br := inst.FireSeq(context.Background(), []string{"go"})
	if br.Err != nil {
		t.Fatalf("FireSeq errored: %v", br.Err)
	}
	if len(br.Trace.GuardsEvaluated) != 0 || len(br.Trace.EffectsEmitted) != 0 {
		t.Fatalf("lite merged trace should carry no rich fields, got guards=%v effects=%v",
			br.Trace.GuardsEvaluated, br.Trace.EffectsEmitted)
	}
}
