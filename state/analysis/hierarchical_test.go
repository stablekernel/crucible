package analysis_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// TestHierarchical_CleanMachine analyzes a well-formed hierarchical machine: a
// Running superstate over Starting/Executing, reaching a final Done state. No
// defect should be reported — reachability must descend into the superstate's
// initial child, and liveness must credit the superstate via its descendants.
func TestHierarchical_Clean(t *testing.T) {
	m := forge("job-hsm").
		Action("noop", func(state.ActionCtx[any]) (state.Effect, error) { return nil, nil }).
		State("queued").
		Transition("queued").On("begin").GoTo("running").
		SuperState("running").
		Initial("starting").
		SubState("starting").
		On("step").GoTo("executing").
		SubState("executing").
		On("finish").GoTo("done").
		EndSuperState().
		State("done").Final().
		Initial("queued").
		Quench()

	r := analysis.Analyze(m)
	if !r.Empty() {
		t.Fatalf("clean hierarchical machine should yield no findings; got:\n%s", r)
	}
}

// TestHierarchical_NestedDeadEnd reports a nested substate that has no outgoing
// transition and no ancestor exit, so an instance entering it is stuck.
func TestHierarchical_NestedDefects(t *testing.T) {
	// "trap" is a substate of "running" with no outgoing edge and no exit on its
	// parent — a genuine nested dead end. "running" itself has the cross-cutting
	// "abort" exit so its other child "starting" is not a dead end.
	m := forge("job-defective").
		State("queued").
		Transition("queued").On("begin").GoTo("running").
		SuperState("running").
		Initial("starting").
		SubState("starting").
		On("trap").GoTo("trap").
		SubState("trap").
		EndSuperState().
		State("done").Final().
		Transition("queued").On("done").GoTo("done").
		Initial("queued").
		Quench()

	r := analysis.Analyze(m)
	if !states(r, analysis.KindDeadEnd)["trap"] {
		t.Fatalf("expected nested 'trap' reported as a dead end; report:\n%s", r)
	}
	// "starting" has an outgoing transition, so it is not a dead end.
	if states(r, analysis.KindDeadEnd)["starting"] {
		t.Fatalf("'starting' has an outgoing transition and must not be a dead end")
	}
	// The compound "running" must never be a dead end (it is left via children).
	if states(r, analysis.KindDeadEnd)["running"] {
		t.Fatalf("compound 'running' must not be reported as a dead end")
	}
}

// TestHierarchical_AncestorExitClearsDeadEnd confirms a leaf with no edge of its
// own is not flagged when its superstate carries a cross-cutting exit.
func TestHierarchical_AncestorExitClearsDeadEnd(t *testing.T) {
	m := forge("job-ancestor-exit").
		State("queued").
		Transition("queued").On("begin").GoTo("running").
		SuperState("running").
		Initial("idle").
		SubState("idle").
		// idle has no transition of its own.
		Transition("running").On("abort").GoTo("done"). // ancestor exit
		EndSuperState().
		State("done").Final().
		Initial("queued").
		Quench()

	r := analysis.Analyze(m)
	if states(r, analysis.KindDeadEnd)["idle"] {
		t.Fatalf("'idle' is exited via its superstate's 'abort'; must not be a dead end; report:\n%s", r)
	}
}

// TestParallel_Clean analyzes a well-formed parallel (orthogonal) machine: an
// "active" superstate with two regions, each reaching its own final substate.
// The region states must flatten into the graph, be reachable via their region
// initial children, and each region's final substate must satisfy liveness.
func TestParallel_Clean(t *testing.T) {
	m := forge("worker-parallel").
		State("offline").
		Transition("offline").On("activate").GoTo("active").
		SuperState("active").
		Region("Exec").
		Initial("idle").
		SubState("idle").
		On("work").GoTo("busy").
		SubState("busy").
		On("finishExec").GoTo("execDone").
		SubState("execDone").Final().
		EndRegion().
		Region("Tele").
		Initial("silent").
		SubState("silent").
		On("report").GoTo("reporting").
		SubState("reporting").
		On("finishTele").GoTo("teleDone").
		SubState("teleDone").Final().
		EndRegion().
		EndSuperState().
		Initial("offline").
		Quench()

	r := analysis.Analyze(m)
	// Region states must be reachable (no unreachable findings).
	if got := r.OfKind(analysis.KindUnreachableState); len(got) != 0 {
		t.Fatalf("parallel region states should all be reachable; got:\n%s", r)
	}
	// Region final substates satisfy liveness; nothing should be cannot-reach-final.
	if got := r.OfKind(analysis.KindCannotReachFinal); len(got) != 0 {
		t.Fatalf("each region reaches its final substate; got:\n%s", r)
	}
}
