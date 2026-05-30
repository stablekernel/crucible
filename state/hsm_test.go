package state_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// notesFrom extracts the ordered (label, state) cascade notes from a result's
// effects, formatted as "label:State" for compact assertion.
func notesFrom(effects []state.Effect) []string {
	var out []string
	for _, e := range effects {
		if n, ok := e.(cascadeNote); ok {
			out = append(out, n.Label+":"+n.State)
		}
	}
	return out
}

// TestCompound_EntersInitialChild asserts entering a superstate cascades into
// its declared initial child down to the deepest leaf.
func TestCompound_EntersInitialChild(t *testing.T) {
	m := buildJobMachine()
	inst := m.Cast(&Job{Status: Queued})
	res := inst.Fire(context.Background(), Enqueue)
	if res.Err != nil {
		t.Fatalf("Fire(Enqueue) err = %v", res.Err)
	}
	if res.NewState != Starting {
		t.Fatalf("NewState = %v, want Starting (initial child of Running)", res.NewState)
	}
	if got := inst.Current(); got != Starting {
		t.Fatalf("Current() = %v, want Starting", got)
	}
}

// TestCompound_EntryCascadeOrder asserts entry actions run outermost->innermost
// when entering a compound state.
func TestCompound_EntryCascadeOrder(t *testing.T) {
	m := buildJobMachine()
	inst := m.Cast(&Job{Status: Queued})
	res := inst.Fire(context.Background(), Enqueue)
	notes := notesFrom(res.Effects)
	want := []string{"entry:Running", "entry:Starting"}
	if strings.Join(notes, ",") != strings.Join(want, ",") {
		t.Fatalf("entry cascade = %v, want %v", notes, want)
	}
	if last := res.Trace.EnteredStates; len(last) != 2 || last[0] != "Running" || last[1] != "Starting" {
		t.Fatalf("EnteredStates = %v, want [Running Starting]", last)
	}
}

// TestCompound_ChildFirstResolution asserts the deepest active substate's
// transitions are matched first.
func TestCompound_ChildFirstResolution(t *testing.T) {
	m := buildJobMachine()
	inst := m.Cast(&Job{Status: Starting})
	res := inst.Fire(context.Background(), Begin)
	if res.Err != nil {
		t.Fatalf("Fire(Begin) err = %v", res.Err)
	}
	if res.NewState != Executing {
		t.Fatalf("NewState = %v, want Executing", res.NewState)
	}
	if res.Trace.MatchedAt != "Starting" {
		t.Fatalf("MatchedAt = %q, want Starting", res.Trace.MatchedAt)
	}
}

// TestCompound_BubblesToAncestor asserts an event unhandled by the active leaf
// bubbles up to the cross-cutting transition on the superstate, and MatchedAt
// names the ancestor.
func TestCompound_BubblesToAncestor(t *testing.T) {
	m := buildJobMachine()
	inst := m.Cast(&Job{Status: Executing})
	res := inst.Fire(context.Background(), Cancel)
	if res.Err != nil {
		t.Fatalf("Fire(Cancel) err = %v", res.Err)
	}
	if res.NewState != Canceled {
		t.Fatalf("NewState = %v, want Canceled", res.NewState)
	}
	if res.Trace.MatchedAt != "Running" {
		t.Fatalf("MatchedAt = %q, want Running (cross-cutting)", res.Trace.MatchedAt)
	}
}

// TestCompound_ExitCascadeOrder asserts exit actions run innermost->outermost
// when leaving a compound state via an ancestor transition.
func TestCompound_ExitCascadeOrder(t *testing.T) {
	m := buildJobMachine()
	inst := m.Cast(&Job{Status: Executing})
	res := inst.Fire(context.Background(), Cancel)
	notes := notesFrom(res.Effects)
	// Exit innermost (Executing) then outermost (Running); Canceled is a leaf
	// with no entry action.
	want := []string{"exit:Executing", "exit:Running"}
	if strings.Join(notes, ",") != strings.Join(want, ",") {
		t.Fatalf("exit cascade = %v, want %v", notes, want)
	}
	if ex := res.Trace.ExitedStates; len(ex) != 2 || ex[0] != "Executing" || ex[1] != "Running" {
		t.Fatalf("ExitedStates = %v, want [Executing Running]", ex)
	}
}

// TestCompound_FinalEmitsDone asserts entering a final state synthesizes a
// done event toward its parent. Here Finish drives Executing->Done (Done is a
// top-level final state), so the macrostep completes the machine.
func TestCompound_FinalEmitsDone(t *testing.T) {
	m := buildJobMachine()
	inst := m.Cast(&Job{Status: Executing})
	res := inst.Fire(context.Background(), Finish)
	if res.Err != nil {
		t.Fatalf("Fire(Finish) err = %v", res.Err)
	}
	if res.NewState != JobDone {
		t.Fatalf("NewState = %v, want JobDone", res.NewState)
	}
}

// TestFinal_RejectsOutgoing asserts a transition out of a final state is
// rejected at runtime for a machine that reaches a final leaf.
func TestFinal_RejectsOutgoing(t *testing.T) {
	m := buildJobMachine()
	inst := m.Cast(&Job{Status: JobDone})
	res := inst.Fire(context.Background(), Cancel)
	var it *state.ErrInvalidTransition
	if !errors.As(res.Err, &it) {
		t.Fatalf("err = %v, want *ErrInvalidTransition", res.Err)
	}
}

// TestParallel_EntersAllRegionInitials asserts entering a parallel superstate
// activates every region's initial state, observable via Configuration.
func TestParallel_EntersAllRegionInitials(t *testing.T) {
	m := buildWorkerMachine()
	inst := m.Cast(&Worker{State: Offline})
	res := inst.Fire(context.Background(), Activate)
	if res.Err != nil {
		t.Fatalf("Fire(Activate) err = %v", res.Err)
	}
	cfg := inst.Configuration()
	if len(cfg) != 2 {
		t.Fatalf("Configuration len = %d, want 2", len(cfg))
	}
	if cfg[0] != Idle || cfg[1] != Silent {
		t.Fatalf("Configuration = %v, want [Idle Silent]", cfg)
	}
	if inst.Current() != Idle {
		t.Fatalf("Current() = %v, want Idle (primary leaf)", inst.Current())
	}
}

// TestParallel_BroadcastsToRegions asserts an event routes to every region; the
// region that handles it advances while the other holds.
func TestParallel_BroadcastsToRegions(t *testing.T) {
	m := buildWorkerMachine()
	inst := m.Cast(&Worker{State: Offline})
	inst.Fire(context.Background(), Activate)

	res := inst.Fire(context.Background(), StartWork)
	if res.Err != nil {
		t.Fatalf("Fire(StartWork) err = %v", res.Err)
	}
	cfg := inst.Configuration()
	if cfg[0] != Busy || cfg[1] != Silent {
		t.Fatalf("Configuration = %v, want [Busy Silent]", cfg)
	}

	// Telemetry region handles its own event independently.
	res = inst.Fire(context.Background(), EnableReporting)
	if res.Err != nil {
		t.Fatalf("Fire(EnableReporting) err = %v", res.Err)
	}
	cfg = inst.Configuration()
	if cfg[0] != Busy || cfg[1] != Reporting {
		t.Fatalf("Configuration = %v, want [Busy Reporting]", cfg)
	}
}

// TestParallel_OnDoneWhenAllRegionsFinal asserts the superstate's OnDone runs
// only once every region has reached a final state.
func TestParallel_OnDoneWhenAllRegionsFinal(t *testing.T) {
	workerDoneFired = false
	m := buildWorkerMachine()
	inst := m.Cast(&Worker{State: Offline})
	inst.Fire(context.Background(), Activate)

	// Drive Execution to its final state; not all regions final yet.
	inst.Fire(context.Background(), StartWork)
	inst.Fire(context.Background(), FinishExecution)
	if workerDoneFired {
		t.Fatal("OnDone fired before all regions reached final")
	}

	// Drive Telemetry to its final state; now all regions are final.
	inst.Fire(context.Background(), EnableReporting)
	res := inst.Fire(context.Background(), FinishTelemetry)
	if res.Err != nil {
		t.Fatalf("Fire(FinishTelemetry) err = %v", res.Err)
	}
	if !workerDoneFired {
		t.Fatal("OnDone did not fire after all regions reached final")
	}
}

// TestHSM_RoundTrip asserts a hierarchical+parallel machine survives
// ToJSON -> LoadFromJSON -> Provide -> Quench losslessly and behaves identically.
func TestHSM_RoundTrip(t *testing.T) {
	m := buildWorkerMachine()
	b1, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}

	// The serialized form must carry the hierarchy, not a flattened graph.
	if !strings.Contains(string(b1), "\"regions\"") {
		t.Fatalf("JSON missing nested regions:\n%s", b1)
	}

	ir, err := state.LoadFromJSON[WorkerState, WorkerEvent, *Worker](b1)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	reg := state.NewRegistry[*Worker]().Action("activeDone", recordWorkerDone)
	m2 := ir.Provide(reg).Quench()

	b2, err := m2.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON (2): %v", err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("HSM round-trip not byte-identical:\n %s\n %s", b1, b2)
	}

	// Behavioral identity: the rehydrated machine enters both region initials.
	inst := m2.Cast(&Worker{State: Offline}, state.WithInitialState(Offline))
	inst.Fire(context.Background(), Activate)
	cfg := inst.Configuration()
	if len(cfg) != 2 || cfg[0] != Idle || cfg[1] != Silent {
		t.Fatalf("rehydrated Configuration = %v, want [Idle Silent]", cfg)
	}
}

// TestHSM_JobRoundTrip asserts the hierarchical job machine round-trips losslessly.
func TestHSM_JobRoundTrip(t *testing.T) {
	m := buildJobMachine()
	b1, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	if !strings.Contains(string(b1), "\"children\"") {
		t.Fatalf("JSON missing nested children:\n%s", b1)
	}
	ir, err := state.LoadFromJSON[JobStatus, JobEvent, *Job](b1)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	reg := state.NewRegistry[*Job]().
		Action("entry", recordAction("entry")).
		Action("exit", recordAction("exit")).
		Action("done", recordAction("done"))
	m2 := ir.Provide(reg).Quench()
	b2, err := m2.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON (2): %v", err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("job round-trip not byte-identical:\n %s\n %s", b1, b2)
	}
}

// TestParallel_MultiRegionErr asserts that when two regions both match an event
// but fail their guards, the result is a MultiRegionErr unwrapping each region's
// typed error.
func TestParallel_MultiRegionErr(t *testing.T) {
	type s = int
	type e = int
	const (
		off s = iota
		par
		aIdle
		aBusy
		bIdle
		bBusy
	)
	const (
		on e = iota
		step
	)

	m := state.Forge[s, e, any]("multi").
		Guard("never", func(state.GuardCtx[any]) bool { return false }).
		State(off).
		Transition(off).On(on).GoTo(par).
		SuperState(par).
		Region("A").
		Initial(aIdle).
		SubState(aIdle).On(step).GoTo(aBusy).When("never").
		SubState(aBusy).
		EndRegion().
		Region("B").
		Initial(bIdle).
		SubState(bIdle).On(step).GoTo(bBusy).When("never").
		SubState(bBusy).
		EndRegion().
		EndSuperState().
		Initial(off).
		CurrentStateFn(func(any) s { return off }).
		Quench()

	inst := m.Cast(nil)
	inst.Fire(context.Background(), on)
	res := inst.Fire(context.Background(), step)
	var multi *state.MultiRegionErr
	if !errors.As(res.Err, &multi) {
		t.Fatalf("err = %v, want *MultiRegionErr", res.Err)
	}
	if len(multi.Errors) != 2 {
		t.Fatalf("MultiRegionErr.Errors = %d, want 2", len(multi.Errors))
	}
	var gf *state.ErrGuardFailed
	if !errors.As(res.Err, &gf) {
		t.Fatalf("MultiRegionErr does not unwrap to *ErrGuardFailed: %v", res.Err)
	}
}

// TestFlat_ConfigurationIsSingleLeaf asserts a flat machine reports a
// single-element configuration equal to Current.
func TestFlat_ConfigurationIsSingleLeaf(t *testing.T) {
	m := buildJobMachine()
	inst := m.Cast(&Job{Status: Queued})
	cfg := inst.Configuration()
	if len(cfg) != 1 || cfg[0] != Queued {
		t.Fatalf("Configuration = %v, want [Queued]", cfg)
	}
}

// TestQuench_SubStateOutsideSuperState asserts the lint catches a SubState call
// outside a SuperState block.
func TestQuench_SubStateOutsideSuperState(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on SubState outside SuperState")
		}
	}()
	_ = state.Forge[JobStatus, JobEvent, *Job]("bad").
		State(Queued).
		SubState(Starting). // outside any SuperState
		Initial(Queued).
		CurrentStateFn(func(j *Job) JobStatus { return j.Status }).
		Quench()
}

// TestQuench_UnclosedSuperState asserts the lint catches an unclosed SuperState.
func TestQuench_UnclosedSuperState(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on unclosed SuperState")
		}
	}()
	_ = state.Forge[JobStatus, JobEvent, *Job]("bad").
		State(Queued).
		SuperState(Running).
		Initial(Starting).
		SubState(Starting).
		// no EndSuperState
		Initial(Queued).
		CurrentStateFn(func(j *Job) JobStatus { return j.Status }).
		Quench()
}

// TestQuench_SuperStateNoInitial asserts a superstate with substates but no
// Initial panics at Quench.
func TestQuench_SuperStateNoInitial(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on superstate without Initial")
		}
	}()
	_ = state.Forge[JobStatus, JobEvent, *Job]("bad").
		State(Queued).
		Transition(Queued).On(Enqueue).GoTo(Running).
		SuperState(Running).
		SubState(Starting).
		SubState(Executing).
		EndSuperState().
		Initial(Queued).
		CurrentStateFn(func(j *Job) JobStatus { return j.Status }).
		Quench()
}
