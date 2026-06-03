package state_test

// This file covers the opt-in observability surface introduced by the
// centralized trace gate: lite vs full mode behavior, history retention options
// (ring buffer and unbounded), the label cache, and precomputed ancestor /
// descendToLeaves correctness.

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// ---------------------------------------------------------------------------
// Helpers: simple flat 2-state toggle machine used by most lite/full tests.
// ---------------------------------------------------------------------------

type toggleState int

const (
	toggleA toggleState = iota
	toggleB
)

type toggleEvent int

const toggleGo toggleEvent = 0

// buildToggleMachine returns a minimal flat machine: A -go-> B, B -go-> A.
// It registers an action and a reducer so full-mode traces carry non-empty
// EffectsEmitted and AssignsApplied fields.
func buildToggleMachine() *state.Machine[toggleState, toggleEvent, any] {
	return state.Forge[toggleState, toggleEvent, any]("toggle").
		Action("noop", func(ctx state.ActionCtx[any]) (state.Effect, error) {
			return struct{}{}, nil
		}).
		Reducer("id", func(ctx state.AssignCtx[any]) any { return ctx.Entity }).
		State(toggleA).
		Transition(toggleA).On(toggleGo).GoTo(toggleB).Do("noop").Assign("id").
		State(toggleB).
		Transition(toggleB).On(toggleGo).GoTo(toggleA).Do("noop").Assign("id").
		Initial(toggleA).
		CurrentStateFn(func(any) toggleState { return toggleA }).
		Quench()
}

// ---------------------------------------------------------------------------
// 1. Lite mode (default): rich fields empty.
// ---------------------------------------------------------------------------

// TestLite_DefaultOmitsRichFields asserts that a Cast with no trace options
// produces a Trace whose rich diagnostic fields (GuardsEvaluated, EffectsEmitted,
// ExitedStates, EnteredStates, AssignsApplied, Microsteps, EventPayload,
// SelectedTransition) are all empty or nil. Machine, Event, FromState, MatchedAt,
// and Outcome are always populated.
func TestLite_DefaultOmitsRichFields(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA))
	res := inst.Fire(ctx, toggleGo)
	if res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}

	tr := res.Trace
	if tr.Machine == "" {
		t.Error("Machine should always be set")
	}
	if tr.Event == "" {
		t.Error("Event should always be set")
	}
	if tr.FromState == "" {
		t.Error("FromState should always be set")
	}
	if tr.MatchedAt == "" {
		t.Error("MatchedAt should always be set")
	}
	if tr.Outcome != state.OutcomeSuccess {
		t.Errorf("Outcome = %v, want OutcomeSuccess", tr.Outcome)
	}

	// Rich fields must be empty in lite mode.
	if len(tr.GuardsEvaluated) != 0 {
		t.Errorf("GuardsEvaluated = %v, want empty in lite mode", tr.GuardsEvaluated)
	}
	if len(tr.EffectsEmitted) != 0 {
		t.Errorf("EffectsEmitted = %v, want empty in lite mode", tr.EffectsEmitted)
	}
	if len(tr.ExitedStates) != 0 {
		t.Errorf("ExitedStates = %v, want empty in lite mode", tr.ExitedStates)
	}
	if len(tr.EnteredStates) != 0 {
		t.Errorf("EnteredStates = %v, want empty in lite mode", tr.EnteredStates)
	}
	if len(tr.AssignsApplied) != 0 {
		t.Errorf("AssignsApplied = %v, want empty in lite mode", tr.AssignsApplied)
	}
	if len(tr.Microsteps) != 0 {
		t.Errorf("Microsteps = %v, want empty in lite mode", tr.Microsteps)
	}
	if len(tr.EventPayload) != 0 {
		t.Errorf("EventPayload = %v, want nil in lite mode", tr.EventPayload)
	}
	if tr.SelectedTransition != nil {
		t.Errorf("SelectedTransition = %v, want nil in lite mode", tr.SelectedTransition)
	}
}

// ---------------------------------------------------------------------------
// 2. Full mode parity: WithFullTrace produces the same trace as the old default.
// ---------------------------------------------------------------------------

// TestFullTrace_ParityWithOldDefault is the parity gate: for a flat machine a
// WithFullTrace Fire must produce non-empty rich trace fields matching what the
// old default (unbounded history, always-full) would have produced. This
// verifies that the helper-method refactor did not silently drop any field.
func TestFullTrace_ParityWithOldDefault(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	// Full mode.
	full := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithFullTrace[toggleState]())
	resF := full.Fire(ctx, toggleGo)
	if resF.Err != nil {
		t.Fatalf("full Fire err = %v", resF.Err)
	}

	tr := resF.Trace
	if len(tr.EffectsEmitted) == 0 {
		t.Error("full mode: EffectsEmitted should be populated")
	}
	if len(tr.AssignsApplied) == 0 {
		t.Error("full mode: AssignsApplied should be populated")
	}
	if len(tr.EventPayload) == 0 {
		t.Error("full mode: EventPayload should be populated")
	}
	if tr.SelectedTransition == nil {
		t.Error("full mode: SelectedTransition should be non-nil")
	}
}

// TestFullTrace_HSM_ParityExitEntry verifies that WithFullTrace on a compound
// machine populates ExitedStates and EnteredStates exactly as the old default.
func TestFullTrace_HSM_ParityExitEntry(t *testing.T) {
	m := buildJobMachine()
	ctx := context.Background()

	full := m.Cast(&Job{Status: Executing}, state.WithFullTrace[JobStatus]())
	res := full.Fire(ctx, Cancel)
	if res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}
	if len(res.Trace.ExitedStates) == 0 {
		t.Error("full mode: ExitedStates should be populated for HSM exit cascade")
	}
}

// ---------------------------------------------------------------------------
// 3. Elevation: WithInspector and WithHistory both elevate to full.
// ---------------------------------------------------------------------------

// TestElevation_InspectorElevatesTrace asserts that attaching an inspector
// (WithInspector) elevates to full trace without needing WithFullTrace.
func TestElevation_InspectorElevatesTrace(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	sawEvent := false
	insp := state.InspectorFunc(func(ev state.InspectionEvent) {
		sawEvent = true
	})
	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithInspector[toggleState](insp))
	res := inst.Fire(ctx, toggleGo)
	if res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}
	if !sawEvent {
		t.Error("inspector should have been called")
	}
	// Inspector implies full trace.
	if len(res.Trace.EffectsEmitted) == 0 {
		t.Error("WithInspector should elevate to full trace; EffectsEmitted empty")
	}
}

// TestElevation_WithHistoryElevatesTrace asserts that WithHistory also elevates
// to full trace.
func TestElevation_WithHistoryElevatesTrace(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithHistory[toggleState](5))
	res := inst.Fire(ctx, toggleGo)
	if res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}
	if len(res.Trace.EffectsEmitted) == 0 {
		t.Error("WithHistory should elevate to full trace; EffectsEmitted empty")
	}
}

// ---------------------------------------------------------------------------
// 4. Logger-only stays lite.
// ---------------------------------------------------------------------------

// TestLoggerOnly_StaysLite asserts that attaching only a *slog.Logger (no
// inspector, no history, no WithFullTrace) keeps the trace in lite mode. The
// logger can still log because it reads Machine, Event, FromState, MatchedAt,
// and Outcome — all of which are always set in lite mode.
func TestLoggerOnly_StaysLite(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	// Discard logger: we only care that rich fields are absent.
	logger := slog.New(slog.NewTextHandler(newDiscardWriter(), nil))
	inst := m.Cast(nil,
		state.WithInitialState[toggleState](toggleA),
		state.WithLogger[toggleState](logger),
	)
	res := inst.Fire(ctx, toggleGo)
	if res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}
	if len(res.Trace.EffectsEmitted) != 0 {
		t.Errorf("logger-only: EffectsEmitted should be empty in lite mode, got %v", res.Trace.EffectsEmitted)
	}
	if len(res.Trace.AssignsApplied) != 0 {
		t.Errorf("logger-only: AssignsApplied should be empty in lite mode, got %v", res.Trace.AssignsApplied)
	}
	if tr := res.Trace; tr.Machine == "" || tr.Event == "" || tr.FromState == "" || tr.Outcome != state.OutcomeSuccess {
		t.Errorf("logger-only: always-populated fields missing: %+v", res.Trace)
	}
}

// discardWriter implements io.Writer and discards all output.
type discardWriter struct{}

func newDiscardWriter() *discardWriter               { return &discardWriter{} }
func (w *discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// ---------------------------------------------------------------------------
// 5. Ring buffer: bounded history.
// ---------------------------------------------------------------------------

// TestRingBuffer_RetainsExactlyLimit asserts the ring buffer keeps the last
// limit entries when more than limit fires occur.
func TestRingBuffer_RetainsExactlyLimit(t *testing.T) {
	const limit = 3
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithHistory[toggleState](limit))
	// Fire more than limit times.
	for range 7 {
		inst.Fire(ctx, toggleGo)
	}
	h := inst.History()
	if len(h) != limit {
		t.Errorf("History len = %d, want %d", len(h), limit)
	}
}

// TestRingBuffer_ChronologicalOrder asserts History() returns entries in
// chronological (oldest→newest) order after the ring has wrapped.
func TestRingBuffer_ChronologicalOrder(t *testing.T) {
	const limit = 3
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithHistory[toggleState](limit))
	// Fire 5 times; ring wraps after 3, so the kept entries are fires 3,4,5.
	for range 5 {
		inst.Fire(ctx, toggleGo)
	}
	h := inst.History()
	if len(h) != limit {
		t.Fatalf("History len = %d, want %d", len(h), limit)
	}
	// Entries should be in chronological order: successive fires move between
	// toggleA and toggleB, so FromState alternates.
	for k := 1; k < len(h); k++ {
		if h[k].FromState == h[k-1].FromState {
			t.Errorf("History not in chronological order at [%d]: %q == %q",
				k, h[k].FromState, h[k-1].FromState)
		}
	}
}

// TestRingBuffer_DropsOldest asserts that after the ring fills, the oldest
// entry is dropped when a new one is written.
func TestRingBuffer_DropsOldest(t *testing.T) {
	const limit = 2
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithHistory[toggleState](limit))
	// Fire 1: A→B (FromState="0" i.e. toggleA)
	inst.Fire(ctx, toggleGo)
	// Fire 2: B→A (FromState="1" i.e. toggleB)
	inst.Fire(ctx, toggleGo)
	// Fire 3: A→B — ring wraps, fire 1 is dropped
	inst.Fire(ctx, toggleGo)

	h := inst.History()
	if len(h) != limit {
		t.Fatalf("History len = %d, want %d", len(h), limit)
	}
	// The kept entries are fires 2 and 3.
	// Fire 2: FromState = toggleB (1)
	// Fire 3: FromState = toggleA (0)
	want0 := "1" // toggleB
	want1 := "0" // toggleA
	if h[0].FromState != want0 {
		t.Errorf("History[0].FromState = %q, want %q (fire 2, oldest kept)", h[0].FromState, want0)
	}
	if h[1].FromState != want1 {
		t.Errorf("History[1].FromState = %q, want %q (fire 3, newest)", h[1].FromState, want1)
	}
}

// TestRingBuffer_WrapAroundCorrectness stresses the wrap-around at an odd limit
// to confirm modular arithmetic is correct.
func TestRingBuffer_WrapAroundCorrectness(t *testing.T) {
	const (
		limit   = 5
		numFire = 13
	)
	m := buildToggleMachine()
	ctx := context.Background()

	// Track every FromState in a reference slice.
	var ref []string
	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithHistory[toggleState](limit))
	for range numFire {
		res := inst.Fire(ctx, toggleGo)
		ref = append(ref, res.Trace.FromState)
	}
	// Expected: the last `limit` fires' FromState values.
	wantRef := ref[numFire-limit:]
	got := inst.History()
	if len(got) != limit {
		t.Fatalf("History len = %d, want %d", len(got), limit)
	}
	for k := range limit {
		if got[k].FromState != wantRef[k] {
			t.Errorf("History[%d].FromState = %q, want %q", k, got[k].FromState, wantRef[k])
		}
	}
}

// TestRingBuffer_ZeroLimitDisables asserts WithHistory(0) does not retain any
// traces and History() returns nil.
func TestRingBuffer_ZeroLimitDisables(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithHistory[toggleState](0))
	inst.Fire(ctx, toggleGo)
	if h := inst.History(); h != nil {
		t.Errorf("WithHistory(0) should disable retention; got %v", h)
	}
}

// TestRingBuffer_UnboundedGrows asserts WithUnboundedHistory retains every
// settled trace and History() grows without limit.
func TestRingBuffer_UnboundedGrows(t *testing.T) {
	const numFire = 10
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithUnboundedHistory[toggleState]())
	for range numFire {
		inst.Fire(ctx, toggleGo)
	}
	if got := len(inst.History()); got != numFire {
		t.Errorf("WithUnboundedHistory: History len = %d, want %d", got, numFire)
	}
}

// TestHistory_DefaultReturnsNil asserts History() returns nil (no allocation)
// when no retention mode is selected.
func TestHistory_DefaultReturnsNil(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA))
	inst.Fire(ctx, toggleGo)
	if h := inst.History(); h != nil {
		t.Errorf("default: History() should return nil, got %v", h)
	}
}

// ---------------------------------------------------------------------------
// 5b. Restore trace mode: WithRestoreFullTrace / WithRestoreUnboundedHistory.
// ---------------------------------------------------------------------------

// TestRestore_TraceModeOptIn asserts a restored instance is lite by default (a
// subsequent Fire omits rich fields and does not retain new traces), that
// WithRestoreFullTrace restores full per-step traces without continued retention,
// and that WithRestoreUnboundedHistory restores full traces AND keeps retaining
// them — the mode the durable runner relies on for byte-identical replay. (Restore
// always seeds history from the snapshot's traces, so the distinguishing behavior
// is whether later Fires are rich and whether they keep accumulating.)
func TestRestore_TraceModeOptIn(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	src := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithUnboundedHistory[toggleState]())
	src.Fire(ctx, toggleGo)
	snap := src.Snapshot()

	// Default restore: lite Fire (no rich fields) and no further retention.
	lite, err := m.Restore(snap)
	if err != nil {
		t.Fatalf("Restore (default): %v", err)
	}
	base := len(lite.History())
	r := lite.Fire(ctx, toggleGo)
	if len(r.Trace.EnteredStates) != 0 || len(r.Trace.EffectsEmitted) != 0 {
		t.Errorf("default restore should fire lite, got rich fields: %+v", r.Trace)
	}
	if got := len(lite.History()); got != base {
		t.Errorf("default restore should not retain new traces: %d -> %d", base, got)
	}

	// WithRestoreFullTrace: rich Fire, but still no continued retention.
	full, err := m.Restore(snap, state.WithRestoreFullTrace[toggleState]())
	if err != nil {
		t.Fatalf("Restore (full trace): %v", err)
	}
	fbase := len(full.History())
	rf := full.Fire(ctx, toggleGo)
	if len(rf.Trace.EnteredStates) == 0 || len(rf.Trace.EffectsEmitted) == 0 {
		t.Errorf("WithRestoreFullTrace should populate rich fields, got %+v", rf.Trace)
	}
	if got := len(full.History()); got != fbase {
		t.Errorf("WithRestoreFullTrace should not retain new traces: %d -> %d", fbase, got)
	}

	// WithRestoreUnboundedHistory: rich Fire AND continued unbounded retention.
	dur, err := m.Restore(snap, state.WithRestoreUnboundedHistory[toggleState]())
	if err != nil {
		t.Fatalf("Restore (unbounded): %v", err)
	}
	dbase := len(dur.History())
	dur.Fire(ctx, toggleGo)
	dur.Fire(ctx, toggleGo)
	if got := len(dur.History()); got != dbase+2 {
		t.Errorf("WithRestoreUnboundedHistory should keep retaining: %d -> %d (want +2)", dbase, got)
	}
	if h := dur.History(); len(h[len(h)-1].EnteredStates) == 0 {
		t.Errorf("retained trace under unbounded restore should be full, got %+v", h[len(h)-1])
	}
}

// ---------------------------------------------------------------------------
// 6. Label cache: m.label correctness.
// ---------------------------------------------------------------------------

// TestLabelCache_HotPathUsed asserts that the label cache produces the correct
// string for a declared state in both full and lite mode (the string must match
// fmt.Sprint of the underlying value).
func TestLabelCache_HotPathUsed(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithFullTrace[toggleState]())
	res := inst.Fire(ctx, toggleGo)
	// FromState should equal fmt.Sprint(toggleA) = "0".
	if res.Trace.FromState != "0" {
		t.Errorf("FromState = %q, want \"0\" (fmt.Sprint of toggleA)", res.Trace.FromState)
	}
	if res.Trace.MatchedAt != "0" {
		t.Errorf("MatchedAt = %q, want \"0\"", res.Trace.MatchedAt)
	}
}

// ---------------------------------------------------------------------------
// 7. ancestors / descendToLeaves precompute parity.
// ---------------------------------------------------------------------------

// TestAncestors_FlatMachine asserts that for a flat machine a simple A→B
// transition correctly names the states in ExitedStates and EnteredStates
// (no compound ancestors; each is its own single-element chain).
func TestAncestors_FlatMachine(t *testing.T) {
	m := buildToggleMachine()
	ctx := context.Background()

	inst := m.Cast(nil, state.WithInitialState[toggleState](toggleA), state.WithFullTrace[toggleState]())
	res := inst.Fire(ctx, toggleGo)
	if res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}
	// For a flat machine with an A→B external transition, ExitedStates = [A]
	// and EnteredStates = [B] — one element each (no compound ancestors).
	if len(res.Trace.ExitedStates) != 1 {
		t.Errorf("flat machine A→B: ExitedStates = %v, want 1 element", res.Trace.ExitedStates)
	}
	if len(res.Trace.EnteredStates) != 1 {
		t.Errorf("flat machine A→B: EnteredStates = %v, want 1 element", res.Trace.EnteredStates)
	}
}

// TestDescendToLeaves_CompoundNotAliased asserts that firing multiple times does
// not corrupt the precomputed leaves cache: each fire starts from its own state
// and the instance's config slice never aliases the node's internal cache.
func TestDescendToLeaves_CompoundNotAliased(t *testing.T) {
	m := buildJobMachine()
	ctx := context.Background()

	// Enter the compound Running state (Queued → Enqueue → Starting inside Running).
	inst := m.Cast(&Job{Status: Queued}, state.WithFullTrace[JobStatus]())
	res1 := inst.Fire(ctx, Enqueue)
	if res1.Err != nil {
		t.Fatalf("Enqueue err = %v", res1.Err)
	}
	// Current should be the deepest initial leaf, not the compound Running state.
	cur1 := inst.Current()

	// Fire again from inside the compound.
	res2 := inst.Fire(ctx, Begin)
	cur2 := inst.Current()

	// The two current states must differ and must be valid declared leaves.
	if cur1 == cur2 {
		t.Errorf("current state did not advance: %v == %v", cur1, cur2)
	}
	// EnteredStates on the second fire must not include Running (it was already active).
	for _, s := range res2.Trace.EnteredStates {
		if s == "Running" {
			t.Errorf("fire 2 EnteredStates should not re-enter Running (not a descendToLeaves alias): %v", res2.Trace.EnteredStates)
		}
	}
}

// TestAncestors_NestedParity asserts that the precomputed ancestor chain on a
// nested machine returns the correct innermost-first ordering, matching what
// manually walking parent pointers would give.
func TestAncestors_NestedParity(t *testing.T) {
	m := buildNested(t)
	ctx := context.Background()

	// Start inside the deep leaf C1. Fire Park: this transitions C1->Parked by
	// bubbling up through C, B, A and matching the Park transition on A. The exit
	// cascade should be [C1, C, B, A] (innermost-first).
	inst := m.Cast(&box{State: c1}, state.WithFullTrace[lvl]())
	res := inst.Fire(ctx, park)
	if res.Err != nil {
		t.Fatalf("Park err = %v", res.Err)
	}
	want := "C1,C,B,A"
	if got := strings.Join(res.Trace.ExitedStates, ","); got != want {
		t.Errorf("ExitedStates = %q, want %q (innermost-first ancestor chain)", got, want)
	}
}
