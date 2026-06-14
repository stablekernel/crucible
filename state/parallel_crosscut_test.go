package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// crosscutMachine builds a parallel state "par" with two single-leaf regions. The
// regions handle "tick" internally; the parallel state declares a cross-cutting
// transition on "abort" to "done" (exiting all regions). A guard on the abort edge
// is supplied so the guarded-cross-cutting branch is exercised. "done" is final.
func crosscutMachine(allowAbort bool) *state.Machine[string, string, *trec] {
	return state.ForgeFor[*trec]("crosscut").
		Guard("canAbort", func(state.GuardCtx[*trec]) bool { return allowAbort }).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		// The cross-cutting transition lives on the parallel superstate: no region
		// consumes "abort", so it bubbles to fireFromState.
		Transition("par").On("abort").GoTo("done").When("canAbort").
		Region("a").
		Initial("aIdle").
		SubState("aIdle").
		EndRegion().
		Region("b").
		Initial("bIdle").
		SubState("bIdle").
		EndRegion().
		EndSuperState().
		State("done").Final().
		Initial("off").
		CurrentStateFn(func(*trec) string { return "off" }).
		Quench()
}

// TestFireFromState_CrossCuttingTransitionCommits proves an event no region
// handles bubbles from the parallel state to a guarded cross-cutting transition
// that commits, exiting the parallel configuration.
func TestFireFromState_CrossCuttingTransitionCommits(t *testing.T) {
	m := crosscutMachine(true)
	inst := m.Cast(&trec{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "abort")
	if res.Err != nil {
		t.Fatalf("cross-cutting abort errored: %v", res.Err)
	}
	if res.NewState != "done" {
		t.Fatalf("state = %v, want done", res.NewState)
	}
	if res.Trace.Outcome != state.OutcomeSuccess {
		t.Fatalf("outcome = %v, want success", res.Trace.Outcome)
	}
}

// TestFireFromState_CrossCuttingGuardFails proves a cross-cutting transition whose
// guard fails leaves the configuration in the parallel state and reports an
// invalid transition (no other candidate handled the event).
func TestFireFromState_CrossCuttingGuardFails(t *testing.T) {
	m := crosscutMachine(false)
	inst := m.Cast(&trec{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "abort")
	var ite *state.InvalidTransitionError
	if !errors.As(res.Err, &ite) {
		t.Fatalf("err = %v, want *InvalidTransitionError (guard blocked the only candidate)", res.Err)
	}
	// The parallel configuration is unchanged.
	cfg := inst.Configuration()
	if len(cfg) != 2 {
		t.Fatalf("configuration = %v, want both regions still active", cfg)
	}
}

// TestFireFromState_CrossCuttingGuardPanics proves a panicking guard on a
// cross-cutting transition surfaces a *GuardPanicError classified
// OutcomeGuardPanic, exercising the guard-panic branch of fireFromState.
func TestFireFromState_CrossCuttingGuardPanics(t *testing.T) {
	m := state.ForgeFor[*trec]("crosscut-panic").
		Guard("boom", func(state.GuardCtx[*trec]) bool { panic("guard blew up") }).
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Transition("par").On("abort").GoTo("done").When("boom").
		Region("a").Initial("aIdle").SubState("aIdle").EndRegion().
		Region("b").Initial("bIdle").SubState("bIdle").EndRegion().
		EndSuperState().
		State("done").Final().
		Initial("off").
		CurrentStateFn(func(*trec) string { return "off" }).
		Quench()

	inst := m.Cast(&trec{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "abort")
	var gpe *state.GuardPanicError
	if !errors.As(res.Err, &gpe) {
		t.Fatalf("err = %v, want *GuardPanicError", res.Err)
	}
	if res.Trace.Outcome != state.OutcomeGuardPanic {
		t.Fatalf("outcome = %v, want OutcomeGuardPanic", res.Trace.Outcome)
	}
}

// TestFireFromState_ForbiddenEventIsConsumed proves an event forbidden on the
// parallel state is consumed (success, no state change) rather than bubbling,
// exercising the forbids branch of fireFromState.
func TestFireFromState_ForbiddenEventIsConsumed(t *testing.T) {
	m := state.ForgeFor[*trec]("crosscut-forbid").
		State("off").
		Transition("off").On("go").GoTo("par").
		SuperState("par").
		Forbid("abort").
		Region("a").Initial("aIdle").SubState("aIdle").EndRegion().
		Region("b").Initial("bIdle").SubState("bIdle").EndRegion().
		EndSuperState().
		State("done").Final().
		Initial("off").
		CurrentStateFn(func(*trec) string { return "off" }).
		Quench()

	inst := m.Cast(&trec{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "abort")
	if res.Err != nil {
		t.Fatalf("forbidden event should be consumed cleanly, got err %v", res.Err)
	}
	if res.Trace.Outcome != state.OutcomeSuccess {
		t.Fatalf("outcome = %v, want success (forbidden, consumed)", res.Trace.Outcome)
	}
	if len(inst.Configuration()) != 2 {
		t.Fatalf("configuration = %v, want both regions still active", inst.Configuration())
	}
}

// TestFireFromState_UnhandledEventIsInvalid proves an event neither a region nor a
// cross-cutting transition handles surfaces an InvalidTransitionError naming the
// event, exercising the no-candidate branch of fireFromState.
func TestFireFromState_UnhandledEventIsInvalid(t *testing.T) {
	m := crosscutMachine(true)
	inst := m.Cast(&trec{}, state.WithInitialState("off"))
	ctx := context.Background()
	if res := inst.Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("entering parallel: %v", res.Err)
	}

	res := inst.Fire(ctx, "nonsense")
	var ite *state.InvalidTransitionError
	if !errors.As(res.Err, &ite) {
		t.Fatalf("err = %v, want *InvalidTransitionError", res.Err)
	}
	if ite.Event != "nonsense" {
		t.Fatalf("invalid transition event = %q, want nonsense", ite.Event)
	}
}
