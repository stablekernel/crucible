package state_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestErrorMessages exercises every typed error's Error() rendering so the
// human-facing messages are covered and stable.
func TestErrorMessages(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&state.ErrInvalidTransition{From: "Draft", Event: "Submit", Reason: "no match"}, "invalid transition"},
		{&state.ErrInvalidTransition{From: "Draft", To: "Approved", Event: "Approve", Reason: "guards failed"}, "from \"Draft\" to \"Approved\""},
		{&state.ErrGuardFailed{GuardName: "hasReviewer", Reason: "nil"}, "guard \"hasReviewer\" failed"},
		{&state.ErrGuardPanic{GuardName: "g", Recovered: "boom"}, "panicked"},
		{&state.ErrPolicyDenied{PolicyName: "p", Reason: "denied"}, "policy \"p\" denied"},
		{&state.ErrUndeclaredState{State: "Ghost"}, "undeclared state \"Ghost\""},
		{&state.ErrUnboundRef{Kind: "guard", Name: "g"}, "unbound guard ref \"g\""},
		{&state.ErrActionFailed{ActionName: "a", TransitionName: "t", Cause: errors.New("x")}, "action \"a\""},
		{&state.ErrNoPath{From: "A", To: "B"}, "no path from \"A\" to \"B\""},
		{&state.ErrNoInitialState{Machine: "m"}, "no CurrentStateFn"},
		{&state.MultiRegionErr{Errors: []error{errors.New("r1"), errors.New("r2")}}, "2 regions errored"},
		{&state.AssayError{Failures: []state.RequirementFailure{{Name: "req"}}}, "assay failed"},
	}
	for _, c := range cases {
		if got := c.err.Error(); !strings.Contains(got, c.want) {
			t.Errorf("%T.Error() = %q, want substring %q", c.err, got, c.want)
		}
	}
}

// TestOutcomeString asserts every Outcome renders its stable discriminant and an
// unknown value falls back to a parenthesized form, so the logging seam and
// tooling have a locked string for each outcome.
func TestOutcomeString(t *testing.T) {
	cases := []struct {
		o    state.Outcome
		want string
	}{
		{state.OutcomeSuccess, "success"},
		{state.OutcomeInvalidTransition, "invalidTransition"},
		{state.OutcomeGuardFailed, "guardFailed"},
		{state.OutcomeGuardPanic, "guardPanic"},
		{state.OutcomePolicyDenied, "policyDenied"},
		{state.OutcomeEffectError, "effectError"},
		{state.OutcomeAssignFailed, "assignFailed"},
		{state.Outcome(99), "outcome(99)"},
	}
	for _, c := range cases {
		if got := c.o.String(); got != c.want {
			t.Errorf("Outcome(%d).String() = %q, want %q", c.o, got, c.want)
		}
	}
}

// TestErrActionFailed_Unwrap asserts the wrapped cause is reachable via
// errors.As/Unwrap.
func TestErrActionFailed_Unwrap(t *testing.T) {
	cause := errors.New("root")
	err := error(&state.ErrActionFailed{ActionName: "a", Cause: cause})
	if !errors.Is(err, cause) {
		t.Fatal("ErrActionFailed should unwrap to its cause")
	}
}

// TestMultiRegionErr_Unwrap asserts a typed region error is reachable through
// the aggregate via errors.As.
func TestMultiRegionErr_Unwrap(t *testing.T) {
	inner := &state.ErrGuardFailed{GuardName: "g", Reason: "no"}
	agg := error(&state.MultiRegionErr{Errors: []error{inner}})
	var gf *state.ErrGuardFailed
	if !errors.As(agg, &gf) {
		t.Fatal("MultiRegionErr should expose region errors to errors.As")
	}
}

// TestRequirements_ReturnsDeclared covers the Requirements accessor for both a
// state that declares requirements and one that does not.
func TestRequirements_ReturnsDeclared(t *testing.T) {
	m := buildDocMachine()
	if reqs := m.Requirements(Approved); len(reqs) == 0 {
		t.Fatal("Approved should declare a requirement")
	}
	if reqs := m.Requirements(Draft); reqs != nil {
		t.Fatalf("Draft declares no requirements, got %v", reqs)
	}
}

// TestInstance_Entity covers the Entity accessor.
func TestInstance_Entity(t *testing.T) {
	m := buildDocMachine()
	doc := &Document{Status: Draft}
	inst := m.Cast(doc)
	if inst.Entity() != doc {
		t.Fatal("Entity() should return the bound entity")
	}
}

// recordMW is a minimal middleware that flips a flag when invoked, used to cover
// the Use installation path and the wrapping at Fire time.
func TestUse_MiddlewareWraps(t *testing.T) {
	invoked := false
	mw := func(next state.FireFunc[DocState, DocEvent, *Document]) state.FireFunc[DocState, DocEvent, *Document] {
		return func(ctx context.Context, e DocEvent) state.FireResult[DocState] {
			invoked = true
			return next(ctx, e)
		}
	}
	m := state.Forge[DocState, DocEvent, *Document]("doc-mw").
		Action("emit", emitEvent).
		State(Draft).State(Submitted).
		Initial(Draft).
		CurrentStateFn(func(d *Document) DocState { return d.Status }).
		Use(mw).
		Transition(Draft).On(Submit).GoTo(Submitted).
		Quench()

	m.Cast(&Document{Status: Draft}).Fire(context.Background(), Submit)
	if !invoked {
		t.Fatal("installed middleware was not invoked")
	}
}

// TestQuenchError_Message covers the quench error rendering by tripping a lint
// (a transition to an undeclared target) and asserting the panic carries a
// descriptive message.
func TestQuenchError_Message(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected Quench to panic on undeclared target")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("recovered non-error: %v", r)
		}
		if msg := err.Error(); !strings.Contains(msg, "crucible/state") {
			t.Fatalf("quench error message = %q", msg)
		}
	}()
	_ = state.Forge[DocState, DocEvent, *Document]("bad").
		State(Draft).
		Initial(Draft).
		CurrentStateFn(func(d *Document) DocState { return d.Status }).
		Transition(Draft).On(Submit).GoTo(Published). // Published never declared
		Quench()
}

// TestAssay_UndeclaredState covers the Assay early return on an unknown state.
func TestAssay_UndeclaredState(t *testing.T) {
	m := buildDocMachine()
	err := m.Assay(DocState(99), &Document{})
	var us *state.ErrUndeclaredState
	if !errors.As(err, &us) {
		t.Fatalf("err = %v, want *ErrUndeclaredState", err)
	}
}

// pState/pEvent back a self-contained parallel machine whose Running superstate
// has a Shutdown transition that no region handles, so the event bubbles to the
// ancestor — exercising fireFromState and isDescendant.
type pState int

const (
	pOff pState = iota
	pRunning
	pR1a
	pR1b
	pR2a
	pR2b
)

func (s pState) String() string { return [...]string{"Off", "Running", "R1a", "R1b", "R2a", "R2b"}[s] }

type pEvent int

const (
	pStart pEvent = iota
	pAdvance1
	pAdvance2
	pShutdown
)

type pEntity struct{ s pState }

// TestParallel_AncestorHandlesEvent covers the path where an event no region of
// a parallel state handles bubbles up to a transition declared on an ancestor of
// the parallel state — exercising the from-state resolution and descendant check.
func TestParallel_AncestorHandlesEvent(t *testing.T) {
	m := state.Forge[pState, pEvent, *pEntity]("parallel").
		State(pOff).
		Transition(pOff).On(pStart).GoTo(pRunning).
		SuperState(pRunning).
		Region("R1").Initial(pR1a).
		SubState(pR1a).On(pAdvance1).GoTo(pR1b).
		SubState(pR1b).
		EndRegion().
		Region("R2").Initial(pR2a).
		SubState(pR2a).On(pAdvance2).GoTo(pR2b).
		SubState(pR2b).
		EndRegion().
		// Declared on the superstate: no region handles pShutdown.
		Transition(pRunning).On(pShutdown).GoTo(pOff).
		EndSuperState().
		Initial(pOff).
		CurrentStateFn(func(e *pEntity) pState { return e.s }).
		Quench()

	inst := m.Cast(&pEntity{s: pRunning}, state.WithInitialState(pRunning))
	res := inst.Fire(context.Background(), pShutdown)
	if res.Err != nil {
		t.Fatalf("Shutdown from parallel state errored: %v", res.Err)
	}
	if res.NewState != pOff {
		t.Fatalf("Shutdown should leave the parallel state to Off, got %v", res.NewState)
	}
}

// TestCancel_FromHierarchicalSubstate covers the child-first bubble to an
// ancestor transition (Cancel declared on the Running superstate) and the
// exit-cascade path through cascade.go.
func TestCancel_FromHierarchicalSubstate(t *testing.T) {
	m := buildJobMachine()
	job := &Job{Status: Queued}
	inst := m.Cast(job)
	inst.Fire(context.Background(), Enqueue) // -> Starting (inside Running)
	res := inst.Fire(context.Background(), Cancel)
	if res.Err != nil {
		t.Fatalf("Cancel from substate errored: %v", res.Err)
	}
	if res.NewState != Canceled {
		t.Fatalf("Cancel should bubble to Canceled, got %v", res.NewState)
	}
	if len(res.Trace.ExitedStates) == 0 {
		t.Fatal("expected an exit cascade recorded in the trace")
	}
}
