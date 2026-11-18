package state_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// upperSettleTransform uppercases a string output and wraps an error's message, and
// notes the event it was given for each outcome in *events.
func upperSettleTransform(events *[]string) func(string, any, error) (any, error, error) {
	return func(event string, output any, err error) (any, error, error) {
		*events = append(*events, event)
		if err != nil {
			return nil, errors.New("wrapped: " + err.Error()), nil
		}
		return strings.ToUpper(output.(string)), nil, nil
	}
}

// settleParentMachine builds a parent whose "supervising" state invokes a child
// MACHINE actor, routing its completion through Assigns that read the outcome from
// AssignCtx.Event: the done-event payload the settle carries.
func settleParentMachine() *state.Machine[string, string, *trec] {
	return state.ForgeFor[*trec]("settleparent").
		Reducer("recordDone", func(in state.AssignCtx[*trec]) *trec {
			e := in.Entity
			if o, ok := in.Event.(string); ok {
				e.notes = append(e.notes, "result:"+o)
			}
			return e
		}).
		Reducer("recordFail", func(in state.AssignCtx[*trec]) *trec {
			e := in.Entity
			if err, ok := in.Event.(error); ok {
				e.notes = append(e.notes, "error:"+err.Error())
			}
			return e
		}).
		State("idle").
		State("supervising").InvokeActor("child", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childErr")).
		State("complete").
		State("failed").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childDone").GoTo("complete").Assign("recordDone").
		Transition("supervising").On("childErr").GoTo("failed").Assign("recordFail").
		Quench()
}

// settleChildBehavior returns an ActorBehavior whose child completes with the fixed
// output "payload" once delivered "finish".
func settleChildBehavior() state.ActorBehavior {
	child := state.ForgeFor[*trec]("settlechild").
		State("working").
		State("done").Final().
		Initial("working").
		Transition("working").On("finish").GoTo("done").
		Quench()
	return func(map[string]any) (state.ActorInstance, error) {
		inst := child.Cast(&trec{}, state.WithInitialState[string]("working"))
		return state.NewActor(inst, func(*state.Instance[string, string, *trec]) any {
			return "payload"
		}), nil
	}
}

// settleHarness casts a settleParentMachine, wires its actor system with the given
// transform, and enters supervising, so the spawned child is ready to settle.
func settleHarness(t *testing.T, transform func(string, any, error) (any, error, error)) (
	*state.Instance[string, string, *trec], *state.ActorSystem[string, string, *trec], string,
) {
	t.Helper()
	ctx := context.Background()
	m := settleParentMachine()
	parent := m.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sys := state.NewActorSystem(parent).Register("child", settleChildBehavior())
	if transform != nil {
		sys.WithSettleTransform(transform)
	}
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q, want supervising", parent.Current())
	}
	return parent, sys, state.ActorID(m.Name(), "supervising", 0)
}

// TestWithSettleTransform_RewritesTheOutcome proves the transform rewrites the
// output and the error a settle routes, before the parent's Assign sees them, so
// AssignCtx.Event carries the rewritten outcome and LastOutput and LastError report
// it, and that the transform gets onDone's event for an output and onError's for an
// error.
func TestWithSettleTransform_RewritesTheOutcome(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		settle    func(*state.ActorSystem[string, string, *trec], string) (state.FireResult[string], bool)
		wantState string
		wantNotes string
		wantEvent string
		wantLast  string
	}{
		{"a delivery settles the output", func(sys *state.ActorSystem[string, string, *trec], id string) (state.FireResult[string], bool) {
			ref, ok := sys.Ref(id)
			if !ok {
				t.Fatalf("no actor ref for %q", id)
			}
			sys.Deliver(ctx, ref, "finish")
			return state.FireResult[string]{}, true
		}, "complete", "result:PAYLOAD", "childDone", "PAYLOAD"},
		{"SettleError settles the error", func(sys *state.ActorSystem[string, string, *trec], id string) (state.FireResult[string], bool) {
			return sys.SettleError(ctx, id, errors.New("boom"))
		}, "failed", "error:wrapped: boom", "childErr", "wrapped: boom"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var events []string
			parent, sys, id := settleHarness(t, upperSettleTransform(&events))
			if _, ok := tc.settle(sys, id); !ok {
				t.Fatalf("the settle of %q reported false", id)
			}
			if parent.Current() != tc.wantState {
				t.Fatalf("parent state = %q, want %q", parent.Current(), tc.wantState)
			}
			if got := strings.Join(parent.Entity().notes, ","); got != tc.wantNotes {
				t.Fatalf("notes = %q, want %q", got, tc.wantNotes)
			}
			if len(events) != 1 || events[0] != tc.wantEvent {
				t.Fatalf("the transform got events %q, want [%q]", events, tc.wantEvent)
			}
			if tc.wantEvent == "childDone" {
				if out, got := sys.LastOutput(); !got || out != tc.wantLast {
					t.Fatalf("LastOutput = %q, %v; want the rewritten %q", out, got, tc.wantLast)
				}
			}
			if tc.wantEvent == "childErr" {
				if err := sys.LastError(); err == nil || err.Error() != tc.wantLast {
					t.Fatalf("LastError = %v, want the rewritten %q", err, tc.wantLast)
				}
			}
		})
	}
}

// TestWithSettleTransform_AbortFiresNothingAndLeavesTheActorRunning proves an abort
// stops the settle: no event fires, so the parent stays where it was and no Assign
// runs, the actor stays running, and the settle reports the abort as the
// ServiceRunner's does — SettleError returns false with it in FireResult.Err — so a
// later settle of the still-running actor completes it.
func TestWithSettleTransform_AbortFiresNothingAndLeavesTheActorRunning(t *testing.T) {
	ctx := context.Background()
	abort := errors.New("no recording")
	var events []string
	parent, sys, id := settleHarness(t, func(event string, output any, err error) (any, error, error) {
		events = append(events, event)
		if len(events) == 1 {
			return nil, nil, abort
		}
		return upperSettleTransform(&events)(event, output, err)
	})

	// The first settle aborts: SettleError reports the abort and fires nothing.
	res, ok := sys.SettleError(ctx, id, errors.New("boom"))
	if ok || !errors.Is(res.Err, abort) {
		t.Fatalf("SettleError = %+v, %v; want false with the abort in FireResult.Err", res, ok)
	}
	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q after the abort, want supervising: no event fired", parent.Current())
	}
	if n := len(parent.Entity().notes); n != 0 {
		t.Fatalf("the abort ran %d Assigns, want none", n)
	}
	if !sys.IsRunning(id) || sys.Running() != 1 {
		t.Fatalf("the aborted actor isn't running: IsRunning(%v), Running %d", sys.IsRunning(id), sys.Running())
	}
	if out, none := sys.LastOutput(); none {
		t.Fatalf("LastOutput = %q after the abort, want none", out)
	}
	if len(events) != 1 || events[0] != "childErr" {
		t.Fatalf("the transform got events %q, want the one settle's [childErr]", events)
	}

	// The actor the abort left running settles again, and the second settle routes.
	if res, ok = sys.SettleError(ctx, id, errors.New("boom")); !ok || res.Err != nil {
		t.Fatalf("the second SettleError = %+v, %v; want the settled routing", res, ok)
	}
	if parent.Current() != "failed" {
		t.Fatalf("parent state = %q after the second settle, want failed", parent.Current())
	}
	if got := strings.Join(parent.Entity().notes, ","); got != "error:wrapped: boom" {
		t.Fatalf("notes = %q, want the second settle's Assign", got)
	}
}

// TestWithSettleTransform_AnAbortedDeliveryFiresNothing proves a delivery whose
// settle aborts fires no event: the parent stays where it was, no Assign runs, the
// actor stays running and LastOutput still reports nothing, as an aborted service
// settle leaves its service in flight. The settle reported the abort to the
// delivery's Tick, as SettleError does (see
// TestWithSettleTransform_AbortFiresNothingAndLeavesTheActorRunning).
func TestWithSettleTransform_AnAbortedDeliveryFiresNothing(t *testing.T) {
	ctx := context.Background()
	abort := errors.New("no recording")
	parent, sys, id := settleHarness(t, func(string, any, error) (any, error, error) {
		return nil, nil, abort
	})
	if !sys.DeliverByID(ctx, id, "finish") {
		t.Fatal("DeliverByID returned false; the actor was running")
	}
	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q after the aborted delivery, want supervising", parent.Current())
	}
	if n := len(parent.Entity().notes); n != 0 {
		t.Fatalf("the aborted delivery ran %d Assigns, want none", n)
	}
	if !sys.IsRunning(id) || sys.Running() != 1 {
		t.Fatalf("the aborted actor isn't running: IsRunning(%v), Running %d", sys.IsRunning(id), sys.Running())
	}
	if out, none := sys.LastOutput(); none {
		t.Fatalf("LastOutput = %q after the abort, want none", out)
	}
}
