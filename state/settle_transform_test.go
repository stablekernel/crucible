package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// AssignCtx.Event: the done-event payload the settle carries. "supervising" returns
// to "idle" on "restart", so a transform (or a host) can leave the state and
// re-enter it, stopping the child and spawning a fresh one under the same id, as a
// supervisor restart does.
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
		Transition("supervising").On("restart").GoTo("idle").
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

// TestWithSettleTransform_ARespawnedActorIsNotSettledByTheOldSettle proves a settle
// whose transform ran for one actor can't settle, or report an abort for, the actor
// respawned under the same id while the transform ran. The transform stops the
// settling actor and spawns a fresh one under its id, as a supervisor restart does,
// with no real concurrency: the old settle must fire nothing and report nothing —
// not even its abort — the new actor must stay running and not done, and no onDone
// may fire, so the new actor settles only with a delivery of its own.
func TestWithSettleTransform_ARespawnedActorIsNotSettledByTheOldSettle(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		aborting bool // the transform aborts the first settle rather than passing its outcome through
	}{{"the settle branch", false}, {"the abort branch", true}} {
		t.Run(tc.name, func(t *testing.T) {
			abort := errors.New("no recording")
			var calls int
			var events []string
			var parent *state.Instance[string, string, *trec]
			var sys *state.ActorSystem[string, string, *trec]
			parent, sys, id := settleHarness(t, func(event string, output any, err error) (any, error, error) {
				calls++
				if calls > 1 {
					return upperSettleTransform(&events)(event, output, err)
				}
				// While the transform runs, the supervisor restarts the actor: leaving
				// supervising stops the settling actor, and re-entering it spawns a fresh
				// one under the same id, the invocation's stable id for its position.
				sys.Absorb(ctx, parent.Fire(ctx, "restart").Effects)
				sys.Absorb(ctx, parent.Fire(ctx, "start").Effects)
				if tc.aborting {
					return nil, nil, abort
				}
				return output, err, nil
			})

			// Drive the settle whose transform respawns the actor under it.
			if tc.aborting {
				res, ok := sys.SettleError(ctx, id, errors.New("boom"))
				if ok || res.Err != nil {
					t.Fatalf("the stale SettleError = %+v, %v; want false with no Err: its abort came from the old actor", res, ok)
				}
			} else if !sys.DeliverByID(ctx, id, "finish") {
				t.Fatal("DeliverByID returned false; the actor was running")
			}

			// The old settle fired nothing: no onDone, no Assign, no abort, and the
			// parent is where the restart left it.
			if parent.Current() != "supervising" {
				t.Fatalf("parent state = %q, want supervising: the old settle fired nothing", parent.Current())
			}
			if n := len(parent.Entity().notes); n != 0 {
				t.Fatalf("the old settle ran %d Assigns, want none", n)
			}
			if out, none := sys.LastOutput(); none {
				t.Fatalf("LastOutput = %q, want none: no outcome settled", out)
			}
			// The respawned actor under the id is running and not done.
			if !sys.IsRunning(id) || sys.Running() != 1 {
				t.Fatalf("the respawned actor isn't running: IsRunning(%v), Running %d", sys.IsRunning(id), sys.Running())
			}

			// The new actor settles with a delivery of its own, carrying its own
			// outcome — the old settle's outcome never reaches the parent.
			if !sys.DeliverByID(ctx, id, "finish") {
				t.Fatal("DeliverByID returned false for the respawned actor")
			}
			if parent.Current() != "complete" {
				t.Fatalf("parent state = %q after the new actor's settle, want complete", parent.Current())
			}
			if got := strings.Join(parent.Entity().notes, ","); got != "result:PAYLOAD" {
				t.Fatalf("notes = %q, want the new actor's own settle's result:PAYLOAD", got)
			}
		})
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

// unroutedParentMachine spawns its child through the spawn action with neither an
// onDone nor an onError — a dynamic spawn routes no completion, where an invoked
// actor always routes, firing the zero event its unset outcome leaves, which the
// parent refuses — so the child's settle routes nothing and its completion is a
// fire-and-forget no-op, exactly as with no transform.
func unroutedParentMachine() *state.Machine[string, string, *trec] {
	return state.ForgeFor[*trec]("unroutedparent").
		State("idle").
		State("supervising").
		Initial("idle").
		Transition("idle").On("start").GoTo("supervising").Spawn("child", unroutedChildID).
		Quench()
}

// unroutedChildID is the explicit id of the dynamically spawned, routing-less child
// the unrouted-settle tests settle.
const unroutedChildID = "unrouted-child"

// finishingChild returns an ActorBehavior whose child completes with the fixed output
// once delivered "finish", whatever the output encodes as.
func finishingChild(output any) state.ActorBehavior {
	child := state.ForgeFor[*trec]("finchild").
		State("working").
		State("done").Final().
		Initial("working").
		Transition("working").On("finish").GoTo("done").
		Quench()
	return func(map[string]any) (state.ActorInstance, error) {
		inst := child.Cast(&trec{}, state.WithInitialState[string]("working"))
		return state.NewActor(inst, func(*state.Instance[string, string, *trec]) any {
			return output
		}), nil
	}
}

// unroutedHarness casts unroutedParentMachine, wiring its actor system with the given
// transform and a child that completes with output, so the routing-less child is
// ready to settle.
func unroutedHarness(t *testing.T, transform func(string, any, error) (any, error, error), output any) (
	*state.Instance[string, string, *trec], *state.ActorSystem[string, string, *trec], string,
) {
	t.Helper()
	ctx := context.Background()
	m := unroutedParentMachine()
	parent := m.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sys := state.NewActorSystem(parent).Register("child", finishingChild(output))
	if transform != nil {
		sys.WithSettleTransform(transform)
	}
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q, want supervising", parent.Current())
	}
	return parent, sys, unroutedChildID
}

// TestWithSettleTransform_AnUnroutedSettleRunsNoTransform proves the transform runs
// only when the settle routes a completion. The child was spawned with neither an
// onDone nor an onError, so its settle routes nothing and behaves exactly as with no
// transform set: the transform is never consulted — not for an output that won't
// encode, whose transform would abort as the durable Runner's does; not for the zero
// event an onDone-less settle would hand it, which a decoder that refuses that event
// would abort on; and not for a failure, which escalates to the parent as it does
// without a transform — nothing fires, and LastOutput and LastError report the raw
// outcome.
func TestWithSettleTransform_AnUnroutedSettleRunsNoTransform(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		output    any  // the child's completion output, when it completes
		fail      bool // settle the child as a failure through SettleError
		transform func(*int) func(string, any, error) (any, error, error)
	}{
		{"an output that won't encode", make(chan int), false, func(calls *int) func(string, any, error) (any, error, error) {
			return func(event string, output any, err error) (any, error, error) {
				*calls++
				if _, jerr := json.Marshal(output); jerr != nil {
					return nil, nil, fmt.Errorf("won't encode: %w", jerr)
				}
				return output, err, nil
			}
		}},
		{"the zero event", "payload", false, func(calls *int) func(string, any, error) (any, error, error) {
			return func(event string, output any, err error) (any, error, error) {
				*calls++
				if event == "" {
					return nil, nil, errors.New("refuses the zero event")
				}
				return output, err, nil
			}
		}},
		{"a failure with no onError", nil, true, func(calls *int) func(string, any, error) (any, error, error) {
			return func(string, any, error) (any, error, error) {
				*calls++
				return nil, nil, errors.New("no recording")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			parent, sys, id := unroutedHarness(t, tc.transform(&calls), tc.output)

			// Drive the unrouted settle: a failure through SettleError, a success
			// through a delivery.
			if tc.fail {
				res, ok := sys.SettleError(ctx, id, errors.New("boom"))
				if ok || res.Err != nil {
					t.Fatalf("SettleError = %+v, %v; want false with no Err: nothing routed and nothing aborted", res, ok)
				}
			} else if !sys.DeliverByID(ctx, id, "finish") {
				t.Fatal("DeliverByID returned false; the actor was running")
			}

			if calls != 0 {
				t.Fatalf("the transform ran %d times for an unrouted settle, want none", calls)
			}
			if parent.Current() != "supervising" {
				t.Fatalf("parent state = %q, want supervising: nothing fired", parent.Current())
			}
			if n := len(parent.Entity().notes); n != 0 {
				t.Fatalf("the unrouted settle ran %d Assigns, want none", n)
			}
			// The actor is done, as a routed settle's is, having settled with no
			// event to fire.
			if sys.IsRunning(id) || sys.Running() != 0 {
				t.Fatalf("the unrouted actor didn't settle: IsRunning(%v), Running %d", sys.IsRunning(id), sys.Running())
			}
			if tc.fail {
				// The raw failure escalated, as it does without a transform, and
				// LastError keeps it.
				if esc := sys.LastEscalation(); esc == nil || esc.ActorID != id {
					t.Fatalf("LastEscalation = %v, want the unrouted failure escalated as without a transform", esc)
				}
				if err := sys.LastError(); err == nil || err.Error() != "boom" {
					t.Fatalf("LastError = %v, want the raw failure", err)
				}
			} else if out, got := sys.LastOutput(); !got || out != tc.output {
				t.Fatalf("LastOutput = %v, %v; want the raw output %v, exactly as with no transform", out, got, tc.output)
			}
		})
	}
}

// halfRoutedMachine invokes its child with only one outcome event declared: doneOnly
// wires an onDone and no onError, so a failure routes nothing; otherwise it wires an
// onError and no onDone, so a success routes nothing. The outcome the invocation left
// undeclared has no event to settle through, exactly as a dynamic spawn that declares
// neither does: an invoked actor's unset outcome used to leave the zero event, which
// the parent refused after the transform (and, for a host, its decoder) was consulted
// for it, so the invocation's own flags — not the zero event — decide routing.
func halfRoutedMachine(doneOnly bool) *state.Machine[string, string, *trec] {
	b := state.ForgeFor[*trec]("halfrouted").
		State("idle").
		State("supervising")
	if doneOnly {
		b = b.InvokeActor("child", state.WithInvokeOnDone("childDone"))
	} else {
		b = b.InvokeActor("child", state.WithInvokeOnError("childErr"))
	}
	return b.
		State("complete").
		State("failed").
		Initial("idle").
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childDone").GoTo("complete").
		Transition("supervising").On("childErr").GoTo("failed").
		Quench()
}

// halfRoutedHarness casts halfRoutedMachine, wires the given transform and the child
// behavior that completes with "payload", and enters supervising, returning the
// parent, the system and the spawned child's id.
func halfRoutedHarness(t *testing.T, doneOnly bool, transform func(string, any, error) (any, error, error)) (
	*state.Instance[string, string, *trec], *state.ActorSystem[string, string, *trec], string,
) {
	t.Helper()
	ctx := context.Background()
	m := halfRoutedMachine(doneOnly)
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

// TestWithSettleTransform_AnInvokedActorWithoutTheOutcomeDeclaredRunsNoTransform
// proves an invoked actor routes only the outcomes its invocation declared, so the
// transform runs for neither: a success with no onDone settles as the fire-and-forget
// no-op a dynamic spawn's always was, and a failure with no onError escalates to the
// parent as a dynamic spawn's does — where each used to fire the zero event its unset
// outcome leaves, consulting the transform for an event the parent then refused.
func TestWithSettleTransform_AnInvokedActorWithoutTheOutcomeDeclaredRunsNoTransform(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		doneOnly bool // wire the onDone only (a failure routes nothing) or the onError only (a success does)
		fail     bool // settle the child as a failure through SettleError rather than a delivery
	}{
		{"a success with no onDone", false, false},
		{"a failure with no onError", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			parent, sys, id := halfRoutedHarness(t, tc.doneOnly, func(event string, output any, err error) (any, error, error) {
				calls++
				if event == "" {
					return nil, nil, errors.New("refuses the zero event")
				}
				return output, err, nil
			})
			if tc.fail {
				if res, ok := sys.SettleError(ctx, id, errors.New("boom")); ok || res.Err != nil {
					t.Fatalf("SettleError = %+v, %v; want false with no Err: nothing routed and nothing aborted", res, ok)
				}
			} else if !sys.DeliverByID(ctx, id, "finish") {
				t.Fatal("DeliverByID returned false; the actor was running")
			}
			if calls != 0 {
				t.Fatalf("the transform ran %d times for the undeclared outcome, want none: no event carries it", calls)
			}
			if parent.Current() != "supervising" {
				t.Fatalf("parent state = %q, want supervising: nothing fired", parent.Current())
			}
			if n := len(parent.Entity().notes); n != 0 {
				t.Fatalf("the settle ran %d Assigns, want none", n)
			}
			// The actor settled, as a routed settle's does, with no event to fire.
			if sys.IsRunning(id) || sys.Running() != 0 {
				t.Fatalf("the actor didn't settle: IsRunning(%v), Running %d", sys.IsRunning(id), sys.Running())
			}
			if tc.fail {
				// The raw failure escalated, as a dynamic spawn's unrouted failure does,
				// and LastError keeps it.
				if esc := sys.LastEscalation(); esc == nil || esc.ActorID != id {
					t.Fatalf("LastEscalation = %v, want the undeclared failure escalated, as a dynamic spawn's is", esc)
				}
				if err := sys.LastError(); err == nil || err.Error() != "boom" {
					t.Fatalf("LastError = %v, want the raw failure", err)
				}
			} else if out, got := sys.LastOutput(); !got || out != "payload" {
				t.Fatalf("LastOutput = %v, %v; want the raw output \"payload\"", out, got)
			}
		})
	}
}

// failingChildBehavior returns an ActorBehavior whose spawn fails, so the system
// routes the failure through the invocation's onError, or escalates it when none was
// declared.
func failingChildBehavior() state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		return nil, errors.New("can't start")
	}
}

// TestASpawnWithoutOnErrorEscalatesNotFiresTheZeroEvent proves a spawn whose
// invocation declared no onError escalates its failure to the parent as a dynamic
// spawn's does, firing no zero event: a parent that declares a transition on the zero
// event never runs it, and nothing was caught for the failure. On main the unset
// onError left the zero event, which fired and such a parent caught.
func TestASpawnWithoutOnErrorEscalatesNotFiresTheZeroEvent(t *testing.T) {
	ctx := context.Background()
	// The parent declares a transition on the zero event, which a fired zero-event
	// onError would take.
	m := state.ForgeFor[*trec]("zeroescape").
		State("idle").
		State("supervising").InvokeActor("child").
		State("caught").
		Initial("idle").
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("").GoTo("caught").
		Quench()
	parent := m.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sys := state.NewActorSystem(parent).Register("child", failingChildBehavior())
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q, want supervising: the spawn failed, no onError was declared, and no zero event fired", parent.Current())
	}
	if n := len(parent.Entity().notes); n != 0 {
		t.Fatalf("the failed spawn ran %d Assigns, want none: no zero event fired", n)
	}
	if esc := sys.LastEscalation(); esc == nil || esc.ActorID != state.ActorID(m.Name(), "supervising", 0) {
		t.Fatalf("LastEscalation = %v, want the failed spawn escalated: no onError was declared", esc)
	}
}
