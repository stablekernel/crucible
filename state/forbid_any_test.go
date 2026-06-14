package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file pins ForbidAny (kernel.go:1255): a forbidden wildcard on a state
// consumes EVERY event not otherwise handled there, ignoring it in place instead
// of bubbling to an ancestor that would handle it. It is the wildcard counterpart
// of Forbid: where Forbid blocks one named event, ForbidAny blocks all unhandled
// events.

// TestForbidAny_ConsumesEveryUnhandledEventWithoutBubbling asserts the ForbidAny
// contract against an ancestor that would otherwise handle the events. The child
// "work" declares ForbidAny. The parent "running" handles "stop" and "kick". Both
// are unhandled at the child, so — unlike a child with NO handler (which lets the
// event bubble), and unlike a plain Forbid (which blocks only one named event) —
// the forbidden wildcard must consume both in place. Neither bubbles to the parent.
func TestForbidAny_ConsumesEveryUnhandledEventWithoutBubbling(t *testing.T) {
	m := provide(state.Forge[string, string, *trec]("forbidany").
		State("idle").
		Transition("idle").On("start").GoTo("running").
		SuperState("running").
		Initial("work").
		Transition("running").On("stop").GoTo("halted").
		Transition("running").On("kick").GoTo("halted").
		SubState("work").
		ForbidAny().
		EndSuperState().
		State("halted").
		Initial("idle"))

	inst := m.Cast(&trec{}, state.WithInitialState("running"))
	if got := inst.Current(); got != "work" {
		t.Fatalf("setup: Current() = %q, want work", got)
	}

	// "stop" is handled by the parent but unhandled at the child: ForbidAny consumes
	// it in place, so it must NOT bubble to the parent's stop -> halted.
	res := inst.Fire(context.Background(), "stop")
	if res.Err != nil {
		t.Fatalf("Fire(stop) err = %v, want nil (forbidden wildcard ignores it)", res.Err)
	}
	if res.NewState == "halted" {
		t.Fatalf("forbidden-wildcard event 'stop' bubbled to ancestor: NewState = halted")
	}
	if inst.Current() != "work" {
		t.Fatalf("Current() = %q, want work (forbidden wildcard consumed 'stop' in place)", inst.Current())
	}

	// "kick" is also handled by the parent but unhandled at the child: unlike plain
	// Forbid (which blocks only one named event and lets other events bubble),
	// ForbidAny blocks this one too — the defining difference between the two.
	res = inst.Fire(context.Background(), "kick")
	if res.Err != nil {
		t.Fatalf("Fire(kick) err = %v, want nil (forbidden wildcard ignores it)", res.Err)
	}
	if res.NewState == "halted" {
		t.Fatalf("forbidden-wildcard event 'kick' bubbled to ancestor: NewState = halted")
	}
	if inst.Current() != "work" {
		t.Fatalf("Current() = %q, want work (forbidden wildcard consumed 'kick' in place)", inst.Current())
	}

	// The outcome of a forbidden (consumed) event is a clean success, mirroring the
	// specific-Forbid contract (TestFireFromState_ForbiddenEventIsConsumed).
	if res.Trace.Outcome != state.OutcomeSuccess {
		t.Fatalf("forbidden-wildcard outcome = %v, want OutcomeSuccess", res.Trace.Outcome)
	}
}
