package analysis_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// feedbackMachine is a canonical path-enumeration example: a linear-with-shortcuts
// feedback flow. "question" branches to "thanks" (good) or "form" (bad) or
// "closed"; "form" reaches "thanks" or "closed"; "thanks" reaches "closed". It is
// guard-free, so ShortestPaths is directly comparable to the kernel's PlanPath.
func feedbackMachine() *state.Machine[string, string, any] {
	return state.Forge[string, string, any]("feedback").
		State("question").
		Transition("question").On("CLICK_GOOD").GoTo("thanks").
		Transition("question").On("CLICK_BAD").GoTo("form").
		Transition("question").On("CLOSE").GoTo("closed").
		State("form").
		Transition("form").On("SUBMIT").GoTo("thanks").
		Transition("form").On("CLOSE").GoTo("closed").
		State("thanks").
		Transition("thanks").On("CLOSE").GoTo("closed").
		State("closed").Final().
		Initial("question").
		CurrentStateFn(func(any) string { return "question" }).
		Quench()
}

// cyclicMachine has a real cycle (a <-> b) plus an exit to a terminal "c", so the
// simple-path walk must refuse to re-enter a state on the current path to
// terminate.
func cyclicMachine() *state.Machine[string, string, any] {
	return state.Forge[string, string, any]("cyclic").
		State("a").
		Transition("a").On("toB").GoTo("b").
		State("b").
		Transition("b").On("toA").GoTo("a").
		Transition("b").On("toC").GoTo("c").
		State("c").Final().
		Initial("a").
		CurrentStateFn(func(any) string { return "a" }).
		Quench()
}

// TestShortestPaths_CoversAllReachable asserts ShortestPaths reaches every state of
// the feedback machine, with the initial state at the empty path and the expected
// minimal lengths.
func TestShortestPaths_CoversAllReachable(t *testing.T) {
	paths, err := analysis.ShortestPaths(feedbackMachine())
	if err != nil {
		t.Fatalf("ShortestPaths: %v", err)
	}

	for _, want := range []string{"question", "form", "thanks", "closed"} {
		if _, ok := paths[want]; !ok {
			t.Fatalf("state %q missing from shortest paths", want)
		}
	}
	if len(paths["question"].Steps) != 0 {
		t.Fatalf("initial path = %v, want empty", paths["question"].Steps)
	}
	// "form" is one event away (CLICK_BAD); "closed" one away (CLOSE); "thanks" one
	// away (CLICK_GOOD).
	if got := len(paths["form"].Steps); got != 1 {
		t.Fatalf("shortest to form = %d steps, want 1", got)
	}
	if got := len(paths["closed"].Steps); got != 1 {
		t.Fatalf("shortest to closed = %d steps, want 1", got)
	}
}

// TestPath_StatesAndEvents asserts Path.States renders the ordered states visited
// (initial first, Target last) and stays consistent with Path.Events: a path of n
// steps yields n events and n+1 states.
func TestPath_StatesAndEvents(t *testing.T) {
	paths, err := analysis.ShortestPaths(feedbackMachine())
	if err != nil {
		t.Fatalf("ShortestPaths: %v", err)
	}

	// The empty path (initial state reaching itself) is just the initial state.
	if got := paths["question"].States("question"); len(got) != 1 || got[0] != "question" {
		t.Fatalf("States for the empty path = %v, want [question]", got)
	}

	// A two-step path to thanks via form: question -> form -> thanks.
	simple, err := analysis.SimplePaths(feedbackMachine())
	if err != nil {
		t.Fatalf("SimplePaths: %v", err)
	}
	viaForm := simple["thanks"][1] // sorted shortest-first; [1] is the two-step route
	states := viaForm.States("question")
	wantStates := []string{"question", "form", "thanks"}
	if len(states) != len(wantStates) {
		t.Fatalf("States = %v, want %v", states, wantStates)
	}
	for i := range wantStates {
		if states[i] != wantStates[i] {
			t.Fatalf("States[%d] = %q, want %q (full %v)", i, states[i], wantStates[i], states)
		}
	}
	// States is always one longer than Events (it includes the initial state).
	if len(states) != len(viaForm.Events())+1 {
		t.Fatalf("len(States)=%d, len(Events)=%d; want States == Events+1", len(states), len(viaForm.Events()))
	}
}

// TestShortestPaths_MatchesPlanPath asserts that for each single target, the
// shortest path's length matches the kernel's PlanPath length on the same guard-free
// machine — ShortestPaths is the multi-target generalization of PlanPath.
func TestShortestPaths_MatchesPlanPath(t *testing.T) {
	m := feedbackMachine()
	paths, err := analysis.ShortestPaths(m)
	if err != nil {
		t.Fatalf("ShortestPaths: %v", err)
	}

	for _, target := range []string{"form", "thanks", "closed"} {
		plan, err := m.PlanPath("question", target, nil)
		if err != nil {
			t.Fatalf("PlanPath to %q: %v", target, err)
		}
		got := len(paths[target].Steps)
		if got != len(plan) {
			t.Fatalf("path length to %q: ShortestPaths=%d PlanPath=%d", target, got, len(plan))
		}
	}
}

// TestSimplePaths_Enumerates asserts SimplePaths lists every acyclic route to a
// state. To "thanks" there are two simple paths: direct (CLICK_GOOD) and via "form"
// (CLICK_BAD, SUBMIT).
func TestSimplePaths_Enumerates(t *testing.T) {
	paths, err := analysis.SimplePaths(feedbackMachine())
	if err != nil {
		t.Fatalf("SimplePaths: %v", err)
	}

	thanks := paths["thanks"]
	if len(thanks) != 2 {
		t.Fatalf("simple paths to thanks = %d, want 2", len(thanks))
	}
	// Sorted shortest-first: the direct one-step path, then the two-step via form.
	if len(thanks[0].Steps) != 1 || thanks[0].Steps[0].Event != "CLICK_GOOD" {
		t.Fatalf("first simple path to thanks = %v, want [CLICK_GOOD]", thanks[0].Events())
	}
	if len(thanks[1].Steps) != 2 {
		t.Fatalf("second simple path to thanks = %v, want 2 steps", thanks[1].Events())
	}
}

// TestSimplePaths_TerminatesOnCycle asserts the simple-path walk terminates on a
// machine with a cycle (a <-> b), returning only acyclic paths. To "c" the single
// simple path is a -> b -> c (toB, toC); the a->b->a->... cycle is never expanded.
func TestSimplePaths_TerminatesOnCycle(t *testing.T) {
	paths, err := analysis.SimplePaths(cyclicMachine())
	if err != nil {
		t.Fatalf("SimplePaths: %v", err)
	}

	toC := paths["c"]
	if len(toC) != 1 {
		t.Fatalf("simple paths to c = %d, want 1", len(toC))
	}
	if got := toC[0].Events(); len(got) != 2 || got[0] != "toB" || got[1] != "toC" {
		t.Fatalf("simple path to c = %v, want [toB toC]", got)
	}

	// "b" is reached by exactly one simple path (toB); the cyclic return to "a" is
	// excluded because "a" is already on the path.
	toB := paths["b"]
	if len(toB) != 1 {
		t.Fatalf("simple paths to b = %d, want 1", len(toB))
	}
}
