package analysis_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// This file benchmarks the static path-enumeration hot paths — ShortestPaths
// (breadth-first, one minimal path per reachable state) and SimplePaths
// (depth-first, every acyclic path per reachable state) — over a non-trivial
// machine with branches and a cycle, so the depth-first enumeration explores a
// meaningful number of routes. Both report allocations and join the benchstat gate.

// buildPathsBenchMachine forges a branchy graph with a back-edge: a pipeline of
// stages where each stage can advance, skip ahead, or bounce back to an earlier
// stage, plus a terminal. The branching and the cycle make SimplePaths enumerate
// many distinct acyclic routes, exercising the depth-first walk's backtracking.
func buildPathsBenchMachine() *state.Machine[string, string, any] {
	return state.Forge[string, string, any]("paths-bench").
		State("intake").
		Transition("intake").On("next").GoTo("triage").
		Transition("intake").On("skip").GoTo("review").
		State("triage").
		Transition("triage").On("next").GoTo("build").
		Transition("triage").On("skip").GoTo("review").
		Transition("triage").On("back").GoTo("intake").
		State("build").
		Transition("build").On("next").GoTo("review").
		Transition("build").On("back").GoTo("triage").
		State("review").
		Transition("review").On("next").GoTo("verify").
		Transition("review").On("back").GoTo("build").
		State("verify").
		Transition("verify").On("ship").GoTo("done").
		Transition("verify").On("back").GoTo("review").
		State("done").Final().
		Initial("intake").
		CurrentStateFn(func(any) string { return "intake" }).
		Quench()
}

// BenchmarkShortestPaths measures the breadth-first shortest-path enumeration over
// the branchy machine: one minimal path per reachable state.
func BenchmarkShortestPaths(b *testing.B) {
	m := buildPathsBenchMachine()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := analysis.ShortestPaths(m); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSimplePaths measures the depth-first acyclic-path enumeration over the
// branchy, cyclic machine: every simple path per reachable state, the exhaustive
// scenario set with backtracking on the back-edges.
func BenchmarkSimplePaths(b *testing.B) {
	m := buildPathsBenchMachine()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := analysis.SimplePaths(m); err != nil {
			b.Fatal(err)
		}
	}
}
