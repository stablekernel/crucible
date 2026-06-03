package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// BenchmarkFire measures a single flat-machine transition through the full Fire
// pipeline (resolve, guard, action, trace).
func BenchmarkFire(b *testing.B) {
	m := buildDocMachine()
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Cast(doc, state.WithInitialState(Draft)).Fire(ctx, Submit)
	}
}

// BenchmarkFireGuarded measures a guarded transition (the guard evaluates before
// the action runs).
func BenchmarkFireGuarded(b *testing.B) {
	m := buildDocMachine()
	doc := &Document{Status: Submitted, ReviewerID: strptr("rev-1")}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Cast(doc, state.WithInitialState(Submitted)).Fire(ctx, Approve)
	}
}

// BenchmarkCascade measures a hierarchical entry cascade: entering the Running
// superstate descends to its initial child, running the entry chain.
func BenchmarkCascade(b *testing.B) {
	m := buildJobMachine()
	job := &Job{Status: Queued}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Cast(job, state.WithInitialState(Queued)).Fire(ctx, Enqueue)
	}
}

// BenchmarkParallelBroadcast measures broadcasting an event across the worker's
// two orthogonal regions.
func BenchmarkParallelBroadcast(b *testing.B) {
	m := buildWorkerMachine()
	w := &Worker{State: Active}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Cast(w, state.WithInitialState(Active)).Fire(ctx, StartWork)
	}
}

// benchToggleState is the state type for the steady-state toggle benchmark.
type benchToggleState int

const (
	benchOn  benchToggleState = iota
	benchOff benchToggleState = iota
)

// benchToggleEvent is the event type for the steady-state toggle benchmark.
type benchToggleEvent int

const benchFlip benchToggleEvent = 0

// buildBenchToggleMachine builds a minimal 2-state machine used by
// BenchmarkFireSteady to drive a pure steady-state toggle without Cast overhead.
func buildBenchToggleMachine() *state.Machine[benchToggleState, benchToggleEvent, any] {
	return state.Forge[benchToggleState, benchToggleEvent, any]("bench-toggle").
		State(benchOn).
		Transition(benchOn).On(benchFlip).GoTo(benchOff).
		State(benchOff).
		Transition(benchOff).On(benchFlip).GoTo(benchOn).
		Initial(benchOn).
		Quench()
}

// BenchmarkFireSteady measures the steady-state Fire cost: Cast once outside
// the loop, then fire a 2-state toggle repeatedly. This isolates per-Fire
// allocation cost from Cast overhead and is the primary benchmark for the
// lite-mode optimization target (< 10 allocs/op).
func BenchmarkFireSteady(b *testing.B) {
	m := buildBenchToggleMachine()
	ctx := context.Background()

	b.Run("lite", func(b *testing.B) {
		inst := m.Cast(nil, state.WithInitialState[benchToggleState](benchOn))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inst.Fire(ctx, benchFlip)
		}
	})

	b.Run("full", func(b *testing.B) {
		inst := m.Cast(nil,
			state.WithInitialState[benchToggleState](benchOn),
			state.WithFullTrace[benchToggleState](),
		)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			inst.Fire(ctx, benchFlip)
		}
	})
}

// BenchmarkLoadFromJSON measures the IR parse hot path on a representative
// machine definition.
func BenchmarkLoadFromJSON(b *testing.B) {
	data, err := buildDocMachine().ToJSON()
	if err != nil {
		b.Fatalf("ToJSON: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := state.LoadFromJSON[DocState, DocEvent, *Document](data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkToJSON measures the canonical serialization hot path.
func BenchmarkToJSON(b *testing.B) {
	m := buildDocMachine()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.ToJSON(); err != nil {
			b.Fatal(err)
		}
	}
}
