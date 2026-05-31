package verify

// This file is the coverage cross-check: on guard-free fixtures — where the
// structural configuration-product model and the kernel's actual execution
// coincide — it replays scenarios through conformance.RunAgainst and asserts the
// states and transitions the real engine enters and fires equal the covered set
// verify reports. Coverage's covered set is computed over the structural explorer;
// this pins it to the executable semantics where the two must agree, so a
// structural over- or under-count would surface as a divergence rather than be
// silently trusted.

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/conformance"
)

// engineCoverage replays a scenario set through real instances of a guard-free
// machine and returns the states actually entered and the transitions actually
// fired, in the same "from -on-> to" / active-state vocabulary the structural
// coverage report uses. It is the executable authority the reported covered set
// must equal.
func engineCoverage(t *testing.T, m *state.Machine[string, string, any], initial string, scenarios [][]string) (states, transitions map[string]bool) {
	t.Helper()
	codec := conformance.EventCodec[string]{
		Named:   func(e string) string { return e },
		Resolve: func(name string) (string, bool) { return name, true },
	}
	parent := buildConfigGraph(m).parent
	states = map[string]bool{}
	transitions = map[string]bool{}

	addActive := func(leaves []string) {
		for _, leaf := range leaves {
			for n := leaf; n != ""; n = parent[n] {
				states[n] = true
			}
		}
	}

	// Every scenario starts in the initial configuration, so its active states count
	// even before any event is fired.
	startInst := m.Cast(nil, state.WithInitialState(initial))
	addActive(startInst.Configuration())

	for _, sc := range scenarios {
		inst := m.Cast(nil, state.WithInitialState(initial))
		for _, ev := range sc {
			fr := inst.Fire(context.Background(), ev)
			// A successful transition records the fired edge (matched source -event-> new
			// state) and the newly active configuration. An event with no enabled
			// transition yields a non-success outcome and leaves the configuration
			// unchanged — the executable counterpart of the structural no-op — so it is
			// neither fatal nor counted.
			if fr.Trace.Outcome == state.OutcomeSuccess {
				transitions[transitionKey(fr.Trace.MatchedAt, ev, fr.NewState)] = true
				addActive(inst.Configuration())
			}
		}
		_ = conformance.RunAgainst(m, conformance.Scenario{
			MachineID:    m.Name(),
			InitialState: initial,
			Events:       eventsForScenario(sc),
		}, nil, codec, initial)
	}
	return states, transitions
}

// eventsForScenario projects an event-name sequence onto conformance scenario
// steps, so the same sequence can be driven through RunAgainst as a portable
// artifact.
func eventsForScenario(events []string) []conformance.Event {
	out := make([]conformance.Event, len(events))
	for i, e := range events {
		out[i] = conformance.Event{Event: e}
	}
	return out
}

// TestCoverage_CrossCheck_Conformance asserts, on guard-free fixtures, that the
// covered states and transitions verify reports equal the states and transitions a
// real instance enters and fires when the scenarios are replayed through the
// kernel — the structural coverage is pinned to executable truth.
func TestCoverage_CrossCheck_Conformance(t *testing.T) {
	cases := []struct {
		name      string
		machine   *state.Machine[string, string, any]
		initial   string
		scenarios [][]string
		// flatEdges is true when the machine's declared transitions map one-to-one to
		// the kernel trace's matched-source/new-state edges — the case for flat
		// machines. A hierarchical machine descends on entry, so the kernel's leaf-level
		// new state differs from the declared composite target; there the cross-check
		// compares state coverage only, since transition identity is well-defined by the
		// flat fixtures.
		flatEdges bool
	}{
		{
			name:    "linear-partial",
			machine: fxLinear(),
			initial: "a",
			scenarios: [][]string{
				{"next", "next"}, // a -> b -> c, leaving d uncovered
			},
			flatEdges: true,
		},
		{
			name:    "linear-full",
			machine: fxLinear(),
			initial: "a",
			scenarios: [][]string{
				{"next", "next", "next"},
			},
			flatEdges: true,
		},
		{
			name:    "branching-both-arms",
			machine: fxBranching(),
			initial: "start",
			scenarios: [][]string{
				{"left"},
				{"right"},
			},
			flatEdges: true,
		},
		{
			name:    "branching-noop",
			machine: fxBranching(),
			initial: "start",
			scenarios: [][]string{
				{"nope", "left"}, // unhandled "nope" is a no-op, then left fires
			},
			flatEdges: true,
		},
		{
			name:    "parallel-both-regions",
			machine: fxParallel(),
			initial: "offline",
			scenarios: [][]string{
				{"activate", "work", "report"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cg := buildConfigGraph(tc.machine)
			f := coverageFor(cg, tc.scenarios)
			rep := f.coverage

			wantStates, wantTransitions := engineCoverage(t, tc.machine, tc.initial, tc.scenarios)

			gotStates := toBoolSet(rep.CoveredStates)
			if !sameSet(gotStates, wantStates) {
				t.Errorf("covered states diverge from engine:\n verify:  %v\n engine:  %v",
					rep.CoveredStates, sortedKeys(wantStates))
			}
			if tc.flatEdges {
				gotTransitions := toBoolSet(rep.CoveredTransitions)
				if !sameSet(gotTransitions, wantTransitions) {
					t.Errorf("covered transitions diverge from engine:\n verify:  %v\n engine:  %v",
						rep.CoveredTransitions, sortedKeys(wantTransitions))
				}
			}
		})
	}
}

func toBoolSet(xs []string) map[string]bool {
	out := map[string]bool{}
	for _, x := range xs {
		out[x] = true
	}
	return out
}
