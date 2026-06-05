package verify

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// topology is the flattened, witness-relevant view of a machine's IR: the
// declared states in declaration order and each state's enclosing parent. It is
// derived from the serialized IR — the same position-independent public view the
// analysis package flattens — so a code-built and a JSON-loaded machine produce
// identical topologies.
type topology struct {
	// order lists every declared state name (including nested substates and region
	// states) in declaration order, for stable output.
	order []string
	// parent maps a state name to its lexically enclosing composite state, "" for a
	// top-level state. Used to derive a witness for a substate entered by initial
	// descent rather than by a firing event.
	parent map[string]string
}

// readTopology flattens the machine's IR into a topology. A machine whose IR
// cannot be read yields the zero topology rather than panicking, matching
// Verify's no-panic contract.
func readTopology[S comparable, E comparable, C any](m *state.Machine[S, E, C]) topology {
	ir, ok := loadIR(m)
	if !ok {
		return topology{parent: map[string]string{}}
	}
	return topologyFromIR(ir)
}

// topologyFromIR builds a topology from an already-loaded IR, so a caller that has
// round-tripped the machine once can reuse it instead of re-serializing.
func topologyFromIR[S comparable, E comparable, C any](ir *state.IR[S, E, C]) topology {
	t := topology{parent: map[string]string{}}
	for i := range ir.States {
		collectStates(&ir.States[i], "", &t)
	}
	return t
}

// collectStates records a state and its parent, then recurses through its
// children and region states, preserving declaration order so the enumeration is
// stable.
func collectStates[S comparable, E comparable, C any](s *state.State[S, E, C], parent string, t *topology) {
	name := fmt.Sprint(s.Name)
	t.order = append(t.order, name)
	t.parent[name] = parent
	for i := range s.Children {
		collectStates(&s.Children[i], name, t)
	}
	for ri := range s.Regions {
		for i := range s.Regions[ri].States {
			collectStates(&s.Regions[ri].States[i], name, t)
		}
	}
}

// initialName returns the machine's initial state name, or "" when the machine
// declares no initial state.
func initialName[S comparable, E comparable, C any](m *state.Machine[S, E, C]) string {
	ir, ok := loadIR(m)
	if !ok {
		return ""
	}
	return initialNameFromIR(ir)
}

// initialNameFromIR returns the initial state name from an already-loaded IR.
func initialNameFromIR[S comparable, E comparable, C any](ir *state.IR[S, E, C]) string {
	if !ir.HasInitial {
		return ""
	}
	return fmt.Sprint(ir.Initial)
}

// loadIR round-trips the machine through its JSON IR, the same technique the
// analysis and evolution packages use to obtain a position-independent public
// view. It reports false when the IR cannot be read rather than returning an
// error, so callers can fall back to an empty result.
func loadIR[S comparable, E comparable, C any](m *state.Machine[S, E, C]) (*state.IR[S, E, C], bool) {
	b, err := m.ToJSON()
	if err != nil {
		return nil, false
	}
	ir, err := state.LoadFromJSON[S, E, C](b)
	if err != nil {
		return nil, false
	}
	return ir, true
}
