package conformance

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// GenerateOption configures scenario generation.
type GenerateOption func(*generateConfig)

type generateConfig struct {
	maxDepth int
}

// WithMaxDepth caps the length of generated event paths. A non-positive value
// (the default) means no cap beyond the reachable graph.
func WithMaxDepth(d int) GenerateOption {
	return func(c *generateConfig) { c.maxDepth = d }
}

// GenerateScenarios derives a scenario for the shortest event path to every
// reachable state, by breadth-first search over the machine's IR graph. This is
// the model-based layer: it mirrors path planning so a machine's own structure
// produces its coverage, with no hand-authored fixtures.
//
// Each generated scenario asserts the final state it targets, the trace length
// (the number of events fired), and that no errors occurred. The namer renders
// each typed event to the stable name the kernel records, so generated
// scenarios are directly serializable and replayable.
//
// Generation walks the IR — the same exported, serializable graph ToJSON emits —
// so it is fully generic: it works for any machine, flat or hierarchical, and
// never reaches into kernel internals.
func GenerateScenarios[S comparable, E comparable, C any](
	m *state.Machine[S, E, C],
	namer EventNamer[E],
	opts ...GenerateOption,
) ([]Scenario, error) {
	cfg := generateConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	data, err := m.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("conformance: generate: %w", err)
	}
	ir, err := state.LoadFromJSON[S, E, C](data)
	if err != nil {
		return nil, fmt.Errorf("conformance: generate: %w", err)
	}
	if !ir.HasInitial {
		return nil, fmt.Errorf("conformance: generate: machine %q has no initial state", m.Name())
	}

	edges := collectEdges(ir.States)
	initial := ir.Initial

	type node struct {
		state S
		path  []E
	}
	visited := map[S]bool{initial: true}
	queue := []node{{state: initial, path: nil}}
	scenarios := []Scenario{}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		for _, e := range edges[cur.state] {
			if visited[e.to] {
				continue
			}
			if cfg.maxDepth > 0 && len(cur.path) >= cfg.maxDepth {
				continue
			}
			next := make([]E, len(cur.path)+1)
			copy(next, cur.path)
			next[len(cur.path)] = e.on

			visited[e.to] = true
			scenarios = append(scenarios, scenarioFor(m.Name(), initial, e.to, next, namer))
			queue = append(queue, node{state: e.to, path: next})
		}
	}

	return scenarios, nil
}

// edge is a directed event-bearing edge in the IR graph.
type edge[S comparable, E comparable] struct {
	to S
	on E
}

// collectEdges flattens the IR's (possibly nested) states into an adjacency map
// of event-bearing, non-internal, non-eventless edges keyed by source state.
func collectEdges[S comparable, E comparable, C any](states []state.State[S, E, C]) map[S][]edge[S, E] {
	out := map[S][]edge[S, E]{}
	var walk func(ss []state.State[S, E, C])
	walk = func(ss []state.State[S, E, C]) {
		for i := range ss {
			s := &ss[i]
			for ti := range s.Transitions {
				t := &s.Transitions[ti]
				// Skip edges that do not produce an event-driven change to a named
				// target: eventless and internal transitions, forbidden blocks, and
				// targetless wildcard catch-alls (no concrete target to assert).
				if t.EventLess || t.Internal || t.Forbidden || t.Wildcard {
					continue
				}
				out[s.Name] = append(out[s.Name], edge[S, E]{to: t.To, on: t.On})
			}
			walk(s.Children)
			for ri := range s.Regions {
				walk(s.Regions[ri].States)
			}
		}
	}
	walk(states)
	return out
}

// scenarioFor builds a scenario that drives the machine from initial to target
// along path, with the standard generated assertion set.
func scenarioFor[S comparable, E comparable](machineID string, initial, target S, path []E, namer EventNamer[E]) Scenario {
	events := make([]Event, len(path))
	for i, e := range path {
		events[i] = Event{Event: namer(e)}
	}
	return Scenario{
		SchemaVersion: schemaVersion,
		MachineID:     machineID,
		Name:          fmt.Sprintf("reach-%v", target),
		InitialState:  fmt.Sprint(initial),
		Events:        events,
		Assertions: []Assertion{
			{Type: AssertFinalState, Expected: fmt.Sprint(target)},
			{Type: AssertTraceLength, Expected: len(path)},
			{Type: AssertNoErrors, Expected: true},
		},
	}
}
