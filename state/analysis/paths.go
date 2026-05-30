package analysis

import (
	"sort"

	"github.com/stablekernel/crucible/state"
)

// This file ships path enumeration over a machine's static transition graph — the
// shortest-paths / simple-paths enumeration over the graph. It lives in the
// analysis package because it reuses the same guard-agnostic flattened graph the
// reachability checks use: paths are enumerated over the IR's event-triggered
// edges without evaluating guards, since a static pass cannot run a host guard and
// a guard can only ever prune an edge at run time, never add one.
//
// Relation to the kernel's PlanPath: PlanPath (in package state) answers "shortest
// event sequence from one state to one target", honoring guards against a concrete
// entity. ShortestPaths generalizes the shortest-path half to "shortest sequence
// from the initial state to EVERY reachable state" with no entity — guard-agnostic,
// so it covers states a guarded PlanPath might not traverse for a particular
// entity. For a single target with a guard-free path the two agree. SimplePaths
// enumerates ALL acyclic paths to each state, the exhaustive scenario set a
// conformance harness draws from.

// Step is one segment of a path: the event that fires from the segment's source
// state, and the destination state it leads to. A path is read as: starting at the
// initial state, fire Step[0].Event to reach Step[0].To, then Step[1].Event, and so
// on. A path segment carries its event plus the resulting
// `state`).
type Step struct {
	// Event is the string label of the event fired for this segment; "always" for an
	// eventless transition traversed on the path.
	Event string
	// From is the state the segment is fired from.
	From string
	// To is the state reached after firing Event.
	To string
}

// Path is one route from the machine's initial state to a target state: the
// ordered events to fire and the states visited along the way. The empty path
// (no steps) is the initial state reaching itself. Events returns just the event
// labels — the input a driver would FireSeq to walk the path.
type Path struct {
	// Target is the state this path ends at.
	Target string
	// Steps are the ordered segments from the initial state to Target.
	Steps []Step
}

// Events returns the ordered event labels of the path — the sequence a host would
// fire to drive an instance from the initial state to Target. An eventless segment
// contributes "always" (the machine auto-fires it; a driver need not).
func (p Path) Events() []string {
	out := make([]string, 0, len(p.Steps))
	for _, s := range p.Steps {
		out = append(out, s.Event)
	}
	return out
}

// States returns the ordered states visited, starting at the initial state and
// ending at Target.
func (p Path) States(initial string) []string {
	out := make([]string, 0, len(p.Steps)+1)
	out = append(out, initial)
	for _, s := range p.Steps {
		out = append(out, s.To)
	}
	return out
}

// ShortestPaths returns the shortest event sequence from the machine's initial
// state to each reachable state, keyed by target state name — the
// `getShortestPaths` analog. It is the multi-target generalization of the kernel's
// PlanPath: a breadth-first walk over the static transition graph yields, for every
// reachable state, one minimal path (fewest events). The initial state maps to the
// empty path.
//
// It is guard-agnostic, exactly like the analysis reachability checks: guards are
// opaque host funcs a static pass cannot evaluate, and a guard only ever removes an
// edge at run time, so a state reachable here is reachable in some run, and a path
// found here is the shortest ignoring guard truth. For a single target whose
// shortest route uses no guarded edge, ShortestPaths agrees with PlanPath; where
// PlanPath's entity disables a guarded edge it may return a longer path or none,
// while ShortestPaths reports the guard-free minimum.
//
// The returned map is deterministic: each path is the BFS-minimal one, with ties
// broken by the declaration order of edges out of each state.
func ShortestPaths[S comparable, E comparable, C any](m *state.Machine[S, E, C]) (map[string]Path, error) {
	g, err := buildGraph(m)
	if err != nil {
		return nil, err
	}
	out := map[string]Path{}
	if !g.hasInitial {
		return out, nil
	}

	out[g.initial] = Path{Target: g.initial}

	type qitem struct {
		state string
		steps []Step
	}
	visited := map[string]bool{g.initial: true}
	queue := []qitem{{state: g.initial}}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, e := range planEdges(g, cur.state) {
			if visited[e.to] {
				continue
			}
			visited[e.to] = true
			next := appendStep(cur.steps, e)
			out[e.to] = Path{Target: e.to, Steps: next}
			queue = append(queue, qitem{state: e.to, steps: next})
		}
	}
	return out, nil
}

// SimplePaths returns every acyclic (simple) path from the machine's initial state
// to each reachable state, keyed by target state name — the `getSimplePaths`
// analog. A simple path visits no state twice, so enumeration always terminates
// even on a machine with cycles: a depth-first walk that refuses to re-enter a
// state already on the current path cannot loop. Each reachable state maps to the
// (possibly several) simple paths that reach it; the initial state maps to a single
// empty path.
//
// Like ShortestPaths it is guard-agnostic — paths are enumerated over the static
// edges without evaluating guards — so the result is the full structural scenario
// set, the same exhaustive enumeration a conformance harness draws coverage from.
// Paths to each target are returned in a deterministic order: discovered by a
// declaration-order depth-first walk, then sorted by length and by their event
// sequence so the set is reproducible.
func SimplePaths[S comparable, E comparable, C any](m *state.Machine[S, E, C]) (map[string][]Path, error) {
	g, err := buildGraph(m)
	if err != nil {
		return nil, err
	}
	out := map[string][]Path{}
	if !g.hasInitial {
		return out, nil
	}

	out[g.initial] = []Path{{Target: g.initial}}

	onPath := map[string]bool{g.initial: true}
	var steps []Step

	var walk func(node string)
	walk = func(node string) {
		for _, e := range planEdges(g, node) {
			if onPath[e.to] {
				continue // re-entering a state on the current path would form a cycle
			}
			next := appendStep(steps, e)
			out[e.to] = append(out[e.to], Path{Target: e.to, Steps: cloneSteps(next)})

			onPath[e.to] = true
			steps = next
			walk(e.to)
			steps = steps[:len(steps)-1]
			delete(onPath, e.to)
		}
	}
	walk(g.initial)

	for k := range out {
		sortPaths(out[k])
	}
	return out, nil
}

// planEdges returns the path-relevant outgoing edges of a state in declaration
// order: event-triggered and eventless transitions that lead somewhere new.
// Internal (self) edges advance no path step and are skipped, matching the
// reachability walk and PlanPath's edge filter. An edge whose target is not a
// declared node is skipped defensively.
func planEdges(g *graph, from string) []edge {
	var out []edge
	for _, e := range g.outgoing[from] {
		if e.internal {
			continue
		}
		if _, ok := g.nodes[e.to]; !ok {
			continue
		}
		out = append(out, e)
	}
	return out
}

// appendStep returns prev with one new Step for edge e appended, copying so the
// caller's slice is never aliased.
func appendStep(prev []Step, e edge) []Step {
	ev := e.on
	if e.eventLess {
		ev = "always"
	}
	next := make([]Step, len(prev)+1)
	copy(next, prev)
	next[len(prev)] = Step{Event: ev, From: e.from, To: e.to}
	return next
}

// cloneSteps returns an independent copy of a step slice, so a stored path does not
// alias the mutable depth-first walk buffer.
func cloneSteps(s []Step) []Step {
	out := make([]Step, len(s))
	copy(out, s)
	return out
}

// sortPaths orders a target's simple paths deterministically: shortest first, then
// lexicographically by event sequence, so the enumeration is reproducible.
func sortPaths(ps []Path) {
	sort.SliceStable(ps, func(i, j int) bool {
		a, b := ps[i].Steps, ps[j].Steps
		if len(a) != len(b) {
			return len(a) < len(b)
		}
		for k := range a {
			if a[k].Event != b[k].Event {
				return a[k].Event < b[k].Event
			}
			if a[k].To != b[k].To {
				return a[k].To < b[k].To
			}
		}
		return false
	})
}
