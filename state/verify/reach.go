package verify

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// This file implements conditional reachability: "can the machine reach a target
// state X along some run that never passes through any avoid state Y?". It is the
// safety/reachability half of a CTL/LTL-lite property set — a witness-carrying
// answer to "reach X without entering Y".
//
// The search is a guard-agnostic breadth-first walk over the same structural
// reachability the analysis package computes (an event/eventless edge advances a
// step; entering a composite descends into its initial children; reaching a
// substate activates its enclosing parent). The single addition over plain
// reachability is avoid-set PRUNING: a configuration whose active state set
// intersects the avoid-set is never expanded, so no path the search returns can
// cross an avoided state.
//
// Membership ("a configuration is in Y"): a state node's activation, when
// entered, brings a whole configuration of active states online — the node
// itself, every enclosing ancestor that must be active for it to be active, and
// the initial-descent leaves a composite or parallel node pulls in. The
// configuration is "in Y" when ANY of those active states is an avoid state.
// This is the standard In(Y) predicate over the active configuration: a
// parallel/compound config counts as passing through Y if any active leaf, any
// active ancestor, or any initial-descent member is Y. The avoid is therefore
// exact for hierarchy — avoiding a region leaf, a superstate ancestor, or a
// sibling initial-descent state each correctly forbids the whole configuration.

// searchGraph is the constrained-search view of a machine's IR: the structural
// edges plus, for each node, the active-configuration members its entry brings
// online. It is built from the same serialized public IR readTopology flattens,
// so a code-built and a JSON-loaded machine search identically.
type searchGraph struct {
	// initial is the machine's initial state name; hasInitial guards the empty case.
	initial    string
	hasInitial bool
	// nodes is the set of declared state names, for membership tests.
	nodes map[string]bool
	// parent maps a state to its lexically enclosing composite, "" at top level.
	parent map[string]string
	// initialChildren maps a composite or parallel state to the states entered by
	// initial descent when it is entered (a compound's initial child, each region's
	// initial child).
	initialChildren map[string][]string
	// edges maps a source state to its path-advancing outgoing transitions in
	// declaration order, so the walk is deterministic.
	edges map[string][]searchEdge
}

// searchEdge is one path-advancing transition: the event fired and the state
// reached. Internal (self) transitions advance no path step and are omitted.
type searchEdge struct {
	event     string
	eventLess bool
	from      string
	to        string
}

// buildSearchGraph flattens the machine's public IR into a searchGraph. A machine
// whose IR cannot be read yields the zero graph (hasInitial false) rather than
// panicking, matching Verify's no-panic contract.
func buildSearchGraph[S comparable, E comparable, C any](m *state.Machine[S, E, C]) searchGraph {
	ir, ok := loadIR(m)
	if !ok {
		return emptySearchGraph()
	}
	return buildSearchGraphFromIR(ir)
}

// buildSearchGraphFromIR builds the searchGraph from an already-loaded IR, so a
// caller that round-tripped the machine once can reuse it.
func buildSearchGraphFromIR[S comparable, E comparable, C any](ir *state.IR[S, E, C]) searchGraph {
	g := emptySearchGraph()
	if ir.HasInitial {
		g.hasInitial = true
		g.initial = fmt.Sprint(ir.Initial)
	}
	for i := range ir.States {
		collectSearch(&ir.States[i], "", &g)
	}
	return g
}

// emptySearchGraph returns a searchGraph with initialized maps and no states.
func emptySearchGraph() searchGraph {
	return searchGraph{
		nodes:           map[string]bool{},
		parent:          map[string]string{},
		initialChildren: map[string][]string{},
		edges:           map[string][]searchEdge{},
	}
}

// collectSearch records one state's structure — parent, initial-descent children,
// and path-advancing edges — and recurses through its children and region states
// in declaration order.
func collectSearch[S comparable, E comparable, C any](s *state.State[S, E, C], parent string, g *searchGraph) {
	name := fmt.Sprint(s.Name)
	g.nodes[name] = true
	g.parent[name] = parent

	for ti := range s.Transitions {
		t := &s.Transitions[ti]
		if t.Internal {
			continue // a self-transition advances no path step
		}
		g.edges[name] = append(g.edges[name], searchEdge{
			event:     fmt.Sprint(t.On),
			eventLess: t.EventLess,
			from:      name,
			to:        fmt.Sprint(t.To),
		})
	}

	if s.InitialChild != nil {
		g.initialChildren[name] = append(g.initialChildren[name], fmt.Sprint(*s.InitialChild))
	}
	for ri := range s.Regions {
		r := &s.Regions[ri]
		if r.InitialChild != nil {
			g.initialChildren[name] = append(g.initialChildren[name], fmt.Sprint(*r.InitialChild))
		}
	}

	for i := range s.Children {
		collectSearch(&s.Children[i], name, g)
	}
	for ri := range s.Regions {
		for i := range s.Regions[ri].States {
			collectSearch(&s.Regions[ri].States[i], name, g)
		}
	}
}

// activeConfig returns the set of states active when node is entered: the node
// itself, every enclosing ancestor (each must be active for the node to be), and
// the transitive initial-descent leaves a composite or parallel node pulls in.
// This is the configuration the avoid-set is tested against — In(Y) holds when
// any member is an avoid state.
func (g searchGraph) activeConfig(node string) map[string]bool {
	cfg := map[string]bool{}

	// Ancestors: walk up the parent chain. Every enclosing composite is active.
	for n := node; n != ""; {
		cfg[n] = true
		n = g.parent[n]
	}

	// Initial descent: entering a composite/parallel state cascades into its
	// initial children, which themselves may cascade further. Collect the closure.
	queue := []string{node}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, ic := range g.initialChildren[cur] {
			if cfg[ic] {
				continue
			}
			cfg[ic] = true
			queue = append(queue, ic)
		}
	}
	return cfg
}

// forbidden reports whether entering node lands in a configuration that includes
// an avoid state — In(avoid) over node's active configuration.
func (g searchGraph) forbidden(node string, avoid map[string]bool) bool {
	if len(avoid) == 0 {
		return false
	}
	for s := range g.activeConfig(node) {
		if avoid[s] {
			return true
		}
	}
	return false
}

// reachAvoiding searches for the shortest event sequence from the initial state
// to target whose every visited configuration avoids the avoid-set. It returns
// the witnessing path and true when one exists; the zero path and false when
// every route to target must pass through an avoided configuration (or target is
// undeclared / itself avoided).
//
// The walk is breadth-first so the witness is minimal in event count; ties are
// broken by declaration order of edges, making the result deterministic. A node
// whose configuration is forbidden is never enqueued, so the search space is
// exactly the avoid-free sub-graph.
func (g searchGraph) reachAvoiding(target string, avoid map[string]bool) (analysis.Path, bool) {
	if !g.hasInitial || !g.nodes[target] {
		return analysis.Path{}, false
	}
	// The initial configuration must itself be clean, else no avoid-free run exists.
	if g.forbidden(g.initial, avoid) {
		return analysis.Path{}, false
	}
	if g.initial == target {
		return analysis.Path{Target: target}, true
	}

	type qitem struct {
		state string
		steps []analysis.Step
	}
	visited := map[string]bool{g.initial: true}
	queue := []qitem{{state: g.initial}}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, e := range g.edges[cur.state] {
			if !g.nodes[e.to] || visited[e.to] {
				continue
			}
			if g.forbidden(e.to, avoid) {
				continue // entering e.to lands in a forbidden configuration
			}
			visited[e.to] = true
			step := analysis.Step{Event: eventLabel(e), From: e.from, To: e.to}
			next := appendSearchStep(cur.steps, step)
			if e.to == target {
				return analysis.Path{Target: target, Steps: next}, true
			}
			queue = append(queue, qitem{state: e.to, steps: next})
		}
	}
	return analysis.Path{}, false
}

// eventLabel renders an edge's event for a witness step: the event name, or
// "always" for an eventless transition the machine auto-fires.
func eventLabel(e searchEdge) string {
	if e.eventLess {
		return "always"
	}
	return e.event
}

// appendSearchStep returns prev with step appended, copying so a queued path
// never aliases a sibling's buffer.
func appendSearchStep(prev []analysis.Step, step analysis.Step) []analysis.Step {
	next := make([]analysis.Step, len(prev)+1)
	copy(next, prev)
	next[len(prev)] = step
	return next
}
