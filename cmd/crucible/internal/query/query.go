// Package query is a pure graph layer over the crucible state IR. It answers
// reachability and path questions used to scope a machine visualization.
//
// It performs no rendering and depends only on the public state package. The
// node-identity scheme matches viewmodel: a node ID is a state's bare Name, so
// selections produced here line up with viewmodel.ViewNode.ID. Traversal is a
// small local BFS/DFS over the public IR — it deliberately does not reuse the
// unexported graph builders in state/analysis or state/verify, which are
// initial-state-only and not exported.
package query

import (
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// Sentinel errors for endpoint resolution, wrapped with the offending name.
var (
	// ErrUnknownState is returned when a name matches no state in the IR.
	ErrUnknownState = errors.New("unknown state")
	// ErrAmbiguousState is returned when a bare name matches more than one
	// state (the same leaf name under different parents).
	ErrAmbiguousState = errors.New("ambiguous state name")
)

// Step is one edge of a path: a transition (or implicit descent) from one node
// to another. Event is the transition's On value, or "" for an implicit
// descent edge into a composite/parallel's initial child.
type Step struct {
	From  string
	To    string
	Event string
}

// Path is an ordered sequence of steps from a source node to a target node.
type Path []Step

// edge is an internal adjacency entry: a destination node ID and the event
// that labels the edge ("" for an implicit descent edge).
type edge struct {
	to    string
	event string
}

// graph is a flattened, name-keyed view of the IR for traversal. nodes is the
// set of all state IDs; adj maps each node to its outgoing edges in document
// order (transitions first, then descent edges).
type graph struct {
	nodes map[string]struct{}
	adj   map[string][]edge
}

// buildGraph flattens the IR into a name-keyed adjacency graph. Every state
// (at any depth, including children and region states) becomes a node. Edges
// are the state's own transitions (From->To, labeled by On) plus implicit
// "descent" edges: a composite emits an edge to its InitialChild, and a
// parallel emits one descent edge per region to that region's initial state.
// Descent edges model "entering a composite/parallel reaches its initials",
// consistent with how the IR nests, and carry an empty event label.
func buildGraph(ir *state.IR[string, string, any]) *graph {
	g := &graph{
		nodes: make(map[string]struct{}),
		adj:   make(map[string][]edge),
	}
	if ir == nil {
		return g
	}
	for i := range ir.States {
		addState(g, &ir.States[i])
	}
	return g
}

// addState records one state and its outgoing edges, then recurses into
// children and region states.
func addState(g *graph, s *state.State[string, string, any]) {
	g.nodes[s.Name] = struct{}{}

	for i := range s.Transitions {
		t := &s.Transitions[i]
		g.adj[s.Name] = append(g.adj[s.Name], edge{to: t.To, event: t.On})
	}

	// Descent into a composite's initial child.
	if len(s.Children) > 0 && s.InitialChild != nil {
		g.adj[s.Name] = append(g.adj[s.Name], edge{to: *s.InitialChild})
	}
	// Descent into each region's initial state.
	for i := range s.Regions {
		r := &s.Regions[i]
		if r.InitialChild != nil {
			g.adj[s.Name] = append(g.adj[s.Name], edge{to: *r.InitialChild})
		}
	}

	for i := range s.Children {
		addState(g, &s.Children[i])
	}
	for i := range s.Regions {
		r := &s.Regions[i]
		for j := range r.States {
			addState(g, &r.States[j])
		}
	}
}

// countByName tallies how many states across the whole IR carry each bare
// Name. Used to detect ambiguity in endpoint resolution.
func countByName(ir *state.IR[string, string, any]) map[string]int {
	counts := make(map[string]int)
	if ir == nil {
		return counts
	}
	var walk func(s *state.State[string, string, any])
	walk = func(s *state.State[string, string, any]) {
		counts[s.Name]++
		for i := range s.Children {
			walk(&s.Children[i])
		}
		for i := range s.Regions {
			for j := range s.Regions[i].States {
				walk(&s.Regions[i].States[j])
			}
		}
	}
	for i := range ir.States {
		walk(&ir.States[i])
	}
	return counts
}

// ResolveEndpoint maps a user-given endpoint name to a node ID using the same
// identity scheme as viewmodel (a node ID is a state's bare Name).
//
// Rule: the name is matched against every state's bare Name across the whole
// tree (leaves, composites, parallels, region states alike). It resolves to
// that same name when exactly one state carries it. Composite and parallel
// names are valid endpoints and resolve to themselves (their container node
// ID); callers that want the "inside" of a composite can rely on the descent
// edges added by buildGraph. The name is rejected with ErrUnknownState when no
// state carries it, and with ErrAmbiguousState when more than one does.
func ResolveEndpoint(ir *state.IR[string, string, any], name string) (string, error) {
	counts := countByName(ir)
	switch counts[name] {
	case 0:
		return "", fmt.Errorf("%w: %q", ErrUnknownState, name)
	case 1:
		return name, nil
	default:
		return "", fmt.Errorf("%w: %q matches %d states", ErrAmbiguousState, name, counts[name])
	}
}

// ReachableFrom returns the set of node IDs reachable from rootID via a local
// BFS over transition and descent edges (compound/parallel-aware). The root
// itself is included. The root must resolve to exactly one state, else an
// error is returned (ErrUnknownState / ErrAmbiguousState).
func ReachableFrom(ir *state.IR[string, string, any], rootID string) (map[string]bool, error) {
	root, err := ResolveEndpoint(ir, rootID)
	if err != nil {
		return nil, err
	}
	g := buildGraph(ir)

	seen := map[string]bool{root: true}
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, e := range g.adj[cur] {
			if !seen[e.to] {
				seen[e.to] = true
				queue = append(queue, e.to)
			}
		}
	}
	return seen, nil
}

// ShortestPath returns the shortest path (fewest edges) from fromID to toID via
// BFS over the IR graph. found is false (not an error) when no path exists.
// Unknown or ambiguous endpoints return an error. A from==to request yields an
// empty path with found=true.
func ShortestPath(ir *state.IR[string, string, any], fromID, toID string) (Path, bool, error) {
	from, err := ResolveEndpoint(ir, fromID)
	if err != nil {
		return nil, false, err
	}
	to, err := ResolveEndpoint(ir, toID)
	if err != nil {
		return nil, false, err
	}
	if from == to {
		return Path{}, true, nil
	}
	g := buildGraph(ir)

	// BFS recording the predecessor step that first reached each node.
	prev := map[string]Step{}
	seen := map[string]bool{from: true}
	queue := []string{from}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, e := range g.adj[cur] {
			if seen[e.to] {
				continue
			}
			seen[e.to] = true
			prev[e.to] = Step{From: cur, To: e.to, Event: e.event}
			if e.to == to {
				return reconstruct(prev, from, to), true, nil
			}
			queue = append(queue, e.to)
		}
	}
	return nil, false, nil
}

// reconstruct walks predecessor steps from to back to from, returning the path
// in forward order.
func reconstruct(prev map[string]Step, from, to string) Path {
	var rev Path
	for cur := to; cur != from; {
		st := prev[cur]
		rev = append(rev, st)
		cur = st.From
	}
	// Reverse into forward order.
	out := make(Path, len(rev))
	for i := range rev {
		out[len(rev)-1-i] = rev[i]
	}
	return out
}

// AllSimplePaths enumerates all acyclic (simple) paths from fromID to toID via
// a local DFS, capped at limit paths. truncated is true when the cap was hit
// and further paths existed but were not enumerated (paths are never silently
// dropped — the caller learns the result is partial). limit must be positive.
// Unknown or ambiguous endpoints return an error.
func AllSimplePaths(ir *state.IR[string, string, any], fromID, toID string, limit int) ([]Path, bool, error) {
	if limit <= 0 {
		return nil, false, fmt.Errorf("cap must be positive, got %d", limit)
	}
	from, err := ResolveEndpoint(ir, fromID)
	if err != nil {
		return nil, false, err
	}
	to, err := ResolveEndpoint(ir, toID)
	if err != nil {
		return nil, false, err
	}
	g := buildGraph(ir)

	// Enumerate up to limit+1 paths: finding the (limit+1)-th proves the result
	// is truncated, and we then trim back to limit so paths are never silently
	// dropped — the caller learns via truncated=true.
	var paths []Path
	onPath := map[string]bool{from: true}
	var cur Path

	var dfs func(node string)
	dfs = func(node string) {
		if len(paths) > limit {
			return // already proved truncation; stop early
		}
		if node == to && len(cur) > 0 {
			paths = append(paths, append(Path(nil), cur...))
			return
		}
		for _, e := range g.adj[node] {
			if onPath[e.to] {
				continue // skip to keep the path simple (acyclic)
			}
			onPath[e.to] = true
			cur = append(cur, Step{From: node, To: e.to, Event: e.event})
			dfs(e.to)
			cur = cur[:len(cur)-1]
			onPath[e.to] = false
			if len(paths) > limit {
				return
			}
		}
	}
	dfs(from)

	truncated := len(paths) > limit
	if truncated {
		paths = paths[:limit]
	}
	return paths, truncated, nil
}
