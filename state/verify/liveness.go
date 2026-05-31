package verify

import (
	"github.com/stablekernel/crucible/state/analysis"
)

// This file implements liveness: "from every reachable configuration, the target
// state Z is always eventually reachable" — the CTL eventuality AG EF Z. It is
// the liveness half of the CTL/LTL-lite property set, the dual of the safety
// (reach-avoiding) check in reach.go.
//
// The property holds when there is no reachable configuration that is stuck in a
// Z-free region: every configuration an instance can reach must retain some run
// that still reaches Z. It is refuted by a counterexample — a concrete reachable
// configuration from which Z can never be reached, i.e. a configuration parked in
// a Z-free terminal or a Z-free cycle.
//
// Terminal and cycle semantics:
//   - A terminal configuration (a final state, or a state with no path-advancing
//     exit) that is not Z is a violation: an instance there has completed without
//     ever reaching Z, and never will.
//   - A Z-free cycle — a strongly-connected region none of whose configurations
//     has an edge toward Z — is a violation: an instance circulating in it can
//     loop forever without progressing to Z.
//
// Both fall out of a single computation. Let towardZ be the set of states from
// which Z is reachable over the structural graph (reverse reachability from Z).
// A reachable state NOT in towardZ is a counterexample: no run from it reaches Z,
// whether because it is a Z-free terminal or because every edge out of it stays
// inside a Z-free region. The check is exact in the same guard-agnostic sense as
// reachability: a guard can only ever prune an edge at run time, never add one,
// so a state from which the structural graph offers no route to Z has no run to Z
// in any instance, and a "holds" verdict means every reachable state retains a
// structural route to Z.
//
// The reported counterexample is the nearest stuck configuration: the
// breadth-first-minimal event sequence from the initial state to a reachable
// state outside towardZ. Reporting the nearest one makes the witness short and
// the result deterministic (ties broken by edge declaration order), and it is
// drivable — replaying its events lands an instance in the stuck configuration.

// liveness decides whether target is always eventually reachable from every
// reachable configuration. It returns the holding verdict and, when the property
// fails, the path to the nearest reachable configuration from which target can
// never be reached. A holding verdict carries the zero path; an undeclared target
// (caller-filtered before this point) is treated as a violation with the empty
// witness only if it is the initial configuration, matching the no-route case.
func (g searchGraph) liveness(target string) (analysis.Path, bool) {
	if !g.hasInitial || !g.nodes[target] {
		return analysis.Path{}, false
	}

	towardZ := g.reachesTarget(target)

	// Walk the reachable space breadth-first from the initial state; the first
	// reachable state that cannot reach target is the nearest counterexample.
	type qitem struct {
		state string
		steps []analysis.Step
	}
	if !towardZ[g.initial] {
		// The initial configuration itself cannot reach target.
		return analysis.Path{Target: g.initial}, false
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
			visited[e.to] = true
			step := analysis.Step{Event: eventLabel(e), From: e.from, To: e.to}
			next := appendSearchStep(cur.steps, step)
			if !towardZ[e.to] {
				// e.to is reachable but cannot reach target: the nearest counterexample.
				return analysis.Path{Target: e.to, Steps: next}, false
			}
			queue = append(queue, qitem{state: e.to, steps: next})
		}
	}
	// Every reachable state can still reach target: the property holds.
	return analysis.Path{}, true
}

// reachesTarget returns the set of states from which target is reachable over the
// structural graph — reverse reachability from target. It mirrors the forward
// reachability the analysis package proves: a state reaches target when some
// sequence of path-advancing edges and structural entries (entering a composite
// descends into its initial children; reaching a substate activates its enclosing
// parent) leads from it to target. target itself is always in the set.
//
// Honoring structural entry is essential for hierarchy: a target entered by
// initial descent (a region's initial child) has no firing edge of its own, so a
// path-advancing-edge-only reverse walk would wrongly conclude no configuration
// reaches it. Mirroring the parent/initial-descent implications — the same ones
// analysis.canReachFinal uses — makes liveness exact for compound and parallel
// machines.
func (g searchGraph) reachesTarget(target string) map[string]bool {
	// Reverse adjacency: rev[to] lists the sources from which entering to follows.
	rev := map[string][]string{}
	addRev := func(from, to string) {
		if !g.nodes[to] {
			return
		}
		rev[to] = append(rev[to], from)
	}
	// Path-advancing edges: from can step to e.to.
	for from, es := range g.edges {
		for _, e := range es {
			addRev(from, e.to)
		}
	}
	// Structural implications, matching forward reachability:
	//   - entering a composite/parallel state enters each initial-descent child;
	//   - reaching a substate activates its enclosing parent.
	for name := range g.nodes {
		for _, ic := range g.initialChildren[name] {
			addRev(name, ic)
		}
		if p := g.parent[name]; p != "" {
			addRev(name, p)
		}
	}

	can := map[string]bool{target: true}
	queue := []string{target}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, pred := range rev[cur] {
			if !can[pred] {
				can[pred] = true
				queue = append(queue, pred)
			}
		}
	}
	return can
}

// reachableSet returns the set of states reachable from the initial state over
// the searchGraph's path-advancing edges and structural entry (initial descent,
// ancestor activation). It is the model verify explores, surfaced so a fidelity
// test can assert it agrees with the analysis package's proven reachability.
func (g searchGraph) reachableSet() map[string]bool {
	seen := map[string]bool{}
	if !g.hasInitial {
		return seen
	}
	var queue []string
	visit := func(name string) {
		if !g.nodes[name] || seen[name] {
			return
		}
		seen[name] = true
		queue = append(queue, name)
	}
	// The initial configuration brings its whole active config online (the initial
	// state, its initial descent, and the ancestors it activates).
	for s := range g.activeConfig(g.initial) {
		visit(s)
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, e := range g.edges[cur] {
			if !g.nodes[e.to] {
				continue
			}
			// Entering e.to brings its whole active config online.
			for s := range g.activeConfig(e.to) {
				visit(s)
			}
		}
	}
	return seen
}

// livenessFor decides a liveness check for a single target over an already-built
// search graph, returning the finding and whether the target is a declared state
// (an undeclared target yields no finding, matching Reachable). It is kept beside
// the searchGraph method so Verify can build one graph and decide many targets.
func livenessFor(g searchGraph, target string) (Finding, bool) {
	if !g.nodes[target] {
		return Finding{}, false
	}
	w, holds := g.liveness(target)
	return Finding{
		Kind:      KindLiveness,
		State:     target,
		Reachable: holds,
		Witness:   w,
	}, true
}
