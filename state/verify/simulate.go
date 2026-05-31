package verify

import (
	"github.com/stablekernel/crucible/state/analysis"
)

// This file implements bounded exhaustive simulation: enumerate the event
// sequences (traces) the machine can run from its initial configuration up to a
// depth bound, evaluate a caller-supplied oracle at every reached configuration,
// and surface the shortest trace that drives the machine into a configuration the
// oracle rejects.
//
// Where invariant checking (invariant.go) decides a fixed catalog of structural
// predicates (mutual exclusion, implication, never-active), bounded simulation
// lets a caller express an arbitrary property as a Go predicate over the active
// configuration and search the bounded reachable space for a concrete
// counterexample. It reuses the same configuration-product explorer the invariant
// check is built on — the canonical model of the reachable configuration space —
// so a parallel machine's orthogonal regions advance independently and the oracle
// sees the true co-active leaf sets.
//
// What "bounded" means precisely: the search explores every configuration
// reachable in at most depth events, where depth is the number of transitions
// fired (an eventless transition counts as a step, exactly as a Witness records
// it). Because the product explorer canonicalizes configurations and never
// re-expands one already discovered at an equal-or-shorter depth, the search is
// finite and the trace it reports for any reached configuration is the shortest
// event sequence to it. The initial configuration is evaluated at depth 0 before
// any event is fired.
//
// What "no violation up to depth N" does NOT mean: it is NOT a proof that the
// property holds in every run. A violation may exist at depth N+1, in a longer
// trace, or in a configuration the bound did not reach. A clean bounded-simulation
// finding is a bounded guarantee only — it states the oracle held across every
// configuration reachable within the bound, nothing more. For an exact, unbounded
// verdict over the fixed structural predicates use [CheckInvariant], which
// explores the whole (finite) reachable configuration space.
//
// Reporting: the first violation encountered in breadth-first discovery order is
// reported — the configuration reachable by the shortest trace (ties broken by
// the explorer's deterministic active-leaf-then-edge declaration order). One
// finding is produced per oracle, carrying that single shortest violating trace,
// so the report is small and deterministic rather than an unbounded list of every
// violating trace.

// Oracle is a caller-supplied predicate over a reached configuration's
// active-state set, evaluated by [SimulateBounded] at every configuration the
// bounded search reaches. It receives the full active configuration — every active
// leaf plus every enclosing ancestor that must be active for it, the same set the
// invariant predicates see — and reports whether the property HOLDS there: return
// true when the configuration is acceptable, false to flag it as a violation.
//
// An Oracle must be a pure function of its argument: bounded simulation evaluates
// it many times across the reachable space and reuses the verdict, so a
// side-effecting or nondeterministic oracle breaks the determinism guarantee.
type Oracle func(active map[string]bool) bool

// simQuery is one bounded-simulation request: search to depth for the shortest
// trace whose reached configuration the oracle rejects, reported under label.
type simQuery struct {
	// label is the stable identity the resulting Finding is keyed by.
	label string
	// depth is the maximum number of events a searched trace may fire; the initial
	// configuration is evaluated at depth 0.
	depth int
	// oracle reports whether the property holds at a reached configuration.
	oracle Oracle
}

// boundedSimFor runs one bounded simulation over an already-built configuration
// graph and returns the finding. The search is a depth-bounded breadth-first walk
// of the configuration-product space: the initial configuration is evaluated
// first, then every configuration reachable within depth events, in discovery
// order. The first configuration the oracle rejects yields a violation finding
// carrying the shortest trace to it; if none is rejected within the bound the
// finding holds with the zero witness.
func boundedSimFor(g configGraph, q simQuery) Finding {
	hold := Finding{Kind: KindBoundedViolation, State: q.label, Reachable: true}
	if !g.hasInitial {
		return hold
	}

	start := canonLeaves(g.descend(g.initial))
	startKey := configKey(start)
	startPath := analysis.Path{Target: startKey}

	// Evaluate the initial configuration at depth 0 before firing any event.
	if !q.oracle(g.activeSet(start)) {
		return Finding{Kind: KindBoundedViolation, State: q.label, Reachable: false, Witness: startPath}
	}
	if q.depth <= 0 {
		return hold
	}

	type frame struct {
		key    string
		leaves []string
		path   analysis.Path
		depth  int
	}
	// seen records the shallowest depth a configuration was discovered at, so a
	// configuration is expanded once and its reported trace is the shortest. The
	// walk is breadth-first, so the first discovery is already the shortest trace.
	seen := map[string]bool{startKey: true}
	queue := []frame{{key: startKey, leaves: start, path: startPath, depth: 0}}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.depth >= q.depth {
			continue // firing another event would exceed the bound
		}
		for _, next := range g.successors(cur.leaves) {
			nextKey := configKey(next.leaves)
			if seen[nextKey] {
				continue
			}
			seen[nextKey] = true
			step := analysis.Step{Event: next.event, From: next.from, To: next.to}
			path := analysis.Path{Target: nextKey, Steps: appendSearchStep(cur.path.Steps, step)}
			if !q.oracle(g.activeSet(next.leaves)) {
				return Finding{Kind: KindBoundedViolation, State: q.label, Reachable: false, Witness: path}
			}
			queue = append(queue, frame{key: nextKey, leaves: next.leaves, path: path, depth: cur.depth + 1})
		}
	}
	return hold
}
