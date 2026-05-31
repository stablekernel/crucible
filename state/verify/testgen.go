package verify

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// This file implements automated covering-suite generation: produce a set of
// typed event sequences that together exercise every reachable state and
// transition of a machine, so a caller can seed a conformance suite or a CI
// coverage gate from the structure alone rather than hand-authoring scenarios.
//
// The model is the same configuration-product explorer the coverage, invariant,
// and bounded checks reason over, so a suite this generates and the coverage it
// is measured against agree by construction: a transition the explorer can fire
// is a transition the suite drives, and feeding the suite back into [Coverage]
// reports 100% of the reachable universe. That round-trip is the generator's
// correctness guarantee.
//
// Greedy covering, not minimal: the algorithm grows scenarios one at a time. For
// each, it walks the shortest route from the initial configuration to a
// configuration that still has an uncovered outgoing transition, fires it, then
// keeps extending along uncovered transitions for as long as the current
// configuration offers one (and the optional length bound permits). It repeats
// until every reachable transition is covered. This yields a small suite, but it
// is explicitly NOT a provably minimal one — computing a minimum-cardinality
// covering walk is a harder optimization the generator deliberately does not
// attempt. The guarantee is coverage completeness and determinism, not
// minimality.
//
// Determinism: the explorer enumerates successors in a fixed active-leaf-then-
// edge order, the universe is walked breadth-first from the initial
// configuration, and ties are broken by that same deterministic order, so the
// emitted suite is byte-stable across runs.
//
// Eventless transitions: an eventless ("always") edge is auto-fired by the
// kernel during run-to-completion and needs no driven event, so it never appears
// in an emitted sequence; the configurations it leads to are still reached and
// covered through the events that enter them.

// CoveringSuite generates a covering suite for a Quenched machine: a set of typed
// event sequences that together exercise every reachable state and every
// reachable transition. Feed the suite to [Coverage] and the report shows full
// coverage of the reachable universe — that round-trip is the suite's guarantee.
//
// The suite is built by a greedy walk of the same configuration-product explorer
// the other checks use: scenarios are grown from the initial configuration, each
// extended to pick up not-yet-covered transitions, until none remain uncovered.
// It is a covering suite, not a provably minimal one: the generator favors a
// small, deterministic suite over a minimum-cardinality one, which is a harder
// optimization it does not attempt.
//
// The result is deterministic and stable across runs. A single-state machine, or
// any machine whose initial configuration has no outgoing transition, yields a
// single empty scenario — the initial configuration is covered with nothing to
// fire. A machine with no initial state yields an empty suite. Unreachable states
// and the transitions out of them are ignored: the suite covers the reachable
// space only, exactly the universe [Coverage] measures against.
//
// Options tune the walk: [MaxScenarioLength] caps each scenario's event count,
// splitting coverage across more, shorter scenarios. Options are additive — new
// tuning arrives as new constructors without changing this signature.
func CoveringSuite[S comparable, E comparable, C any](m *state.Machine[S, E, C], opts ...SuiteOption) [][]E {
	cfg := suiteConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	g := buildConfigGraph(m)
	resolve, ok := eventResolver(m)
	if !g.hasInitial || !ok {
		return nil
	}

	plan := coveringPlan(g, cfg)
	suite := make([][]E, 0, len(plan))
	for _, names := range plan {
		seq := make([]E, 0, len(names))
		for _, n := range names {
			ev, found := resolve[n]
			if !found {
				continue // an eventless edge carries no driven event
			}
			seq = append(seq, ev)
		}
		suite = append(suite, seq)
	}
	return suite
}

// suiteConfig is the accumulated configuration of a [CoveringSuite] run.
type suiteConfig struct {
	// maxLength caps the number of events in any one generated scenario; zero or
	// negative means unbounded.
	maxLength int
}

// SuiteOption configures a [CoveringSuite] generation. Options compose left to
// right; with no options the generator emits an unbounded greedy covering suite.
type SuiteOption func(*suiteConfig)

// MaxScenarioLength caps the number of events in any single generated scenario.
// A covering walk that would exceed the cap is split: the generator ends the
// current scenario and starts a fresh one from the initial configuration to pick
// up the remaining uncovered transitions, so the union still covers everything —
// just across more, shorter scenarios. A non-positive bound is treated as
// unbounded.
func MaxScenarioLength(n int) SuiteOption {
	return func(c *suiteConfig) { c.maxLength = n }
}

// eventResolver builds the name-to-typed-event map a covering suite uses to turn
// the explorer's string edge labels back into typed events. It walks the same
// serialized IR the explorer flattens, so the names agree. It reports false when
// the IR cannot be read, matching the no-panic contract.
func eventResolver[S comparable, E comparable, C any](m *state.Machine[S, E, C]) (map[string]E, bool) {
	ir, ok := loadIR(m)
	if !ok {
		return nil, false
	}
	out := map[string]E{}
	for i := range ir.States {
		collectEventNames(&ir.States[i], out)
	}
	return out, true
}

// collectEventNames records each non-eventless transition's name-to-typed-event
// mapping, recursing through children and region states. An eventless transition
// has no driven event, so it is skipped — its target is reached through the
// events that enter it.
func collectEventNames[S comparable, E comparable, C any](s *state.State[S, E, C], out map[string]E) {
	for ti := range s.Transitions {
		t := &s.Transitions[ti]
		if t.EventLess {
			continue
		}
		out[fmt.Sprint(t.On)] = t.On
	}
	for i := range s.Children {
		collectEventNames(&s.Children[i], out)
	}
	for ri := range s.Regions {
		for i := range s.Regions[ri].States {
			collectEventNames(&s.Regions[ri].States[i], out)
		}
	}
}

// coveringPlan computes the covering suite as event-name sequences over the
// configuration-product explorer. It first enumerates the reachable transition
// universe, then greedily grows scenarios — each routed to a configuration with
// an uncovered transition and then extended along further uncovered transitions —
// until every reachable transition is covered. A machine with no reachable
// transitions yields a single empty scenario so the initial configuration is
// still covered.
func coveringPlan(g configGraph, cfg suiteConfig) [][]string {
	universe := reachableTransitions(g)
	if len(universe) == 0 {
		return [][]string{{}} // nothing to fire; the empty scenario covers the initial config
	}

	covered := map[string]bool{}
	start := canonLeaves(g.descend(g.initial))

	var plan [][]string
	for len(covered) < len(universe) {
		seq, fired := growScenario(g, start, covered, universe, cfg.maxLength)
		gained := false
		for k := range fired {
			if !covered[k] {
				covered[k] = true
				gained = true
			}
		}
		if !gained {
			// This scenario covered no new transition: either no uncovered transition is
			// reachable within the bound, or a shared event name keeps resolving to an
			// already-covered edge (structural nondeterminism the analysis pass flags).
			// Stop rather than loop forever; the suite covers all it can reach.
			break
		}
		plan = append(plan, seq)
	}
	if len(plan) == 0 {
		return [][]string{{}}
	}
	return plan
}

// growScenario builds one scenario: route from the start configuration to the
// nearest configuration with an uncovered outgoing transition, then keep firing
// uncovered transitions for as long as the reached configuration offers one (and
// the length bound permits). It returns the event-name sequence and the set of
// transition keys that sequence fires, so the caller can fold them into the
// covered set. The fired set is consistent with what replaying the sequence over
// the same explorer (the model [Coverage] uses) would exercise. An empty fired
// set means no uncovered transition was reachable within the bound.
func growScenario(g configGraph, start []string, covered, universe map[string]bool, maxLen int) ([]string, map[string]bool) {
	prefix, cur, ok := routeToUncovered(g, start, covered, universe, maxLen)
	if !ok {
		return nil, nil
	}

	// pending tracks transitions this scenario has already fired on top of the
	// globally covered set, so a cycle (run -> paused -> run -> ...) does not refire
	// the same edge forever: once every outgoing edge of the current configuration
	// is covered or pending, the scenario ends.
	pending := map[string]bool{}
	for k := range covered {
		pending[k] = true
	}
	seq := prefix
	for k := range firedBy(g, start, seq) {
		pending[k] = true
	}
	for maxLen <= 0 || len(seq) < maxLen {
		next, advanced := extendUncovered(g, cur, pending, universe)
		if !advanced {
			break
		}
		seq = append(seq, next.event)
		pending[transitionKeyForStep(next)] = true
		cur = next.leaves
	}

	// Replay the assembled sequence from start over the explorer and record every
	// transition it fires, so the fired set matches exactly what Coverage would
	// count for this scenario — keeping the greedy covered set sound.
	return seq, firedBy(g, start, seq)
}

// routeToUncovered breadth-first searches from start for the shortest event-name
// route to a configuration that has at least one uncovered, in-universe outgoing
// transition. It returns that route, the configuration it reaches, and whether
// one was found within the optional length bound. The start configuration itself
// is checked first, so a route may be empty.
func routeToUncovered(g configGraph, start []string, covered, universe map[string]bool, maxLen int) ([]string, []string, bool) {
	if hasUncovered(g, start, covered, universe) {
		return nil, start, true
	}
	type frame struct {
		leaves []string
		seq    []string
	}
	startKey := configKey(start)
	seen := map[string]bool{startKey: true}
	queue := []frame{{leaves: start}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if maxLen > 0 && len(cur.seq) >= maxLen {
			continue // a longer route would exceed the per-scenario bound
		}
		for _, next := range g.successors(cur.leaves) {
			if !universe[transitionKeyForStep(next)] {
				continue
			}
			nextKey := configKey(next.leaves)
			if seen[nextKey] {
				continue
			}
			seen[nextKey] = true
			seq := appendEvent(cur.seq, next.event)
			if hasUncovered(g, next.leaves, covered, universe) {
				return seq, next.leaves, true
			}
			queue = append(queue, frame{leaves: next.leaves, seq: seq})
		}
	}
	return nil, nil, false
}

// extendUncovered returns the next transition to fire out of the current
// configuration toward an uncovered edge. It scans successors in the explorer's
// deterministic order for the first uncovered, in-universe transition, then
// resolves the event through matchSuccessor — the same first-match-by-event-name
// selection [Coverage] replay uses — so the transition the suite records as fired
// is exactly the one a replay of the emitted sequence would fire. When the
// first-match for that event differs from the scanned edge (two transitions share
// an event name), it returns the first-match instead, keeping firing and coverage
// in lockstep.
func extendUncovered(g configGraph, leaves []string, covered, universe map[string]bool) (configStep, bool) {
	for _, cand := range g.successors(leaves) {
		key := transitionKeyForStep(cand)
		if !universe[key] || covered[key] {
			continue
		}
		// Resolve via the same first-match selection a replay would make, so the edge
		// recorded as fired is the edge actually driven.
		fired, ok := matchSuccessor(g, leaves, cand.event)
		if !ok {
			continue
		}
		return fired, true
	}
	return configStep{}, false
}

// hasUncovered reports whether a configuration has at least one uncovered,
// in-universe outgoing transition — the test routeToUncovered searches for.
func hasUncovered(g configGraph, leaves []string, covered, universe map[string]bool) bool {
	_, ok := extendUncovered(g, leaves, covered, universe)
	return ok
}

// firedBy replays an event-name sequence from the start configuration over the
// explorer and returns the set of transition keys it fires, mirroring the
// deterministic first-match selection [Coverage] uses (so the suite's covered set
// agrees with the coverage it is measured against). An event that matches no
// enabled transition is a clean no-op, as in the kernel.
func firedBy(g configGraph, start []string, events []string) map[string]bool {
	fired := map[string]bool{}
	cur := start
	for _, ev := range events {
		next, ok := matchSuccessor(g, cur, ev)
		if !ok {
			continue
		}
		fired[transitionKeyForStep(next)] = true
		cur = next.leaves
	}
	return fired
}

// reachableTransitions enumerates the reachable transition universe over the
// configuration-product explorer: every structural edge fired between reachable
// configurations, keyed "from -event-> to". It is the same walk coverageUniverse
// performs, so the suite covers exactly the universe Coverage measures against.
func reachableTransitions(g configGraph) map[string]bool {
	universe := map[string]bool{}
	if !g.hasInitial {
		return universe
	}
	start := canonLeaves(g.descend(g.initial))
	startKey := configKey(start)
	seen := map[string][]string{startKey: start}
	queue := []string{startKey}
	for len(queue) > 0 {
		curKey := queue[0]
		queue = queue[1:]
		for _, next := range g.successors(seen[curKey]) {
			universe[transitionKeyForStep(next)] = true
			nextKey := configKey(next.leaves)
			if _, ok := seen[nextKey]; ok {
				continue
			}
			seen[nextKey] = next.leaves
			queue = append(queue, nextKey)
		}
	}
	return universe
}

// transitionKeyForStep renders a configStep as the same "from -event-> to" key
// coverage reports transitions by, so the suite's covered set and a Coverage
// report speak the same identities.
func transitionKeyForStep(s configStep) string {
	return transitionKey(s.from, s.event, s.to)
}

// appendEvent returns seq with event appended, copying so a queued route never
// aliases a sibling's buffer.
func appendEvent(seq []string, event string) []string {
	out := make([]string, len(seq)+1)
	copy(out, seq)
	out[len(seq)] = event
	return out
}
