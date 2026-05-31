package verify

import (
	"fmt"
	"sort"
	"strings"
)

// This file implements structural coverage analysis: given a set of scenarios
// (ordered event sequences), report which reachable states and transitions the
// scenarios exercise, against the universe of reachable states and transitions
// the machine declares — and the concrete gap they leave uncovered.
//
// The model is the same configuration-product explorer the invariant and bounded
// checks reason over, so the coverage metric is consistent with the rest of
// verify. The universe is guard-agnostic: a state or transition is "reachable" if
// the structural explorer can reach or fire it assuming guards pass, because a
// guard can only ever prune an edge at run time, never add one. Coverage measures
// a scenario set against that same structural universe, so a state or transition
// reported uncovered is a real gap the scenarios leave unexercised — not an
// artifact of a different reachability model.
//
// What a scenario exercises: replaying a scenario drives the explorer from the
// initial configuration, firing the structural edge each event names. The states
// it enters are the active configurations the run visits (every active leaf plus
// its enclosing ancestors); the transitions it fires are the structural edges it
// traverses. An event that names no enabled transition from the current
// configuration is a clean no-op — it advances nothing and is neither an error nor
// counted — mirroring a kernel Fire of an unhandled event, which is ignored.
//
// Transition identity: a transition is keyed "from -on-> to" (the same rendering
// the analysis package uses), so a reported transition reads as the declared edge
// it names regardless of which configuration fired it.

// CoverageReport is the structural-coverage breakdown a [Coverage] pass produces:
// which reachable states and transitions a scenario set exercises, the universe it
// is measured against, and the concrete uncovered remainder. Read it from a
// [Result] with [Result.Coverage]. The zero report describes a machine with an
// empty universe — 100%-covered by convention, since there is nothing to cover.
type CoverageReport struct {
	// CoveredStates are the reachable states the scenarios entered, sorted.
	CoveredStates []string
	// UncoveredStates are the reachable states no scenario entered, sorted — the
	// state gap a scenario set leaves.
	UncoveredStates []string
	// CoveredTransitions are the reachable transitions the scenarios fired, each
	// rendered "from -on-> to", sorted.
	CoveredTransitions []string
	// UncoveredTransitions are the reachable transitions no scenario fired, each
	// rendered "from -on-> to", sorted — the transition gap a scenario set leaves.
	UncoveredTransitions []string
}

// StateCoverage returns the fraction of reachable states the scenarios entered, in
// the range [0,1]. A machine with no reachable states is fully covered (1) by
// convention, since there is nothing left to cover.
func (c CoverageReport) StateCoverage() float64 {
	return fraction(len(c.CoveredStates), len(c.CoveredStates)+len(c.UncoveredStates))
}

// TransitionCoverage returns the fraction of reachable transitions the scenarios
// fired, in the range [0,1]. A machine with no reachable transitions is fully
// covered (1) by convention.
func (c CoverageReport) TransitionCoverage() float64 {
	return fraction(len(c.CoveredTransitions), len(c.CoveredTransitions)+len(c.UncoveredTransitions))
}

// String renders the coverage breakdown as two labeled lines — states and
// transitions — each with its covered/total count, percentage, and the uncovered
// remainder, so a report is human-readable and diffable.
func (c CoverageReport) String() string {
	sTot := len(c.CoveredStates) + len(c.UncoveredStates)
	tTot := len(c.CoveredTransitions) + len(c.UncoveredTransitions)
	var b strings.Builder
	fmt.Fprintf(&b, "states: %d/%d (%s)", len(c.CoveredStates), sTot, percent(c.StateCoverage()))
	if len(c.UncoveredStates) > 0 {
		fmt.Fprintf(&b, "; uncovered %v", c.UncoveredStates)
	}
	fmt.Fprintf(&b, "\ntransitions: %d/%d (%s)", len(c.CoveredTransitions), tTot, percent(c.TransitionCoverage()))
	if len(c.UncoveredTransitions) > 0 {
		fmt.Fprintf(&b, "; uncovered %v", c.UncoveredTransitions)
	}
	return b.String()
}

// fraction returns covered/total as a fraction in [0,1], treating an empty
// universe (total 0) as fully covered.
func fraction(covered, total int) float64 {
	if total == 0 {
		return 1
	}
	return float64(covered) / float64(total)
}

// percent renders a [0,1] fraction as a whole-or-tenth-of-a-percent string, so a
// report reads "100.0%" / "66.7%" deterministically.
func percent(f float64) string {
	return fmt.Sprintf("%.1f%%", f*100)
}

// transitionKey renders a structural edge as the stable "from -on-> to" identity a
// covered/uncovered transition is reported by, matching the analysis package's
// edge label.
func transitionKey(from, on, to string) string {
	return fmt.Sprintf("%s -%s-> %s", from, on, to)
}

// coverageFor computes the structural coverage of a scenario set over an
// already-built configuration graph. It first enumerates the reachable universe —
// every state active in some reachable configuration and every structural edge
// fired between reachable configurations — then replays each scenario over the
// same explorer to record the states and transitions it exercises, and reports the
// covered set and the uncovered remainder.
func coverageFor(g configGraph, scenarios [][]string) Finding {
	universeStates, universeTransitions := coverageUniverse(g)

	enteredStates := map[string]bool{}
	firedTransitions := map[string]bool{}
	if g.hasInitial {
		// Every scenario starts in the initial configuration, so its states are
		// entered before any event is replayed — an empty scenario still covers the
		// initial configuration.
		start := canonLeaves(g.descend(g.initial))
		for s := range g.activeSet(start) {
			enteredStates[s] = true
		}
		for _, sc := range scenarios {
			replayScenario(g, start, sc, enteredStates, firedTransitions)
		}
	}

	report := CoverageReport{
		CoveredStates:        intersect(universeStates, enteredStates),
		UncoveredStates:      difference(universeStates, enteredStates),
		CoveredTransitions:   intersect(universeTransitions, firedTransitions),
		UncoveredTransitions: difference(universeTransitions, firedTransitions),
	}
	return Finding{
		Kind:      KindCoverage,
		State:     coverageLabel,
		Reachable: len(report.UncoveredStates) == 0 && len(report.UncoveredTransitions) == 0,
		coverage:  &report,
	}
}

// coverageUniverse enumerates the reachable structural universe: the set of states
// active in some reachable configuration and the set of structural edges fired
// between reachable configurations, each as a sorted key list. It walks the same
// breadth-first configuration-product exploration the other checks use, so the
// universe is exactly the reachable space verify reasons over.
func coverageUniverse(g configGraph) (states, transitions []string) {
	stateSet := map[string]bool{}
	transitionSet := map[string]bool{}
	if !g.hasInitial {
		return nil, nil
	}

	start := canonLeaves(g.descend(g.initial))
	startKey := configKey(start)
	seen := map[string][]string{startKey: start}
	queue := []string{startKey}
	for s := range g.activeSet(start) {
		stateSet[s] = true
	}

	for len(queue) > 0 {
		curKey := queue[0]
		queue = queue[1:]
		for _, next := range g.successors(seen[curKey]) {
			transitionSet[transitionKey(next.from, next.event, next.to)] = true
			nextKey := configKey(next.leaves)
			if _, ok := seen[nextKey]; ok {
				continue
			}
			seen[nextKey] = next.leaves
			for s := range g.activeSet(next.leaves) {
				stateSet[s] = true
			}
			queue = append(queue, nextKey)
		}
	}
	return coverageKeys(stateSet), coverageKeys(transitionSet)
}

// replayScenario drives the explorer through one scenario's event sequence from
// the start configuration, recording the states entered and transitions fired. For
// each event it fires the first matching enabled transition from the current
// configuration (deterministic active-leaf-then-edge order); an event that matches
// no enabled transition is a clean no-op that leaves the configuration unchanged.
func replayScenario(g configGraph, start []string, events []string, enteredStates, firedTransitions map[string]bool) {
	cur := start
	for _, ev := range events {
		next, ok := matchSuccessor(g, cur, ev)
		if !ok {
			continue // unhandled event from this configuration: a no-op, as the kernel ignores it
		}
		firedTransitions[transitionKey(next.from, next.event, next.to)] = true
		for s := range g.activeSet(next.leaves) {
			enteredStates[s] = true
		}
		cur = next.leaves
	}
}

// matchSuccessor returns the configuration reached by firing event from the
// current configuration, and whether any enabled transition matched. It selects the
// first successor whose triggering event equals the named event, in the explorer's
// deterministic active-leaf-then-edge order, mirroring the kernel's run-to-
// completion choice of the first enabled transition.
func matchSuccessor(g configGraph, leaves []string, event string) (configStep, bool) {
	for _, next := range g.successors(leaves) {
		if next.event == event {
			return next, true
		}
	}
	return configStep{}, false
}

// coverageKeys returns the keys of a set in sorted order, the canonical form a
// covered/uncovered list is reported in.
func coverageKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// intersect returns the members of the sorted universe that are present in have,
// preserving the universe's sorted order — the covered subset.
func intersect(universe []string, have map[string]bool) []string {
	var out []string
	for _, k := range universe {
		if have[k] {
			out = append(out, k)
		}
	}
	return out
}

// difference returns the members of the sorted universe absent from have,
// preserving the universe's sorted order — the uncovered remainder.
func difference(universe []string, have map[string]bool) []string {
	var out []string
	for _, k := range universe {
		if !have[k] {
			out = append(out, k)
		}
	}
	return out
}
