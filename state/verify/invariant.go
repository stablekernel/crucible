package verify

import (
	"fmt"
	"sort"
	"strings"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// This file implements invariant checking: "a predicate over the active-state
// configuration holds in every reachable configuration". It is the safety half of
// the property set that reasons about whole configurations rather than single
// target states — "states A and B are never simultaneously active", "whenever X
// is active Y is active", "state S is never active".
//
// Predicates are over the active configuration of states only — pure structural
// IR, no runtime context or guard values. A guard can only ever prune an edge at
// run time, never add one, so a configuration reachable structurally is reachable
// in some run; a predicate that holds across every structural configuration holds
// in every run, and a structural violation is a real reachable violation whose
// witness drives an instance into it.
//
// Why a configuration-product explorer (not the single-node searchGraph of
// reach.go): orthogonal regions progress independently, so a parallel machine
// reaches product configurations — Exec at busy while Tele is still silent, then
// at loud — in which two leaves of different regions are co-active. The single
// node walk that powers reachability and liveness models one entered state at a
// time and cannot represent two independently-advanced region leaves being active
// together. Invariants such as mutual exclusion turn precisely on that co-activity,
// so the explorer below tracks the full set of active leaves as one configuration
// and advances one active leaf at a time, leaving orthogonal sibling regions in
// place — mirroring the kernel's parallel semantics structurally.

// Invariant is a named predicate over a reachable configuration's active-state
// set. Construct one with [MutualExclusion], [Implies], or [NeverActive] and pass
// it to [CheckInvariant]. Its [Invariant.Label] is the stable identity a [Result]
// indexes the resulting [Finding] by.
type Invariant struct {
	// label is the stable, human-readable identity of this invariant, used as the
	// Finding.State and the Result.Invariant lookup key.
	label string
	// holds reports whether the predicate is satisfied by an active-state set. The
	// set contains every active state of a configuration: each active leaf and every
	// enclosing ancestor that must be active for it.
	holds func(active map[string]bool) bool
}

// Label returns the invariant's stable identity — the key a [Result.Invariant]
// lookup uses and the name a report prints. Two invariants built from the same
// constructor and arguments share a label.
func (inv Invariant) Label() string { return inv.label }

// MutualExclusion builds the invariant "states a and b are never simultaneously
// active in any reachable configuration". It is violated by a reachable
// configuration whose active set contains both a and b — for example two leaves of
// orthogonal parallel regions that advance independently. The order of a and b
// does not affect the verdict; the label is canonicalized so MutualExclusion(a,b)
// and MutualExclusion(b,a) share an identity.
func MutualExclusion(a, b string) Invariant {
	x, y := a, b
	if y < x {
		x, y = y, x
	}
	return Invariant{
		label: fmt.Sprintf("mutex(%s,%s)", x, y),
		holds: func(active map[string]bool) bool {
			return !active[a] || !active[b]
		},
	}
}

// Implies builds the invariant "whenever antecedent is active, consequent is
// active too" — the co-presence/implication property. It is violated by a
// reachable configuration in which antecedent is active and consequent is not.
func Implies(antecedent, consequent string) Invariant {
	return Invariant{
		label: fmt.Sprintf("implies(%s=>%s)", antecedent, consequent),
		holds: func(active map[string]bool) bool {
			return !active[antecedent] || active[consequent]
		},
	}
}

// NeverActive builds the invariant "state s is never active in any reachable
// configuration". It holds when s is unreachable or otherwise never enters any
// reachable configuration's active set, and is violated by the first reachable
// configuration that activates s.
func NeverActive(s string) Invariant {
	return Invariant{
		label: fmt.Sprintf("never(%s)", s),
		holds: func(active map[string]bool) bool {
			return !active[s]
		},
	}
}

// configGraph is the configuration-product view of a machine's IR: the structural
// information needed to enumerate every reachable configuration of active leaves.
// It is built from the same serialized public IR readTopology flattens, so a
// code-built and a JSON-loaded machine explore identically.
type configGraph struct {
	// initial is the machine's initial state name; hasInitial guards the empty case.
	initial    string
	hasInitial bool
	// parent maps a state to its lexically enclosing composite/parallel state, "" at
	// top level.
	parent map[string]string
	// leaf reports whether a state is a leaf (no children, no regions): the atoms a
	// configuration is composed of.
	leaf map[string]bool
	// compoundInitial maps a compound state to its initial child.
	compoundInitial map[string]string
	// regionInitials maps a parallel state to its regions' initial children, in
	// region declaration order.
	regionInitials map[string][]string
	// edges maps a source state to its path-advancing outgoing transitions in
	// declaration order, so exploration is deterministic.
	edges map[string][]searchEdge
}

// buildConfigGraph flattens the machine's public IR into a configGraph. A machine
// whose IR cannot be read yields the zero graph (hasInitial false) rather than
// panicking, matching Verify's no-panic contract.
func buildConfigGraph[S comparable, E comparable, C any](m *state.Machine[S, E, C]) configGraph {
	g := configGraph{
		parent:          map[string]string{},
		leaf:            map[string]bool{},
		compoundInitial: map[string]string{},
		regionInitials:  map[string][]string{},
		edges:           map[string][]searchEdge{},
	}
	ir, ok := loadIR(m)
	if !ok {
		return g
	}
	if ir.HasInitial {
		g.hasInitial = true
		g.initial = fmt.Sprint(ir.Initial)
	}
	for i := range ir.States {
		collectConfig(&ir.States[i], "", &g)
	}
	return g
}

// collectConfig records one state's structure — parent, leaf-ness, compound and
// region initial children, and path-advancing edges — and recurses through its
// children and region states in declaration order.
func collectConfig[S comparable, E comparable, C any](s *state.State[S, E, C], parent string, g *configGraph) {
	name := fmt.Sprint(s.Name)
	g.parent[name] = parent
	g.leaf[name] = len(s.Children) == 0 && len(s.Regions) == 0

	for ti := range s.Transitions {
		t := &s.Transitions[ti]
		if t.Internal {
			continue // a self-transition advances no configuration step
		}
		g.edges[name] = append(g.edges[name], searchEdge{
			event:     fmt.Sprint(t.On),
			eventLess: t.EventLess,
			from:      name,
			to:        fmt.Sprint(t.To),
		})
	}

	if s.InitialChild != nil {
		g.compoundInitial[name] = fmt.Sprint(*s.InitialChild)
	}
	for ri := range s.Regions {
		r := &s.Regions[ri]
		if r.InitialChild != nil {
			g.regionInitials[name] = append(g.regionInitials[name], fmt.Sprint(*r.InitialChild))
		}
	}

	for i := range s.Children {
		collectConfig(&s.Children[i], name, g)
	}
	for ri := range s.Regions {
		for i := range s.Regions[ri].States {
			collectConfig(&s.Regions[ri].States[i], name, g)
		}
	}
}

// descend returns the leaves entered when a state becomes active: the state
// itself if it is a leaf; a compound's initial child descended recursively; or,
// for a parallel state, every region's initial child descended recursively (so
// all regions come online at once). The result is the leaf-set contribution of
// entering node.
func (g configGraph) descend(node string) []string {
	if g.leaf[node] {
		return []string{node}
	}
	if child, ok := g.compoundInitial[node]; ok {
		return g.descend(child)
	}
	var out []string
	for _, ric := range g.regionInitials[node] {
		out = append(out, g.descend(ric)...)
	}
	if len(out) == 0 {
		// A composite with neither an initial child nor regions has no descent; treat
		// the node itself as the active atom so it is not lost from a configuration.
		return []string{node}
	}
	return out
}

// activeSet expands a set of active leaves into the full active configuration:
// every leaf plus every enclosing ancestor that must be active for it. This is
// the set an invariant predicate is evaluated against.
func (g configGraph) activeSet(leaves []string) map[string]bool {
	active := map[string]bool{}
	for _, leaf := range leaves {
		for n := leaf; n != ""; n = g.parent[n] {
			active[n] = true
		}
	}
	return active
}

// region returns the parallel-region root that owns node, by walking ancestors
// until one is a parallel state (declares region initials). It returns "" when
// node is not inside any region — a flat or purely compound spine. The owning
// region root is the boundary a transition's exit clears: firing an edge out of a
// leaf exits the leaves descending from the highest ancestor strictly below the
// owning parallel state, leaving orthogonal sibling regions untouched.
func (g configGraph) parallelAncestor(node string) string {
	for n := g.parent[node]; n != ""; n = g.parent[n] {
		if len(g.regionInitials[n]) > 0 {
			return n
		}
	}
	return ""
}

// configExploration is the result of walking every reachable configuration: each
// distinct configuration (a canonical key over its active leaves) mapped to the
// shortest path that reaches it, plus that configuration's active-leaf list. The
// order slice lists configuration keys in breadth-first discovery order so a scan
// for the nearest violation is deterministic.
type configExploration struct {
	order   []string
	leaves  map[string][]string
	witness map[string]analysis.Path
}

// explore enumerates every reachable configuration breadth-first from the initial
// configuration, recording the shortest witnessing event sequence to each. A
// configuration is a set of active leaves; firing an event on one active leaf (or
// an enclosing ancestor up to its owning region) advances that leaf while
// orthogonal sibling regions stay put, exactly mirroring the kernel's parallel
// run-to-completion. The walk is deterministic: configurations are expanded in
// discovery order and each configuration's outgoing edges are tried by active-leaf
// declaration order then edge declaration order.
func (g configGraph) explore() configExploration {
	exp := configExploration{
		leaves:  map[string][]string{},
		witness: map[string]analysis.Path{},
	}
	if !g.hasInitial {
		return exp
	}
	start := canonLeaves(g.descend(g.initial))
	startKey := configKey(start)
	exp.order = append(exp.order, startKey)
	exp.leaves[startKey] = start
	exp.witness[startKey] = analysis.Path{Target: startKey}

	queue := []string{startKey}
	for len(queue) > 0 {
		curKey := queue[0]
		queue = queue[1:]
		curLeaves := exp.leaves[curKey]
		curPath := exp.witness[curKey]

		for _, next := range g.successors(curLeaves) {
			nextKey := configKey(next.leaves)
			if _, seen := exp.witness[nextKey]; seen {
				continue
			}
			step := analysis.Step{Event: next.event, From: next.from, To: next.to}
			path := analysis.Path{Target: nextKey, Steps: appendSearchStep(curPath.Steps, step)}
			exp.order = append(exp.order, nextKey)
			exp.leaves[nextKey] = next.leaves
			exp.witness[nextKey] = path
			queue = append(queue, nextKey)
		}
	}
	return exp
}

// configStep is one configuration-advancing transition discovered from a
// configuration: the event fired, the source/target leaf it advanced, and the
// resulting canonical leaf set.
type configStep struct {
	event  string
	from   string
	to     string
	leaves []string
}

// successors returns the configurations reachable from one configuration in a
// single event, in deterministic order. For each active leaf, every enabled
// transition on the leaf or an enclosing ancestor (up to but excluding the leaf's
// owning parallel state) advances the configuration: the advanced leaf and any of
// its orthogonal-internal siblings under the transition's source are replaced by
// the descent of the transition target, while leaves in other regions are
// retained.
func (g configGraph) successors(leaves []string) []configStep {
	var out []configStep
	for _, leaf := range leaves {
		// Walk the leaf and its ancestors up to (and including) the boundary just
		// below the owning parallel state, so a transition declared on a region
		// substate or on a plain compound spine is all reachable, but an edge does not
		// escape past the orthogonal boundary.
		region := g.parallelAncestor(leaf)
		for src := leaf; src != ""; src = g.parent[src] {
			for _, e := range g.edges[src] {
				exited := g.leavesUnder(src)
				retained := subtract(leaves, exited)
				entered := canonLeaves(g.descend(e.to))
				out = append(out, configStep{
					event:  eventLabel(e),
					from:   leaf,
					to:     e.to,
					leaves: canonLeaves(append(retained, entered...)),
				})
			}
			if src == region {
				break // do not climb past the owning parallel state
			}
		}
	}
	return out
}

// leavesUnder returns the set of leaves in the subtree rooted at node — the leaves
// a transition out of node exits. For a leaf it is the node itself; for a composite
// it is every descendant leaf, found by walking the parent relation (which covers
// both compound children and region states).
func (g configGraph) leavesUnder(node string) map[string]bool {
	children := g.childIndex()
	out := map[string]bool{}
	var walk func(n string)
	walk = func(n string) {
		if g.leaf[n] {
			out[n] = true
			return
		}
		for _, c := range children[n] {
			walk(c)
		}
	}
	walk(node)
	if len(out) == 0 {
		out[node] = true
	}
	return out
}

// childIndex inverts the parent map into a sorted child list per state, so a
// subtree walk is deterministic and does not range a map directly.
func (g configGraph) childIndex() map[string][]string {
	idx := map[string][]string{}
	for name, p := range g.parent {
		if p != "" {
			idx[p] = append(idx[p], name)
		}
	}
	for p := range idx {
		sort.Strings(idx[p])
	}
	return idx
}

// invariantFor decides a single invariant over an already-built exploration,
// returning the finding. The exploration is walked in breadth-first discovery
// order so the reported counterexample is the nearest violating configuration,
// making the witness short and the result deterministic.
func invariantFor(g configGraph, exp configExploration, inv Invariant) Finding {
	for _, key := range exp.order {
		active := g.activeSet(exp.leaves[key])
		if !inv.holds(active) {
			return Finding{
				Kind:      KindInvariant,
				State:     inv.label,
				Reachable: false,
				Witness:   exp.witness[key],
			}
		}
	}
	return Finding{
		Kind:      KindInvariant,
		State:     inv.label,
		Reachable: true,
	}
}

// canonLeaves returns the sorted, de-duplicated leaf set, the canonical form a
// configuration is keyed and compared by.
func canonLeaves(leaves []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(leaves))
	for _, l := range leaves {
		if seen[l] {
			continue
		}
		seen[l] = true
		out = append(out, l)
	}
	sort.Strings(out)
	return out
}

// configKey joins a canonical leaf set into a stable configuration identity, the
// name a counterexample's Witness.Target carries.
func configKey(leaves []string) string {
	return strings.Join(leaves, "|")
}

// subtract returns the members of leaves not present in remove.
func subtract(leaves []string, remove map[string]bool) []string {
	out := make([]string, 0, len(leaves))
	for _, l := range leaves {
		if !remove[l] {
			out = append(out, l)
		}
	}
	return out
}
