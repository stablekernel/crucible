package symbolic

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// Overlap reports two competing transitions — the same source state firing on the
// same event — whose guards are not provably disjoint, so both can be enabled at
// once: a candidate nondeterminism the analyzer could not rule out.
type Overlap[S comparable, E comparable] struct {
	// From is the shared source state, On the shared event.
	From S
	On   E
	// ToA and ToB are the two competing transitions' targets.
	ToA S
	ToB S
}

// Overlaps scans a machine for competing transitions (same source, same event)
// whose guards are not provably disjoint. It is conservative: a pair is reported
// unless the analyzer can PROVE the guards mutually exclusive, so transitions
// guarded only by opaque guards (named Go-func or Rich) — or unguarded — are
// reported as potential overlaps. An empty result means every same-source/same-event
// pair is provably disjoint, i.e. the machine is provably deterministic on its
// guarded choices over the analyzable Core guards.
func Overlaps[S comparable, E comparable, C any](m *state.Machine[S, E, C]) ([]Overlap[S, E], error) {
	js, err := m.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("symbolic: serialize machine: %w", err)
	}
	ir, err := state.LoadFromJSON[S, E, C](js)
	if err != nil {
		return nil, fmt.Errorf("symbolic: load machine IR: %w", err)
	}

	var schema state.ContextSchema
	if ir.Context != nil {
		schema = *ir.Context
	}

	// Collect every state, descending into compound children and parallel regions.
	var states []state.State[S, E, C]
	var collect func(ss []state.State[S, E, C])
	collect = func(ss []state.State[S, E, C]) {
		for i := range ss {
			states = append(states, ss[i])
			collect(ss[i].Children)
			for r := range ss[i].Regions {
				collect(ss[i].Regions[r].States)
			}
		}
	}
	collect(ir.States)

	type key struct {
		from S
		on   E
	}
	var overlaps []Overlap[S, E]
	for si := range states {
		// Group same-source/same-event transitions, tracking each key's first-seen
		// order so the scan is deterministic: ranging the map directly would emit
		// overlaps in random order across runs and break golden/idempotency checks.
		groups := map[key][]state.Transition[S, E, C]{}
		var order []key
		for _, t := range states[si].Transitions {
			k := key{from: t.From, on: t.On}
			if _, seen := groups[k]; !seen {
				order = append(order, k)
			}
			groups[k] = append(groups[k], t)
		}
		for _, k := range order {
			ts := groups[k]
			for i := 0; i < len(ts); i++ {
				for j := i + 1; j < len(ts); j++ {
					if !Disjoint(transitionGuard(ts[i]), transitionGuard(ts[j]), schema) {
						overlaps = append(overlaps, Overlap[S, E]{From: k.from, On: k.on, ToA: ts[i].To, ToB: ts[j].To})
					}
				}
			}
		}
	}
	return overlaps, nil
}

// transitionGuard builds the effective guard of a transition: the conjunction of its
// named-ref guards (each an opaque leaf to the analyzer) and its composite GuardExpr.
// A transition with no guard yields the empty conjunction, which the analyzer treats
// as always-true (the transition is always enabled).
func transitionGuard[S comparable, E comparable, C any](t state.Transition[S, E, C]) state.GuardNode[S] {
	var nodes []state.GuardNode[S]
	for _, g := range t.Guards {
		nodes = append(nodes, state.Guard[S](g.Name))
	}
	if t.GuardExpr != nil {
		nodes = append(nodes, *t.GuardExpr)
	}
	return state.And(nodes...)
}
