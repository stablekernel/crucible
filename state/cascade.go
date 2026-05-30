package state

import "context"

// cascade computes the exit and entry sets for a transition from the source
// leaf `from` to the target `to`, following standard statecharts semantics:
//
//  1. Find the Least Common Ancestor (LCA) of source and target.
//  2. Exit from the source leaf up to (but not including) the LCA — innermost
//     first.
//  3. Enter from the LCA down to the target — outermost first; if the target is
//     compound or parallel, the walk continues transitively into its initial
//     children (every region's initial child for a parallel target).
//
// For a flat machine (no parents), exits == [from] and entries == [to], i.e. the
// flat behavior.
func (m *Machine[S, E, C]) cascade(from, to S) (exits, entries []S) {
	lca, hasLCA := m.lca(from, to)

	// Exits: from-chain innermost-first, stopping below the LCA.
	for _, s := range m.ancestors(from) {
		if hasLCA && s == lca {
			break
		}
		exits = append(exits, s)
	}

	// Entries: the to-chain outermost-first, stopping below the LCA, then the
	// descent into the target's initial children.
	toChain := m.ancestors(to) // innermost-first: [to, parent, ... root]
	var enterChain []S
	for _, s := range toChain {
		if hasLCA && s == lca {
			break
		}
		enterChain = append(enterChain, s)
	}
	// Reverse to outermost-first.
	for k := len(enterChain) - 1; k >= 0; k-- {
		entries = append(entries, enterChain[k])
	}

	// Descend into the target's initial children, recording the interior states
	// entered along the way (the descent leaves are part of the configuration;
	// the interior compound/region states are entry-cascade members too).
	entries = append(entries, m.descentInterior(to)...)
	return exits, entries
}

// reentersSelfOrAncestor reports whether transition t is an external reentering
// transition whose target is the source leaf itself or one of its ancestors. For
// such a transition the standard LCA cascade is empty (the target is its own
// least common ancestor), so a dedicated reentry cascade is used instead. A
// reentering transition to an unrelated target is an ordinary external
// transition and uses the LCA cascade.
func reentersSelfOrAncestor[S comparable, E comparable, C any](t *Transition[S, E, C], from, to S, m *Machine[S, E, C]) bool {
	if !t.Reenter {
		return false
	}
	for _, anc := range m.ancestors(from) {
		if anc == to {
			return true
		}
	}
	return false
}

// reenterCascade computes the exit and entry sets for an external reentering
// transition from source leaf `from` to target `to`, where `to` is `from` or an
// ancestor of `from`. It exits from the source up to and including the target
// (innermost-first), then re-enters from the target back down to the source
// spine and descends into the target's initial children (outermost-first). For a
// self-transition (to == from) this is exit [from], entry [from].
func (m *Machine[S, E, C]) reenterCascade(from, to S) (exits, entries []S) {
	chain := m.ancestors(from) // innermost-first: [from, ..., to, ...]
	for _, s := range chain {
		exits = append(exits, s)
		if s == to {
			break
		}
	}
	// Entries are the exits reversed (outermost-first), then the descent into the
	// target's initial children.
	for k := len(exits) - 1; k >= 0; k-- {
		entries = append(entries, exits[k])
	}
	entries = append(entries, m.descentInterior(to)...)
	return exits, entries
}

// descentInterior returns the interior states entered when descending into a
// compound or parallel target's initial children, outermost-first. It excludes
// the target itself (already in the entry chain) but includes nested compound
// states and the leaves reached.
func (m *Machine[S, E, C]) descentInterior(name S) []S {
	n, ok := m.resolveNode(name)
	if !ok {
		return nil
	}
	s := n.state
	switch {
	case isParallel(s):
		var out []S
		for ri := range s.Regions {
			r := &s.Regions[ri]
			if r.InitialChild != nil {
				out = append(out, *r.InitialChild)
				out = append(out, m.descentInterior(*r.InitialChild)...)
			}
		}
		return out
	case isCompound(s):
		if s.InitialChild == nil {
			return nil
		}
		out := []S{*s.InitialChild}
		out = append(out, m.descentInterior(*s.InitialChild)...)
		return out
	default:
		return nil
	}
}

// settleDone applies the final-state done-event semantics after entering `to`.
// Walking up from the entered leaf, each completed ancestor records a done
// microstep and runs that ancestor's OnDone actions; settling continues upward
// while ancestors are complete (a completed inner compound state is itself a
// "done" arrival toward its own parent). Completion is judged against the active
// configuration tracked on the instance, so a parallel parent completes only
// when every region's active leaf is final.
func (i *Instance[S, E, C]) settleDone(to S, entity C, tr *Trace) (effects []Effect, name string, err error) {
	m := i.machine

	n, ok := m.resolveNode(to)
	if !ok || !n.state.IsFinal {
		return effects, "", nil
	}

	cur := to
	for {
		cn, ok := m.resolveNode(cur)
		if !ok || !cn.hasParent {
			return effects, "", nil
		}
		parent := cn.parent
		pn, ok := m.resolveNode(parent)
		if !ok {
			return effects, "", nil
		}
		tr.Microsteps = append(tr.Microsteps, "done."+fmtState(cur))
		if !i.stateComplete(parent) {
			return effects, "", nil
		}
		eff, aname, aerr := i.runActions(pn.state.OnDone, entity, tr)
		effects = append(effects, eff...)
		if aerr != nil {
			return effects, aname, aerr
		}
		cur = parent
	}
}

// stateComplete reports whether a state counts as done given the instance's
// active configuration: a final leaf is done; a compound state is done when its
// active leaf is final; a parallel state is done when every region's active leaf
// is final.
func (i *Instance[S, E, C]) stateComplete(name S) bool {
	m := i.machine
	n, ok := m.resolveNode(name)
	if !ok {
		return false
	}
	s := n.state
	switch {
	case isParallel(s):
		for ri := range s.Regions {
			leaf, ok := i.activeLeafIn(s.Regions[ri].Name, name)
			if !ok {
				return false
			}
			ln, ok := m.resolveNode(leaf)
			if !ok || !ln.state.IsFinal {
				return false
			}
		}
		return true
	case isCompound(s):
		// Complete when an active leaf descending from this state is final.
		for _, leaf := range i.config {
			if m.isDescendant(leaf, name) {
				ln, ok := m.resolveNode(leaf)
				if ok && ln.state.IsFinal {
					return true
				}
			}
		}
		return false
	default:
		return s.IsFinal
	}
}

// activeLeafIn returns the active configuration leaf belonging to region
// `region` of parallel state `parallel`, if any.
func (i *Instance[S, E, C]) activeLeafIn(region string, parallel S) (S, bool) {
	m := i.machine
	for _, leaf := range i.config {
		n, ok := m.resolveNode(leaf)
		if !ok {
			continue
		}
		// Walk up to the region boundary: the leaf belongs to `region` of
		// `parallel` when an ancestor node carries that region and parent.
		cur := leaf
		for {
			cn, ok := m.resolveNode(cur)
			if !ok || !cn.hasParent {
				break
			}
			if cn.region == region && cn.parent == parallel {
				return leaf, true
			}
			cur = cn.parent
		}
		_ = n
	}
	var zero S
	return zero, false
}

// isDescendant reports whether `leaf` is `ancestor` or nested beneath it.
func (m *Machine[S, E, C]) isDescendant(leaf, ancestor S) bool {
	for _, s := range m.ancestors(leaf) {
		if s == ancestor {
			return true
		}
	}
	return false
}

// _ keeps context imported for the kernel's pure-step contract symmetry.
var _ = context.Background
