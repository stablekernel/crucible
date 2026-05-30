package state

// This file adds history pseudo-states to the hierarchical layer. A history
// pseudo-state belongs to a compound state and remembers that compound's last
// active configuration. Transitioning *to* the history state re-enters the
// remembered configuration instead of the compound's InitialChild:
//
//   - shallow history restores the compound's last active *direct child*
//     (then descends into that child's own initial children);
//   - deep history restores the full nested *leaf* configuration beneath the
//     compound verbatim.
//
// If the compound has no recorded history yet (never entered/exited), the
// resolver falls back to the history state's declared default target, else the
// compound's InitialChild.
//
// History pseudo-states are *structure*: they serialize as part of the IR and
// round-trip losslessly. The *recorded* per-compound configuration is runtime
// instance state — it lives on the Instance, is empty after Cast, and is read
// and written by Fire as the instance advances. Fire stays pure: the recorded
// history is threaded through the instance just like the active configuration,
// with no IO, clock, or hidden global mutation.

// isHistory reports whether a state is a history pseudo-state.
func isHistory[S comparable, E comparable, C any](s *State[S, E, C]) bool {
	return s.HistoryType != HistoryNone
}

// resolveHistory expands a transition target that names a history pseudo-state
// into the concrete entry target(s) to enter. It returns the set of leaves to
// activate and the entry-cascade interior to record. When the target is not a
// history state, ok is false and the caller proceeds with the ordinary cascade.
//
// The expansion reads the instance's recorded history for the history state's
// owning compound:
//
//   - shallow: the recorded last-active direct child of the compound, descended
//     to its leaves; absent → default target, else the compound's InitialChild.
//   - deep: the recorded last-active leaf configuration beneath the compound,
//     verbatim; absent → default target descended to leaves, else InitialChild.
//
// The returned target is the deepest single state to "enter toward" so the
// existing cascade machinery computes the exit/entry sets; for deep history with
// a multi-leaf record (a parallel compound) the recorded leaves are returned so
// the caller restores the exact configuration.
func (i *Instance[S, E, C]) resolveHistory(target S) (leaves []S, owner S, ok bool) {
	m := i.machine
	n, found := m.resolveNode(target)
	if !found || !isHistory(n.state) {
		var zero S
		return nil, zero, false
	}
	// The history pseudo-state's parent is the compound it remembers.
	owner = n.parent
	deep := n.state.HistoryType == HistoryDeep

	if deep {
		if rec, has := i.historyDeep[owner]; has && len(rec) > 0 {
			return append([]S(nil), rec...), owner, true
		}
	} else {
		if child, has := i.historyShallow[owner]; has {
			return m.descendToLeaves(child), owner, true
		}
	}

	// No recorded history: fall back to the declared default target, else the
	// owning compound's InitialChild.
	if n.state.HistoryDefault != nil {
		return m.descendToLeaves(*n.state.HistoryDefault), owner, true
	}
	on, ownerFound := m.resolveNode(owner)
	if ownerFound && on.state.InitialChild != nil {
		return m.descendToLeaves(*on.state.InitialChild), owner, true
	}
	// A compound with no initial child degenerates to entering the compound
	// itself (matches descendToLeaves' leaf fallback elsewhere).
	return []S{owner}, owner, true
}

// recordHistory captures the configuration of every exited compound into the
// instance's per-compound history. It runs during commit, before the active
// configuration is advanced, so it observes the leaves about to be left. For
// each exited compound state it records both the shallow direct child and the
// deep leaf set, so a later history-targeted entry can restore either flavor.
//
// `exits` is the exit cascade (innermost-first) and `fromConfig` is the active
// leaf configuration prior to the transition.
func (i *Instance[S, E, C]) recordHistory(exits []S, fromConfig []S) {
	m := i.machine
	for _, s := range exits {
		n, ok := m.resolveNode(s)
		if !ok || !isCompound(n.state) {
			continue
		}
		// Deep: the leaves of fromConfig that descend from this compound.
		var deep []S
		for _, leaf := range fromConfig {
			if m.isDescendant(leaf, s) {
				deep = append(deep, leaf)
			}
		}
		if len(deep) == 0 {
			continue
		}
		if i.historyDeep == nil {
			i.historyDeep = map[S][]S{}
		}
		i.historyDeep[s] = deep

		// Shallow: the compound's direct child on the spine toward the first
		// recorded leaf.
		if child, ok := m.directChildToward(s, deep[0]); ok {
			if i.historyShallow == nil {
				i.historyShallow = map[S]S{}
			}
			i.historyShallow[s] = child
		}
	}
}

// entryChainTo returns the entry chain entered when transitioning from `from` to
// `to`, outermost-first, up to and including `to` but excluding any descent into
// `to`'s initial children. It mirrors the entry half of cascade without the
// default interior descent, so a history restore can substitute its own interior.
func (m *Machine[S, E, C]) entryChainTo(from, to S) []S {
	lca, hasLCA := m.lca(from, to)
	toChain := m.ancestors(to) // innermost-first: [to, parent, ... root]
	var enterChain []S
	for _, s := range toChain {
		if hasLCA && s == lca {
			break
		}
		enterChain = append(enterChain, s)
	}
	var entries []S
	for k := len(enterChain) - 1; k >= 0; k-- {
		entries = append(entries, enterChain[k])
	}
	return entries
}

// restoreInterior returns the interior states entered when restoring `leaves`
// beneath compound `compound`, outermost-first, excluding `compound` itself. It
// is the history analog of descentInterior: rather than following each
// compound's InitialChild, it follows the spine toward the remembered leaves.
// The returned slice is deduplicated and ordered shallowest-first so entry
// actions run outermost-to-innermost, matching the ordinary entry cascade.
func (m *Machine[S, E, C]) restoreInterior(compound S, leaves []S) []S {
	seen := map[S]bool{compound: true}
	var out []S
	for _, leaf := range leaves {
		// Spine from compound (exclusive) down to leaf (inclusive), outermost-first.
		chain := m.ancestors(leaf) // innermost-first
		var spine []S
		for _, anc := range chain {
			if anc == compound {
				break
			}
			spine = append(spine, anc)
		}
		for k := len(spine) - 1; k >= 0; k-- {
			s := spine[k]
			if seen[s] {
				continue
			}
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// directChildToward returns the direct child of compound `compound` that lies on
// the ancestor spine of `leaf` (i.e. the substate of `compound` that contains
// `leaf`). ok is false when `leaf` does not descend from `compound`.
func (m *Machine[S, E, C]) directChildToward(compound, leaf S) (S, bool) {
	chain := m.ancestors(leaf) // innermost-first: [leaf, parent, ..., root]
	for idx, anc := range chain {
		if anc == compound {
			if idx == 0 {
				// leaf == compound: no proper descendant.
				var zero S
				return zero, false
			}
			return chain[idx-1], true
		}
	}
	var zero S
	return zero, false
}
