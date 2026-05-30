package state

// This file holds the hierarchical and orthogonal (parallel) machinery layered
// over the flat kernel. A flat machine exercises none of it: every flat state
// indexes as a depthless leaf with no parent and no regions, so resolution and
// the entry/exit cascade collapse to the flat behavior.

// node is the resolved, runtime view of one state in the (possibly nested)
// graph. The Machine builds one node per reachable state at Quench time, wiring
// parents, region membership, and ancestor chains so Fire can resolve
// child-first and compute the exit/entry cascade without re-walking the tree.
type node[S comparable, E comparable, C any] struct {
	state  *State[S, E, C]
	parent S
	// hasParent is false for the top-level states; true for any nested substate.
	hasParent bool
	// region names the orthogonal region this node belongs to, when its parent
	// is a parallel state. Empty for compound-state children and top-level nodes.
	region string
	depth  int
}

// indexHierarchy walks the (nested) state graph and records a node per state,
// wiring parent and region membership. It runs once at Quench. The top-level
// states drive the walk; Children and Regions recurse.
func (m *Machine[S, E, C]) indexHierarchy() {
	m.nodes = map[S]*node[S, E, C]{}
	for i := range m.states {
		m.walkNode(&m.states[i], *new(S), false, "", 0)
	}
}

// walkNode records one state and recurses into its children and regions.
func (m *Machine[S, E, C]) walkNode(s *State[S, E, C], parent S, hasParent bool, region string, depth int) {
	m.nodes[s.Name] = &node[S, E, C]{
		state:     s,
		parent:    parent,
		hasParent: hasParent,
		region:    region,
		depth:     depth,
	}
	for i := range s.Children {
		c := &s.Children[i]
		c.Parent = s
		m.walkNode(c, s.Name, true, region, depth+1)
	}
	for ri := range s.Regions {
		r := &s.Regions[ri]
		for i := range r.States {
			c := &r.States[i]
			c.Parent = s
			m.walkNode(c, s.Name, true, r.Name, depth+1)
		}
	}
}

// resolveNode returns the runtime node for a state name.
func (m *Machine[S, E, C]) resolveNode(name S) (*node[S, E, C], bool) {
	n, ok := m.nodes[name]
	return n, ok
}

// isCompound reports whether a state declares child substates.
func isCompound[S comparable, E comparable, C any](s *State[S, E, C]) bool {
	return len(s.Children) > 0
}

// isParallel reports whether a state declares orthogonal regions.
func isParallel[S comparable, E comparable, C any](s *State[S, E, C]) bool {
	return len(s.Regions) > 0
}

// descendToLeaves returns the active leaves reached by entering name and
// cascading into its initial children. For a leaf it returns [name]; for a
// compound state it follows InitialChild transitively; for a parallel state it
// concatenates each region's descent, in region-declaration order.
func (m *Machine[S, E, C]) descendToLeaves(name S) []S {
	n, ok := m.resolveNode(name)
	if !ok {
		return nil
	}
	s := n.state
	switch {
	case isParallel(s):
		var leaves []S
		for ri := range s.Regions {
			r := &s.Regions[ri]
			if r.InitialChild != nil {
				leaves = append(leaves, m.descendToLeaves(*r.InitialChild)...)
			}
		}
		return leaves
	case isCompound(s):
		if s.InitialChild == nil {
			return []S{name}
		}
		return m.descendToLeaves(*s.InitialChild)
	default:
		return []S{name}
	}
}

// ancestors returns name and its ancestors, innermost-first (name, parent, ...).
func (m *Machine[S, E, C]) ancestors(name S) []S {
	var chain []S
	cur := name
	for {
		n, ok := m.resolveNode(cur)
		if !ok {
			break
		}
		chain = append(chain, cur)
		if !n.hasParent {
			break
		}
		cur = n.parent
	}
	return chain
}

// lca returns the least common ancestor of two states, walking both ancestor
// chains. ok is false when the states share no ancestor (distinct top-level
// spines), in which case the cascade exits/enters to the root.
func (m *Machine[S, E, C]) lca(a, b S) (lca S, ok bool) {
	aChain := m.ancestors(a)
	inA := map[S]bool{}
	for _, s := range aChain {
		inA[s] = true
	}
	for _, s := range m.ancestors(b) {
		if inA[s] {
			return s, true
		}
	}
	return lca, false
}
