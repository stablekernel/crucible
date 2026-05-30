package state

// This file adds the hierarchical and orthogonal DSL surface: open/close pairs
// for superstates and regions, substate declarations, final-state marking, and
// the entry/exit/done action declarations. The block stack mirrors the chained
// method grain so an unclosed block is caught by a single lint at Quench.

// SuperState declares a compound (hierarchical) state and opens its block. The
// substates declared until the matching EndSuperState become its children, and
// Initial inside the block names the child entered when the superstate is
// entered.
func (b *Builder[S, E, C]) SuperState(name S) *Builder[S, E, C] {
	// A superstate may be nested inside another SuperState block — or inside a
	// Region's substate list — to arbitrary depth. The block stack and the flat
	// stateDef registry already carry parent/region placement at any level, and
	// assembleHierarchy folds the registry into the nested State tree depth-first.
	b.declareState(name)
	file, line := callerSite()
	blk := &block[S, E, C]{kind: blockSuper, owner: b.curState, srcFile: file, srcLine: line}
	b.blocks = append(b.blocks, blk)
	b.curTransition = nil
	return b
}

// SubState declares a substate of the current SuperState or Region block.
func (b *Builder[S, E, C]) SubState(name S) *Builder[S, E, C] {
	if len(b.blocks) == 0 {
		b.recordHSMDiag("SubState called outside a SuperState block")
		// Still declare it as a top-level state so the chain stays usable.
		return b.declareState(name)
	}
	return b.declareState(name)
}

// EndSuperState closes the most-recent SuperState block.
func (b *Builder[S, E, C]) EndSuperState() *Builder[S, E, C] {
	if len(b.blocks) == 0 {
		b.recordHSMDiag("EndSuperState without an open SuperState")
		return b
	}
	top := b.blocks[len(b.blocks)-1]
	if top.kind != blockSuper {
		b.recordHSMDiag("EndSuperState while a Region block is still open")
		return b
	}
	b.closeSuper(top)
	b.blocks = b.blocks[:len(b.blocks)-1]
	b.curTransition = nil
	return b
}

// Region opens an orthogonal region inside the current SuperState block. States
// declared until the matching EndRegion belong to the region, and Initial names
// the region's initial state.
func (b *Builder[S, E, C]) Region(name string) *Builder[S, E, C] {
	if len(b.blocks) == 0 || b.blocks[len(b.blocks)-1].kind != blockSuper {
		b.recordHSMDiag("Region called outside a SuperState block")
	}
	var owner *stateDef[S, E, C]
	if len(b.blocks) > 0 {
		owner = b.blocks[len(b.blocks)-1].owner
	} else {
		owner = b.curState
	}
	file, line := callerSite()
	blk := &block[S, E, C]{kind: blockRegion, owner: owner, region: name, srcFile: file, srcLine: line}
	b.blocks = append(b.blocks, blk)
	b.curTransition = nil
	return b
}

// EndRegion closes the most-recent Region block.
func (b *Builder[S, E, C]) EndRegion() *Builder[S, E, C] {
	if len(b.blocks) == 0 {
		b.recordHSMDiag("EndRegion without an open Region")
		return b
	}
	top := b.blocks[len(b.blocks)-1]
	if top.kind != blockRegion {
		b.recordHSMDiag("EndRegion while a SuperState block is still open")
		return b
	}
	b.closeRegion(top)
	b.blocks = b.blocks[:len(b.blocks)-1]
	b.curTransition = nil
	return b
}

// History declares a history pseudo-state inside the current SuperState block.
// The pseudo-state remembers the owning compound's last active configuration:
// HistoryShallow restores the compound's last active direct child, HistoryDeep
// restores its full nested leaf configuration. Transition to it (by name) to
// re-enter the remembered configuration instead of the compound's Initial. Use
// DefaultTo to declare the target entered when no history has been recorded yet;
// without it the resolver falls back to the compound's Initial.
//
// A history pseudo-state is structure, not a leaf: it never appears in the
// active configuration and is not eligible as a compound's Initial. Declaring
// one outside a SuperState block is a Quench lint.
func (b *Builder[S, E, C]) History(name S, kind HistoryType) *Builder[S, E, C] {
	if kind == HistoryNone {
		kind = HistoryShallow
	}
	if len(b.blocks) == 0 || b.blocks[len(b.blocks)-1].kind != blockSuper {
		b.recordHSMDiag("History called outside a SuperState block")
	}
	b.declareState(name)
	if b.curState != nil {
		b.curState.state.HistoryType = kind
		b.curState.isHistory = true
	}
	// A history pseudo-state is not a real substate: do not count it toward the
	// block's child count, so it cannot satisfy the "has substates but no
	// Initial" requirement nor become eligible as the compound's Initial.
	if len(b.blocks) > 0 {
		b.blocks[len(b.blocks)-1].childCount--
	}
	b.curTransition = nil
	return b
}

// DefaultTo sets the fallback target of the most-recent history pseudo-state,
// entered when its owning compound has no recorded history yet. It is a no-op
// (recorded as a lint at Quench) when the most-recent state is not a history
// pseudo-state.
func (b *Builder[S, E, C]) DefaultTo(target S) *Builder[S, E, C] {
	if b.curState == nil || b.curState.state.HistoryType == HistoryNone {
		b.recordHSMDiag("DefaultTo called on a non-history state")
		return b
	}
	t := target
	b.curState.state.HistoryDefault = &t
	return b
}

// Final marks the most-recent state as terminal.
func (b *Builder[S, E, C]) Final() *Builder[S, E, C] {
	if b.curState != nil {
		b.curState.state.IsFinal = true
	}
	return b
}

// OnEntry attaches a named entry-action ref to the most-recent state.
func (b *Builder[S, E, C]) OnEntry(actionName string, params ...map[string]any) *Builder[S, E, C] {
	if b.curState != nil {
		b.curState.state.OnEntry = append(b.curState.state.OnEntry, Ref{Name: actionName, Params: firstParams(params)})
	}
	return b
}

// OnExit attaches a named exit-action ref to the most-recent state.
func (b *Builder[S, E, C]) OnExit(actionName string, params ...map[string]any) *Builder[S, E, C] {
	if b.curState != nil {
		b.curState.state.OnExit = append(b.curState.state.OnExit, Ref{Name: actionName, Params: firstParams(params)})
	}
	return b
}

// OnDone attaches a named done-action ref to the most-recent state. It runs when
// the state completes — a compound state when its active leaf is final, a
// parallel state when every region is final.
func (b *Builder[S, E, C]) OnDone(actionName string, params ...map[string]any) *Builder[S, E, C] {
	if b.curState != nil {
		b.curState.state.OnDone = append(b.curState.state.OnDone, Ref{Name: actionName, Params: firstParams(params)})
	}
	return b
}

// closeSuper records the superstate's InitialChild from the block, leaving the
// child assembly (nesting the substates) to assembleHierarchy at Quench.
func (b *Builder[S, E, C]) closeSuper(blk *block[S, E, C]) {
	if blk.hasInitial {
		init := blk.initial
		blk.owner.state.InitialChild = &init
	} else if blk.childCount > 0 {
		b.recordHSMDiagAt("superstate has substates but no Initial", blk.srcFile, blk.srcLine)
	}
}

// closeRegion records the region's initial child; the region's States slice is
// assembled at Quench from the flat registry.
func (b *Builder[S, E, C]) closeRegion(blk *block[S, E, C]) {
	owner := blk.owner
	var init *S
	if blk.hasInitial {
		v := blk.initial
		init = &v
	} else if blk.childCount > 0 {
		b.recordHSMDiagAt("region has states but no Initial", blk.srcFile, blk.srcLine)
	}
	// Reserve the region entry in declaration order; States are filled at Quench.
	owner.state.Regions = append(owner.state.Regions, Region[S, E, C]{Name: blk.region, InitialChild: init})
}

// assembleHierarchy folds the builder's flat stateDef registry into the nested
// top-level State slice: each substate is placed into its parent's Children or
// the matching Region's States, in declaration order. States authored already
// nested (via Provide) are returned as-is.
func (b *Builder[S, E, C]) assembleHierarchy() []State[S, E, C] {
	if b.prebuilt {
		out := make([]State[S, E, C], 0, len(b.states))
		for _, sd := range b.states {
			out = append(out, sd.state)
		}
		return out
	}

	byName := map[S]*stateDef[S, E, C]{}
	for _, sd := range b.states {
		byName[sd.state.Name] = sd
	}

	// Index direct children of each parent in declaration order. A child placed
	// in a region is recorded separately so it lands in the region's States slice
	// rather than the parent's Children. Building this index first lets us
	// assemble the tree depth-first, so a nested compound is fully populated
	// before it is copied into its own parent (value copies otherwise drop
	// grandchildren added after the parent was copied).
	for _, sd := range b.states {
		if !sd.hasParent {
			continue
		}
		parent := byName[sd.parent]
		if parent != nil {
			parent.childDefs = append(parent.childDefs, sd)
		}
	}

	// Emit top-level states in declaration order, recursing into each so the
	// full nested structure (any depth) is materialized before the value copy.
	var out []State[S, E, C]
	for _, sd := range b.states {
		if sd.hasParent {
			continue
		}
		out = append(out, b.materialize(sd))
	}
	return out
}

// materialize returns sd.state with its Children and Region States fully
// assembled, recursing to arbitrary depth so nested compounds carry their
// complete subtree before the value is copied into a parent.
func (b *Builder[S, E, C]) materialize(sd *stateDef[S, E, C]) State[S, E, C] {
	s := sd.state
	// Reset slices we are about to rebuild so repeated calls stay idempotent.
	s.Children = nil
	for ri := range s.Regions {
		s.Regions[ri].States = nil
	}
	for _, child := range sd.childDefs {
		cs := b.materialize(child)
		if child.region != "" {
			placeInRegionState(&s, child.region, cs)
			continue
		}
		s.Children = append(s.Children, cs)
	}
	return s
}

// placeInRegionState appends a fully-materialized substate into the named
// region's States slice on the value-copied parent state.
func placeInRegionState[S comparable, E comparable, C any](parent *State[S, E, C], region string, s State[S, E, C]) {
	for ri := range parent.Regions {
		if parent.Regions[ri].Name == region {
			parent.Regions[ri].States = append(parent.Regions[ri].States, s)
			return
		}
	}
}

// hasChildSubstates reports whether any declared state names sd as its
// compound (non-region) parent.
func (b *Builder[S, E, C]) hasChildSubstates(sd *stateDef[S, E, C]) bool {
	for _, other := range b.states {
		if other.hasParent && other.region == "" && other.parent == sd.state.Name {
			return true
		}
	}
	return false
}

// recordHSMDiag records an HSM lint finding (error severity) without a site.
func (b *Builder[S, E, C]) recordHSMDiag(msg string) {
	file, line := callerSite2()
	b.recordHSMDiagAt(msg, file, line)
}

// recordHSMDiagAt records an HSM lint finding with an explicit site.
func (b *Builder[S, E, C]) recordHSMDiagAt(msg, file string, line int) {
	b.hsmDiags = append(b.hsmDiags, diagnostic{Diagnostic: Diagnostic{
		Severity: diagError,
		Message:  msg,
		SrcFile:  file,
		SrcLine:  line,
	}})
}
