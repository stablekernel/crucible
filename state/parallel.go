package state

import (
	"context"
	"errors"
	"fmt"
)

// activeParallelAncestor returns the parallel state currently active in the
// configuration, if any. A parallel state is active when the configuration holds
// leaves descending from two or more of its regions. The result is the
// innermost such state, so nested orthogonality resolves deepest-first.
func (i *Instance[S, E, C]) activeParallelAncestor() (S, bool) {
	m := i.machine
	var best S
	bestDepth := -1
	found := false
	if len(i.config) < 2 {
		var zero S
		return zero, false
	}
	// Candidate parallel states are the ancestors shared by the config leaves.
	// Presize from the leaf count so the map does not grow-and-rehash while the
	// orthogonal config is scanned. This runs only when already in a parallel
	// configuration (len(config) >= 2); flat machines return above without
	// allocating.
	seen := make(map[S]int, len(i.config))
	for _, leaf := range i.config {
		for _, anc := range m.ancestors(leaf) {
			n, ok := m.resolveNode(anc)
			if !ok {
				continue
			}
			if isParallel(n.state) {
				seen[anc]++
				if seen[anc] >= 2 && n.depth > bestDepth {
					best = anc
					bestDepth = n.depth
					found = true
				}
			}
		}
	}
	return best, found
}

// enclosingActiveParallel returns the nearest ancestor of `parallel` that is
// itself an active parallel state (orthogonally active in the live config),
// supporting the outward bubble for nested parallels: when no region of an inner
// parallel handles an event, the search continues in the enclosing parallel's
// sibling regions. The walk starts above `parallel` (its own entry is skipped)
// and returns the first active parallel ancestor, if any.
func (i *Instance[S, E, C]) enclosingActiveParallel(parallel S) (S, bool) {
	m := i.machine
	for _, anc := range m.ancestors(parallel) {
		if anc == parallel {
			continue // skip self; we want a strictly enclosing parallel
		}
		n, ok := m.resolveNode(anc)
		if !ok {
			continue
		}
		if isParallel(n.state) && i.parallelActive(anc) {
			return anc, true
		}
	}
	var zero S
	return zero, false
}

// fireParallel broadcasts the event to every region of an active parallel state.
// Each region resolves independently child-first within the region; effects
// concatenate in region-declaration order; a per-region "done" arrival and the
// macrostep are recorded in the trace. If no region handles the event, the event
// bubbles up from the parallel state through its ancestors (a cross-cutting
// transition that exits all regions).
func (i *Instance[S, E, C]) fireParallel(ctx context.Context, parallel S, event E, tr Trace) FireResult[S] {
	m := i.machine
	pn, _ := m.resolveNode(parallel)
	entity := i.entity

	var effects []Effect
	var regionErrs []error
	anyHandled := false
	tr.MatchedAt = m.label(parallel)

	// The triggering event's payload (WithEventData) is broadcast to every region:
	// resolve it once, before the region loop, so the region that actually handles
	// the event sees it. eventData is consume-once, so calling it per region would
	// hand the payload to whichever region is visited first and strip it from the
	// rest — masking a service/actor onDone result from the region that routes it.
	eventData := i.eventData(event)

	for ri := range pn.state.Regions {
		r := &pn.state.Regions[ri]
		leaf, ok := i.activeLeafIn(r.Name, parallel)
		if !ok {
			continue
		}
		handled, eff, err := i.fireRegion(parallel, r, leaf, event, entity, eventData, &tr)
		if handled {
			anyHandled = true
			effects = append(effects, eff...)
			tr.note(fmt.Sprintf("region:%s", r.Name))
			if err != nil {
				regionErrs = append(regionErrs, err)
			}
		}
	}

	if !anyHandled {
		// No region of THIS parallel consumed the event. When this parallel is
		// itself nested inside an enclosing active parallel, the event must bubble
		// OUTWARD to the enclosing parallel's regions before falling back to a
		// cross-cutting transition: a handler may live in an outer sibling region
		// (e.g. y1 -> y2 in the enclosing parallel's second region) while this inner
		// parallel is active. Broadcasting to the enclosing parallel re-offers the
		// event to every outer region; the outer region containing this parallel
		// only re-checks its own spine (this parallel's leaves already declined), so
		// no inner region is double-delivered.
		if outer, ok := i.enclosingActiveParallel(parallel); ok {
			return i.fireParallel(ctx, outer, event, tr)
		}
		// No enclosing active parallel: offer the event to the parallel state and
		// its ancestors as a cross-cutting transition, carrying the same resolved
		// payload (the regions did not consume it, so a cross-cutting Assign — e.g.
		// an actor onDone that exits the parallel state — still sees the done result).
		return i.fireFromState(ctx, parallel, event, eventData, tr)
	}

	switch len(regionErrs) {
	case 0:
		// Settle completion: when every region is final, run the parallel
		// state's OnDone (and cascade upward).
		doneEff, dname, derr := i.settleParallelDone(parallel, entity, &tr)
		effects = append(effects, doneEff...)
		if derr != nil {
			tr.Outcome = OutcomeEffectError
			return FireResult[S]{
				NewState: i.current, Effects: effects, Trace: tr,
				Err: &ActionFailedError{TransitionName: "onDone:" + fmtState(parallel), ActionName: dname, Cause: derr},
			}
		}
		tr.Outcome = OutcomeSuccess
		return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr}
	case 1:
		tr.Outcome = regionErrOutcome(regionErrs[0])
		return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr, Err: regionErrs[0]}
	default:
		tr.Outcome = regionErrOutcome(regionErrs[0])
		return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr, Err: &MultiRegionError{Errors: regionErrs}}
	}
}

// regionErrOutcome maps a region-level error to the appropriate outcome,
// mirroring the main commit path: a bound action that errored surfaces as
// OutcomeEffectError, an assign reducer panic as OutcomeAssignFailed, and a
// failing guard predicate as OutcomeGuardFailed.
func regionErrOutcome(err error) Outcome {
	var afe *ActionFailedError
	if errors.As(err, &afe) {
		return OutcomeEffectError
	}
	var ap *AssignPanicError
	if errors.As(err, &ap) {
		return OutcomeAssignFailed
	}
	return OutcomeGuardFailed
}

// fireRegion resolves the event within one region, child-first from the region's
// active leaf up to (but not crossing) the region boundary. handled is true only
// when a transition's guards passed and its cascade ran (or its guard PANICKED,
// surfaced as a hard error). A candidate that matched but failed a guard PREDICATE
// (returned false) does NOT consume the event: handled is false so the event
// bubbles to the parallel-state-level handler, matching the compound-state shape
// where a false guard continues up the ancestor chain.
func (i *Instance[S, E, C]) fireRegion(
	parallel S, r *Region[S, E, C], leaf S, event E, entity C, eventData any, tr *Trace,
) (handled bool, effects []Effect, err error) {
	m := i.machine

	for _, anc := range m.ancestors(leaf) {
		if anc == parallel {
			break // do not cross the region boundary
		}
		n, ok := m.resolveNode(anc)
		if !ok {
			continue
		}
		cands := matchingTransitions(n.state, event)
		for _, t := range cands {
			pass := true
			for _, g := range t.Guards {
				tr.recordGuard(g.Name)
				okg, gErr := m.evalGuard(g, entity)
				if gErr != nil {
					return true, nil, gErr
				}
				if !okg {
					pass = false
					break
				}
			}
			if pass && t.GuardExpr != nil {
				res := i.evalGuardExpr(t.GuardExpr, entity, tr)
				if res.err != nil {
					return true, nil, res.err
				}
				if !res.ok {
					pass = false
				}
			}
			if pass {
				eff, aErr := i.applyRegionTransition(parallel, r, leaf, t, eventData, tr)
				return true, eff, aErr
			}
		}
	}
	// Either no candidate matched, or every match failed a guard predicate. In
	// both cases the region did not consume the event; bubble it.
	return false, nil, nil
}

// applyRegionTransition advances one region's leaf and runs the exit/transition/
// entry cascade confined to that region. It mirrors the main commit path: a
// reducer that errors or panics stops the cascade immediately and surfaces the
// error to the caller rather than silently no-oping.
func (i *Instance[S, E, C]) applyRegionTransition(
	parallel S, r *Region[S, E, C], leaf S, t *Transition[S, E, C], eventData any, tr *Trace,
) ([]Effect, error) {
	m := i.machine
	to := t.To

	// A history pseudostate target owned by a compound nested in THIS region
	// re-enters the remembered configuration of that compound (or its default /
	// initial when nothing is recorded yet). resolveHistory expands it to the
	// concrete leaves and retargets the cascade at the owning compound; the
	// cross-region history-target variant is already rejected at Quench, so any
	// history target reaching here is region-internal and well-defined.
	var restoreLeaves []S
	if leaves, owner, isHist := i.resolveHistory(to); isHist {
		restoreLeaves = leaves
		to = owner
	}

	exits, entries := m.cascade(leaf, to)
	if restoreLeaves != nil {
		// Substitute the compound's default descent with the remembered descent:
		// keep the entry chain up to and including the compound, then enter the
		// recorded interior leaves instead of the InitialChild spine.
		entries = m.entryChainTo(leaf, to)
		entries = append(entries, m.restoreInterior(to, restoreLeaves)...)
	}

	// cur threads the context by value through this region's cascade, exactly as
	// commit does (fire.go). Actions in a phase read cur as it stood at phase
	// entry (read-only); that phase's assigns then fold cur, each reducer seeing
	// the prior result. The folded value is committed to the instance once, at the
	// end — the sole context-mutation site on the region path.
	//
	// The fold base is the LIVE instance context (i.entity), not the broadcast
	// snapshot passed in for guard/action evaluation: sequential regions of one
	// macrostep compose in declaration order, so this region must observe (and
	// fold onto) the prior regions' committed folds. Using the frozen broadcast
	// snapshot here would discard the earlier regions' assigns.
	cur := i.entity

	// Record the history of every compound being exited before the configuration
	// advances, so a later history-targeted entry restores the leaves left here.
	i.recordHistory(exits, i.config)

	var effects []Effect
	for _, s := range exits {
		tr.recordExit(m.label(s))
		if n, ok := m.resolveNode(s); ok {
			eff, errName, err := i.runActions(n.state.OnExit, cur, tr)
			effects = append(effects, eff...)
			if err != nil {
				return effects, &ActionFailedError{TransitionName: transName(leaf, to), ActionName: errName, Cause: err}
			}
			next, _, aErr := i.applyAssigns(n.state.OnExitAssign, cur, eventData, tr)
			if aErr != nil {
				return effects, aErr
			}
			cur = next
		}
	}

	// Auto-cancel/stop-on-exit: every exited region substate that armed an
	// `after` timer, ran an invoked service, or ran an invoked actor emits the
	// corresponding CancelScheduled/StopService/StopActor effect — identical to
	// the normal exit cascade in commit, so lifecycle effects are symmetric on
	// the region path.
	effects = append(effects, i.afterEffectsOnExit(exits, tr)...)
	effects = append(effects, i.invokeEffectsOnExit(exits, tr)...)
	effects = append(effects, i.actorEffectsOnExit(exits, tr)...)

	// Swap this region's leaf in the configuration. A history restore pins the
	// remembered leaves; otherwise descend into the target's initial children.
	if restoreLeaves != nil {
		i.replaceRegionLeaf(r, leaf, append([]S(nil), restoreLeaves...))
	} else {
		i.replaceRegionLeaf(r, leaf, m.descendToLeaves(to))
	}

	eff, errName, err := i.runActions(t.Effects, cur, tr)
	effects = append(effects, eff...)
	if err != nil {
		return effects, &ActionFailedError{TransitionName: transName(leaf, to), ActionName: errName, Cause: err}
	}
	next, _, aErr := i.applyAssigns(t.Assigns, cur, eventData, tr)
	if aErr != nil {
		return effects, aErr
	}
	cur = next

	for _, s := range entries {
		tr.recordEntry(m.label(s))
		if n, ok := m.resolveNode(s); ok {
			eff, errName, err := i.runActions(n.state.OnEntry, cur, tr)
			effects = append(effects, eff...)
			if err != nil {
				return effects, &ActionFailedError{TransitionName: transName(leaf, to), ActionName: errName, Cause: err}
			}
			next, _, aErr := i.applyAssigns(n.state.OnEntryAssign, cur, eventData, tr)
			if aErr != nil {
				return effects, aErr
			}
			cur = next
		}
	}

	// On-entry lifecycle effects: every entered region substate that declares an
	// `after` transition, an invoked service, or an invoked actor emits the
	// corresponding ScheduleAfter/StartService/SpawnActor effect — identical to
	// the normal entry cascade in commit. Without this, a state entered inside a
	// parallel region would silently never start its timer/service/actor.
	effects = append(effects, i.afterEffectsOnEntry(entries, tr)...)
	effects = append(effects, i.invokeEffectsOnEntry(entries, tr)...)
	effects = append(effects, i.actorEffectsOnEntry(entries, tr)...)

	// Done-event semantics for compounds INTERIOR to this region: entering a final
	// leaf may complete a nested compound, which runs that compound's OnDone (and
	// cascades upward while ancestors complete). The walk is bounded at the owning
	// parallel state: the parallel-state-level done is settled separately by
	// settleParallelDone in fireParallel, so settleInteriorDone must not run the
	// parallel's OnDone here or it would double-emit. OnDone reads the folded
	// context (cur), consistent with every other action reading context read-only.
	doneEff, dname, derr := i.settleInteriorDone(to, parallel, cur, tr)
	effects = append(effects, doneEff...)
	if derr != nil {
		return effects, &ActionFailedError{TransitionName: transName(leaf, to), ActionName: dname, Cause: derr}
	}

	// Commit the folded context to the instance — the sole context-mutation site
	// on the region path (mirrors commit's single G1 write).
	i.entity = cur

	// Enqueue any internal events this region transition raises, mirroring the
	// main commit path (fire.go). The queue is drained by the macrostep's
	// run-to-completion loop, so a Raise on a region-internal transition is
	// delivered to sibling regions (or the parallel state) within the same Fire.
	i.enqueueRaised(t, tr)
	return effects, nil
}

// replaceRegionLeaf substitutes the region's old active leaf in the
// configuration with the new descended leaves, preserving order.
func (i *Instance[S, E, C]) replaceRegionLeaf(r *Region[S, E, C], old S, repl []S) {
	out := make([]S, 0, len(i.config))
	for _, leaf := range i.config {
		if leaf == old {
			out = append(out, repl...)
			continue
		}
		out = append(out, leaf)
	}
	i.config = out
	if len(i.config) > 0 {
		i.current = i.config[0]
	}
	_ = r
}

// exitActionChain expands the structural exit cascade into the full ordered set
// of states whose OnExit actions/assigns must run when the cascade exits a
// parallel superstate. The structural cascade (computed from the matched state's
// spine) names only one leaf chain; a cross-cutting transition out of a parallel
// state abandons every region's active leaf, so each region leaf's OnExit must
// run too — and in the locked order: innermost-leaf-first within each region,
// regions in declaration order, then the parallel state's own OnExit after its
// children.
//
// For every structural exit state that is an active parallel (holds leaves in
// two or more of its regions), the active-leaf spine of each region is inserted
// — innermost-first up to but excluding the parallel — ahead of the parallel
// itself. Non-parallel exits pass through unchanged, so a flat or single-spine
// exit cascade is returned identically (no allocation beyond the copy). A
// region leaf already present in the structural exits (the matched state's own
// region spine) is not duplicated.
func (i *Instance[S, E, C]) exitActionChain(exits []S) []S {
	m := i.machine

	// Fast path: no exited state is an active parallel, so the structural cascade
	// already names every OnExit-bearing state. This is every flat/compound exit.
	anyParallel := false
	for _, s := range exits {
		if n, ok := m.resolveNode(s); ok && isParallel(n.state) && i.parallelActive(s) {
			anyParallel = true
			break
		}
	}
	if !anyParallel {
		return exits
	}

	seen := make(map[S]bool, len(exits))
	for _, s := range exits {
		seen[s] = true
	}

	out := make([]S, 0, len(exits)+len(i.config))
	for _, s := range exits {
		n, ok := m.resolveNode(s)
		if ok && isParallel(n.state) && i.parallelActive(s) {
			// Emit each region's active-leaf spine (innermost-first, up to but
			// excluding the parallel) before the parallel's own OnExit, regions in
			// declaration order.
			for ri := range n.state.Regions {
				leaf, found := i.activeLeafIn(n.state.Regions[ri].Name, s)
				if !found {
					continue
				}
				for _, anc := range m.ancestors(leaf) {
					if anc == s {
						break // do not cross the region boundary into the parallel
					}
					if seen[anc] {
						continue
					}
					seen[anc] = true
					out = append(out, anc)
				}
			}
		}
		out = append(out, s)
	}
	return out
}

// parallelActive reports whether the parallel state currently holds active
// leaves in two or more of its regions, i.e. it is orthogonally active in the
// live configuration. A parallel state with only one region populated (or none)
// is not treated as active for exit-set expansion.
func (i *Instance[S, E, C]) parallelActive(parallel S) bool {
	m := i.machine
	n, ok := m.resolveNode(parallel)
	if !ok || !isParallel(n.state) {
		return false
	}
	count := 0
	for ri := range n.state.Regions {
		if _, found := i.activeLeafIn(n.state.Regions[ri].Name, parallel); found {
			count++
			if count >= 2 {
				return true
			}
		}
	}
	return false
}

// settleInteriorDone runs the OnDone of every compound INTERIOR to a region that
// completes when a region transition drives an active leaf final. It mirrors
// settleDone (cascade.go) but is bounded at the owning parallel state: the walk
// stops the moment the next ancestor is `parallel`, without noting or running
// the parallel's own OnDone. The parallel-state-level done is settled separately
// by settleParallelDone in fireParallel; running it here too would double-emit
// the parallel OnDone within a single macrostep.
//
// `to` is the entered leaf, `parallel` the region's owning parallel state, and
// `entity` the folded context the OnDone actions read.
func (i *Instance[S, E, C]) settleInteriorDone(to, parallel S, entity C, tr *Trace) (effects []Effect, name string, err error) {
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
		if parent == parallel {
			// Reached the region boundary; the parallel's done is settled by
			// settleParallelDone, not here.
			return effects, "", nil
		}
		pn, ok := m.resolveNode(parent)
		if !ok {
			return effects, "", nil
		}
		tr.note("done." + m.label(cur))
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

// settleParallelDone runs the parallel state's OnDone (cascading upward) once all
// regions have reached a final leaf.
func (i *Instance[S, E, C]) settleParallelDone(parallel S, entity C, tr *Trace) (effects []Effect, name string, err error) {
	m := i.machine
	if !i.stateComplete(parallel) {
		return nil, "", nil
	}
	pn, ok := m.resolveNode(parallel)
	if !ok {
		return nil, "", nil
	}
	tr.note("done." + m.label(parallel))
	eff, aname, aerr := i.runActions(pn.state.OnDone, entity, tr)
	if aerr != nil {
		return eff, aname, aerr
	}
	// Continue settling upward through the enclosing spine. A parallel state is
	// never IsFinal, so settleDone (which gates on IsFinal) is a no-op here;
	// instead settle each enclosing compound through completion semantics, exactly
	// as settleInteriorDone does for compounds interior to a region. Walking from
	// the parallel up its ancestors, every enclosing compound that is now
	// stateComplete records a done microstep and runs its OnDone, innermost-first,
	// until an incomplete ancestor halts the cascade. The parallel's OWN OnDone
	// already ran above and is not re-emitted here.
	up, uname, uerr := i.settleEnclosingDone(parallel, entity, tr)
	eff = append(eff, up...)
	if uerr != nil {
		return eff, uname, uerr
	}
	return eff, "", nil
}

// settleEnclosingDone settles the done/OnDone of every compound that ENCLOSES a
// completed state, cascading innermost-first up the ancestor spine while each
// enclosing ancestor is stateComplete. It is the upward counterpart shared by
// settleParallelDone: a parallel state is never IsFinal, so settleDone cannot
// carry the cascade past it; this routes through stateComplete instead, the same
// completion gate settleInteriorDone uses. The starting state's own OnDone is the
// caller's responsibility and is never run here.
func (i *Instance[S, E, C]) settleEnclosingDone(start S, entity C, tr *Trace) (effects []Effect, name string, err error) {
	m := i.machine

	cur := start
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
		tr.note("done." + m.label(cur))
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

// fireFromState resolves the event from an explicit state up through its
// ancestors (used when no region handled the event in a parallel state). eventData
// is the already-resolved payload the matched transition's Assign reads from
// AssignCtx.Event, passed through from fireParallel so a cross-cutting Assign sees
// the same payload the regions were offered.
func (i *Instance[S, E, C]) fireFromState(ctx context.Context, start S, event E, eventData any, tr Trace) FireResult[S] {
	m := i.machine
	entity := i.entity
	from := i.current

	for _, anc := range m.ancestors(start) {
		n, ok := m.resolveNode(anc)
		if !ok {
			continue
		}
		if forbids(n.state, event) {
			tr.MatchedAt = m.label(anc)
			tr.Outcome = OutcomeSuccess
			tr.note("forbidden." + fmt.Sprint(event) + "@" + m.label(anc))
			return FireResult[S]{NewState: from, Trace: tr}
		}
		for _, t := range matchingTransitions(n.state, event) {
			pass := true
			for _, g := range t.Guards {
				tr.recordGuard(g.Name)
				okg, gErr := m.evalGuard(g, entity)
				if gErr != nil {
					tr.Outcome = OutcomeGuardPanic
					return FireResult[S]{NewState: from, Trace: tr, Err: gErr}
				}
				if !okg {
					pass = false
					break
				}
			}
			if pass && t.GuardExpr != nil {
				res := i.evalGuardExpr(t.GuardExpr, entity, &tr)
				if res.err != nil {
					tr.Outcome = OutcomeGuardPanic
					return FireResult[S]{NewState: from, Trace: tr, Err: res.err}
				}
				if !res.ok {
					pass = false
				}
			}
			if pass {
				tr.MatchedAt = m.label(anc)
				return i.commit(ctx, t, start, anc, entity, eventData, tr)
			}
		}
	}

	err := &InvalidTransitionError{
		From:   fmtState(from),
		Event:  fmt.Sprint(event),
		Reason: "no transition declared for this state and event",
	}
	return FireResult[S]{NewState: from, Trace: tr, Err: err}
}
