package state

import (
	"context"
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
	seen := map[S]int{}
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
	tr.MatchedAt = fmtState(parallel)

	for ri := range pn.state.Regions {
		r := &pn.state.Regions[ri]
		leaf, ok := i.activeLeafIn(r.Name, parallel)
		if !ok {
			continue
		}
		handled, eff, err := i.fireRegion(parallel, r, leaf, event, entity, &tr)
		if handled {
			anyHandled = true
			effects = append(effects, eff...)
			tr.Microsteps = append(tr.Microsteps, fmt.Sprintf("region:%s", r.Name))
			if err != nil {
				regionErrs = append(regionErrs, err)
			}
		}
	}

	if !anyHandled {
		// No region consumed the event: offer it to the parallel state and its
		// ancestors as a cross-cutting transition.
		return i.fireFromState(ctx, parallel, event, tr)
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
				Err: &ErrActionFailed{TransitionName: "onDone:" + fmtState(parallel), ActionName: dname, Cause: derr},
			}
		}
		tr.Outcome = OutcomeSuccess
		return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr}
	case 1:
		tr.Outcome = OutcomeGuardFailed
		return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr, Err: regionErrs[0]}
	default:
		tr.Outcome = OutcomeGuardFailed
		return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr, Err: &MultiRegionErr{Errors: regionErrs}}
	}
}

// fireRegion resolves the event within one region, child-first from the region's
// active leaf up to (but not crossing) the region boundary. handled is true when
// a transition matched (whether it then succeeded or failed a guard/effect).
func (i *Instance[S, E, C]) fireRegion(
	parallel S, r *Region[S, E, C], leaf S, event E, entity C, tr *Trace,
) (handled bool, effects []Effect, err error) {
	m := i.machine

	matched := false
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
			matched = true
			pass := true
			for _, g := range t.Guards {
				tr.GuardsEvaluated = append(tr.GuardsEvaluated, g.Name)
				okg, gErr := m.evalGuard(g, entity)
				if gErr != nil {
					return true, nil, gErr
				}
				if !okg {
					pass = false
					err = &ErrGuardFailed{GuardName: g.Name, Reason: "predicate returned false"}
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
					err = &ErrGuardFailed{GuardName: joinLeafs(res.failedLeafs), Reason: "composite guard failed"}
				}
			}
			if pass {
				eff := i.applyRegionTransition(r, leaf, t, entity, tr)
				return true, eff, nil
			}
		}
	}
	// A matched-but-guard-failed candidate still counts as handled, so the event
	// is not spuriously bubbled to a cross-cutting transition.
	return matched, nil, err
}

// applyRegionTransition advances one region's leaf and runs the exit/transition/
// entry cascade confined to that region.
func (i *Instance[S, E, C]) applyRegionTransition(
	r *Region[S, E, C], leaf S, t *Transition[S, E, C], entity C, tr *Trace,
) []Effect {
	m := i.machine
	to := t.To
	exits, entries := m.cascade(leaf, to)

	var effects []Effect
	for _, s := range exits {
		tr.ExitedStates = append(tr.ExitedStates, fmtState(s))
		if n, ok := m.resolveNode(s); ok {
			eff, _, _ := i.runActions(n.state.OnExit, entity, tr)
			effects = append(effects, eff...)
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

	// Swap this region's leaf in the configuration.
	i.replaceRegionLeaf(r, leaf, m.descendToLeaves(to))

	eff, _, _ := i.runActions(t.Effects, entity, tr)
	effects = append(effects, eff...)

	for _, s := range entries {
		tr.EnteredStates = append(tr.EnteredStates, fmtState(s))
		if n, ok := m.resolveNode(s); ok {
			eff, _, _ := i.runActions(n.state.OnEntry, entity, tr)
			effects = append(effects, eff...)
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
	return effects
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
	tr.Microsteps = append(tr.Microsteps, "done."+fmtState(parallel))
	eff, aname, aerr := i.runActions(pn.state.OnDone, entity, tr)
	if aerr != nil {
		return eff, aname, aerr
	}
	// Continue settling upward if the parallel state itself completes a parent.
	if pn.hasParent {
		up, uname, uerr := i.settleDone(parallel, entity, tr)
		eff = append(eff, up...)
		if uerr != nil {
			return eff, uname, uerr
		}
	}
	return eff, "", nil
}

// fireFromState resolves the event from an explicit state up through its
// ancestors (used when no region handled the event in a parallel state).
func (i *Instance[S, E, C]) fireFromState(ctx context.Context, start S, event E, tr Trace) FireResult[S] {
	m := i.machine
	entity := i.entity
	from := i.current

	for _, anc := range m.ancestors(start) {
		n, ok := m.resolveNode(anc)
		if !ok {
			continue
		}
		if forbids(n.state, event) {
			tr.MatchedAt = fmtState(anc)
			tr.Outcome = OutcomeSuccess
			tr.Microsteps = append(tr.Microsteps, "forbidden."+fmt.Sprint(event)+"@"+fmtState(anc))
			return FireResult[S]{NewState: from, Trace: tr}
		}
		for _, t := range matchingTransitions(n.state, event) {
			pass := true
			for _, g := range t.Guards {
				tr.GuardsEvaluated = append(tr.GuardsEvaluated, g.Name)
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
				tr.MatchedAt = fmtState(anc)
				return i.commit(ctx, t, start, anc, entity, tr)
			}
		}
	}

	err := &ErrInvalidTransition{
		From:   fmtState(from),
		Event:  fmt.Sprint(event),
		Reason: "no transition declared for this state and event",
	}
	return FireResult[S]{NewState: from, Trace: tr, Err: err}
}
