package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// as is a thin alias over errors.As for the kernel's internal typed-error
// checks, keeping call sites terse.
func as(err error, target any) bool { return errors.As(err, target) }

// Guards and actions receive the entity the Instance was Cast with; the kernel
// supplies it from Instance.entity at Fire time. The entity is never threaded
// through context.
//
// The kernel keeps Fire pure: it never reads the clock, never performs IO, and
// the only state it advances is the Instance.current field.

// Fire runs the full transition pipeline for a single event. To drive a sequence
// of events into one instance use FireSeq; to fan one event across many instances
// use the top-level FireEach.
func (i *Instance[S, E, C]) Fire(ctx context.Context, event E, opts ...FireOption) FireResult[S] {
	cfg := fireConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	// A supplied payload (WithEventData) reaches the triggering transition's Assign
	// through AssignCtx.Event. It is macrostep-local: set for this Fire and cleared
	// when the macrostep settles, so it never leaks into a later Fire.
	i.fireData = cfg.eventData
	i.hasFireData = cfg.hasData
	defer func() {
		i.fireData = nil
		i.hasFireData = false
		// The raised-event queue is macrostep-local: the run-to-completion loop
		// drains it on the success path, but a macrostep that errors returns early
		// (fireCore/runToCompletion) with events still queued. Reset it here so a
		// stale internal event cannot leak into — and replay during — a later Fire.
		i.raised = nil
	}()
	return i.fireWithMiddleware(ctx, event)
}

// eventData resolves the payload an assign reads from AssignCtx.Event: the explicit
// payload supplied to this Fire via WithEventData (consumed once, so only the
// triggering transition sees it), else the boxed triggering event itself.
func (i *Instance[S, E, C]) eventData(event E) any {
	if i.hasFireData {
		i.hasFireData = false
		return i.fireData
	}
	return event
}

// fireWithMiddleware wraps the core step in the installed middleware chain,
// outside-in, so the first-installed middleware is the outermost wrapper.
func (i *Instance[S, E, C]) fireWithMiddleware(ctx context.Context, event E) FireResult[S] {
	next := func(ctx context.Context, event E) FireResult[S] {
		return i.fireCore(ctx, event)
	}
	mws := i.machine.middleware
	for k := len(mws) - 1; k >= 0; k-- {
		next = mws[k](next)
	}
	res := next(ctx, event)
	// Retain the trace according to the history mode selected at Cast:
	//   unbounded → append; ring buffer → fill then overwrite; neither → no-op.
	switch {
	case i.histUnbounded:
		i.history = append(i.history, res.Trace)
	case i.histLimit > 0:
		if len(i.history) < i.histLimit {
			// Ring not yet full: fill forward.
			i.history = append(i.history, res.Trace)
		} else {
			// Ring full: overwrite oldest entry at histHead, then advance.
			i.history[i.histHead] = res.Trace
			i.histHead = (i.histHead + 1) % i.histLimit
		}
	}
	// Surface the settled result to a registered inspector as event/transition/
	// snapshot observations. The call is gated on a non-nil inspector inside
	// emitInspection, so an un-inspected Fire is unchanged and performs no IO.
	i.emitInspection(res)
	// Write the settled outcome to the optional structured-logging seam. Like
	// emitInspection it is gated on a nil logger inside emitLog, so an un-logged
	// Fire is unchanged and performs no IO.
	i.emitLog(res)
	return res
}

// maxMicrosteps bounds the run-to-completion loop so a machine whose raised
// events or eventless transitions form a cycle fails fast with a typed error
// instead of spinning forever. It is generous enough that no well-formed machine
// reaches it within one macrostep.
const maxMicrosteps = 10_000

// fireCore drives one macrostep to a stable configuration: it runs the external
// event's transition, then runs the run-to-completion loop — draining internal
// events enqueued by Raise actions and auto-firing enabled eventless ("always")
// transitions — until neither remains. Every sub-step's effects and trace detail
// are accumulated into the single returned result, and each is recorded in the
// Trace microsteps. The internal queue is local to this call, so Fire stays
// pure: no clock, no IO.
func (i *Instance[S, E, C]) fireCore(ctx context.Context, event E) FireResult[S] {
	res := i.fireOnce(ctx, event)
	if res.Err != nil {
		return res
	}
	return i.runToCompletion(ctx, res)
}

// runToCompletion settles the macrostep after the triggering transition: it
// drains raised internal events first (FIFO, in the same macrostep), then fires
// any enabled eventless transition, looping until the configuration is stable.
// Effects concatenate and microsteps accumulate onto the seed result. A raised
// event or eventless transition that errors stops the loop and surfaces the
// error; an unhandled raised event is ignored (it had no enabled transition),
// rather than failing the macrostep.
func (i *Instance[S, E, C]) runToCompletion(ctx context.Context, res FireResult[S]) FireResult[S] {
	steps := 0
	for {
		// Internal (raised) events take priority and are processed FIFO.
		if len(i.raised) > 0 {
			ev := i.raised[0]
			i.raised = i.raised[1:]
			steps++
			if steps > maxMicrosteps {
				return microstepOverflow(res, i.current)
			}
			sub := i.fireOnce(ctx, ev)
			res.Effects = append(res.Effects, sub.Effects...)
			absorbMicrosteps(&res.Trace, sub.Trace)
			res.NewState = i.current
			if sub.Err != nil {
				// An unhandled raised event (no enabled transition) is ignored; any
				// other failure stops the macrostep and surfaces.
				if isNoTransition(sub.Err) {
					continue
				}
				res.Err = sub.Err
				res.Trace.Outcome = sub.Trace.Outcome
				return res
			}
			continue
		}

		// No pending internal events: fire one enabled eventless transition.
		t, anc, leaf, ok := i.selectEventless()
		if !ok {
			return res
		}
		steps++
		if steps > maxMicrosteps {
			return microstepOverflow(res, i.current)
		}
		sub := i.commitEventless(ctx, t, anc, leaf)
		res.Effects = append(res.Effects, sub.Effects...)
		absorbMicrosteps(&res.Trace, sub.Trace)
		res.NewState = i.current
		if sub.Err != nil {
			res.Err = sub.Err
			res.Trace.Outcome = sub.Trace.Outcome
			return res
		}
	}
}

// commitEventless dispatches one selected eventless ("always") transition. A
// transition resident inside an active parallel region is routed through the
// region commit path (applyRegionTransition), which advances only that region's
// leaf and leaves the sibling regions' leaves in place; otherwise it goes through
// the whole-configuration commit path. Routing region-resident eventless
// transitions through commit would replace the entire configuration and drop the
// sibling region leaves (the K5/T1b corruption).
//
// leaf is the active configuration leaf the transition was found on (its source
// in the live config); anc is the ancestor node the transition is declared on.
func (i *Instance[S, E, C]) commitEventless(ctx context.Context, t *Transition[S, E, C], anc, leaf S) FireResult[S] {
	if parallel, r, ok := i.regionOwning(leaf); ok {
		tr := i.seedTrace("always")
		tr.MatchedAt = i.machine.label(anc)
		eff, err := i.applyRegionTransition(parallel, r, leaf, t, i.eventData(t.On), &tr)
		if err != nil {
			tr.Outcome = regionErrOutcome(err)
			return FireResult[S]{NewState: i.current, Effects: eff, Trace: tr, Err: err}
		}
		tr.Outcome = OutcomeSuccess
		return FireResult[S]{NewState: i.current, Effects: eff, Trace: tr}
	}
	return i.commit(ctx, t, leaf, anc, i.entity, i.eventData(t.On), i.seedTrace("always"))
}

// regionOwning returns the parallel state and region that directly contain the
// given active leaf, when the leaf is inside an active parallel state. It walks
// the leaf's ancestor chain to the region boundary — the first node whose region
// name is set — and confirms that boundary's parent is an active parallel state
// (a parallel currently holding leaves in two or more of its regions). A leaf
// outside any active parallel returns ok=false.
func (i *Instance[S, E, C]) regionOwning(leaf S) (parallel S, r *Region[S, E, C], ok bool) {
	m := i.machine
	parent, found := i.activeParallelAncestor()
	if !found {
		var zero S
		return zero, nil, false
	}
	cur := leaf
	for {
		cn, resolved := m.resolveNode(cur)
		if !resolved || !cn.hasParent {
			break
		}
		if cn.region != "" && cn.parent == parent {
			pn, pok := m.resolveNode(parent)
			if !pok {
				break
			}
			for ri := range pn.state.Regions {
				if pn.state.Regions[ri].Name == cn.region {
					return parent, &pn.state.Regions[ri], true
				}
			}
			break
		}
		cur = cn.parent
	}
	var zero S
	return zero, nil, false
}

// marshalEventPayload renders the structured, JSON form of the event value driving
// a Fire, for the journal/replay seam: a future deterministic replay reconstructs
// the exact event from this payload rather than re-parsing the Event label. It is
// a pure, allocation-only marshal — no IO — so Fire stays pure. An event with no
// JSON encoding yields nil and the field is simply omitted, keeping the record
// additive and the trace deterministic.
func marshalEventPayload[E comparable](event E) json.RawMessage {
	raw, err := json.Marshal(event)
	if err != nil {
		return nil
	}
	return raw
}

// seedTrace builds a fresh Trace for an internal sub-step (a raised event or an
// eventless transition), tagged with the active leaf so the microstep record is
// self-describing. The trace inherits the instance's full mode so all sub-steps
// within a macrostep are internally consistent.
func (i *Instance[S, E, C]) seedTrace(event string) Trace {
	return Trace{
		Machine:   i.machine.name,
		Event:     event,
		FromState: i.machine.label(i.current),
		MatchedAt: i.machine.label(i.current),
		Outcome:   OutcomeInvalidTransition,
		full:      i.traceFull,
	}
}

// selectEventless finds one enabled eventless ("always") transition for the
// current configuration. It scans every active leaf in the configuration — not
// only config[0]'s spine — so an eventless transition resident in a non-first
// parallel region is selected rather than starved (the K5/T1a fix). Each leaf is
// resolved child-first, bubbling up through its ancestors; the first transition
// whose guards all pass is returned with the ancestor it was declared on (anc)
// and the active configuration leaf it was found under (leaf), the latter so the
// caller can route a region-resident transition through the region commit path.
//
// Leaves are scanned in configuration order, which is region-declaration order
// for an orthogonal configuration. Selection returns a single transition, so one
// macrostep microstep fires exactly one eventless transition; multiple regions'
// eventless transitions settle across successive microsteps in declaration
// order, preserving the run-to-completion determinism contract.
func (i *Instance[S, E, C]) selectEventless() (t *Transition[S, E, C], anc, leaf S, ok bool) {
	m := i.machine
	for _, l := range i.config {
		for _, a := range m.ancestors(l) {
			n, found := m.resolveNode(a)
			if !found {
				continue
			}
			for ti := range n.state.Transitions {
				cand := &n.state.Transitions[ti]
				if !cand.EventLess {
					continue
				}
				if i.guardsPass(cand) {
					return cand, a, l, true
				}
			}
		}
	}
	var zero S
	return nil, zero, zero, false
}

// guardsPass reports whether every guard on a transition currently passes. A
// guard that errors (panics) is treated as not passing, so a faulty guard never
// silently enables an eventless transition.
//
// This is the EVENTLESS (`Always`) half of the kernel's guard eval-error policy:
// an errored guard fails CLOSED (the transition is treated as not-taken and no
// error surfaces). It is deliberately asymmetric with an EVENT-DRIVEN guard, which
// fails LOUD — an error there sets OutcomeGuardPanic and is returned as a Fire
// error (see the guard evaluation in fireSpine). The kernel owns this policy; the
// expr subpackage documents the same asymmetry from its side.
func (i *Instance[S, E, C]) guardsPass(t *Transition[S, E, C]) bool {
	for _, g := range t.Guards {
		ok, err := i.machine.evalGuard(g, i.entity)
		if err != nil || !ok {
			return false
		}
	}
	if t.GuardExpr != nil {
		res := i.evalGuardExpr(t.GuardExpr, i.entity, nil)
		if res.err != nil || !res.ok {
			return false
		}
	}
	return true
}

// absorbMicrosteps folds an internal sub-step's observable trace detail into the
// macrostep's running trace, preserving order across the run-to-completion loop.
// In lite mode the sub-step carries no rich fields, so the early-return also
// avoids any append calls.
func absorbMicrosteps(dst *Trace, sub Trace) {
	if !dst.full {
		return
	}
	if sub.Event != "" {
		dst.Microsteps = append(dst.Microsteps, sub.Event)
	}
	dst.Microsteps = append(dst.Microsteps, sub.Microsteps...)
	dst.GuardsEvaluated = append(dst.GuardsEvaluated, sub.GuardsEvaluated...)
	dst.EffectsEmitted = append(dst.EffectsEmitted, sub.EffectsEmitted...)
	dst.AssignsApplied = append(dst.AssignsApplied, sub.AssignsApplied...)
	dst.ExitedStates = append(dst.ExitedStates, sub.ExitedStates...)
	dst.EnteredStates = append(dst.EnteredStates, sub.EnteredStates...)
}

// microstepOverflow returns the macrostep result annotated with the typed
// run-to-completion overflow error.
func microstepOverflow[S comparable](res FireResult[S], state S) FireResult[S] {
	res.Err = &MicrostepOverflowError{Limit: maxMicrosteps, State: fmtState(state)}
	res.Trace.Outcome = OutcomeInvalidTransition
	res.NewState = state
	return res
}

// isNoTransition reports whether err is the "no transition declared" outcome —
// the benign result of a raised event the current configuration does not handle.
func isNoTransition(err error) bool {
	var it *InvalidTransitionError
	if as(err, &it) {
		return it.Reason == "no transition declared for this state and event"
	}
	return false
}

// fireOnce is the pure single-event transition step. It resolves the event
// against the active configuration child-first, bubbling up through ancestors,
// and routes to every active orthogonal region. A flat machine collapses to a
// single leaf with no parent, so this reduces to the flat behavior.
func (i *Instance[S, E, C]) fireOnce(ctx context.Context, event E) FireResult[S] {
	m := i.machine
	from := i.current

	tr := Trace{
		Machine:   m.name,
		Event:     fmt.Sprint(event),
		FromState: m.label(from),
		MatchedAt: m.label(from),
		Outcome:   OutcomeInvalidTransition,
		full:      i.traceFull,
	}
	// EventPayload is only marshaled in full mode: it allocates and the
	// journal/replay seam is not needed when no observer reads the trace.
	if i.traceFull {
		tr.EventPayload = marshalEventPayload(event)
	}

	if _, ok := m.stateByName(from); !ok {
		if _, ok := m.resolveNode(from); !ok {
			err := &InvalidTransitionError{
				From:   fmtState(from),
				Event:  fmt.Sprint(event),
				Reason: "current state is not declared",
			}
			return FireResult[S]{NewState: from, Trace: tr, Err: err}
		}
	}

	// Orthogonal routing: if the active configuration spans multiple regions of
	// a common parallel ancestor, broadcast to every region first. This precedes
	// the final-leaf check because one region reaching its final state must not
	// block events still bound for the other regions.
	if pa, ok := i.activeParallelAncestor(); ok {
		return i.fireParallel(ctx, pa, event, tr)
	}

	// A transition out of a final leaf is rejected (runtime guard mirroring the
	// builder lint, for machines loaded from JSON).
	if n, ok := m.resolveNode(from); ok && n.state.IsFinal {
		err := &InvalidTransitionError{
			From:   fmtState(from),
			Event:  fmt.Sprint(event),
			Reason: "state is final",
		}
		return FireResult[S]{NewState: from, Trace: tr, Err: err}
	}

	return i.fireSpine(ctx, event, tr)
}

// fireSpine resolves a single active spine child-first, bubbling up through
// ancestors until a transition matches, then commits the cascade.
func (i *Instance[S, E, C]) fireSpine(ctx context.Context, event E, tr Trace) FireResult[S] {
	m := i.machine
	from := i.current
	entity := i.entity

	chain := m.ancestors(from)
	var lastGuardErr error
	sawGuardFail := false

	for _, anc := range chain {
		n, ok := m.resolveNode(anc)
		if !ok {
			continue
		}
		// A forbidden declaration consumes the event at this state and halts the
		// bubble: distinct from "no handler", which would keep climbing. The event
		// is ignored — no state change, no effects, a success outcome.
		if forbids(n.state, event) {
			tr.MatchedAt = m.label(anc)
			tr.Outcome = OutcomeSuccess
			tr.note("forbidden." + fmt.Sprint(event) + "@" + m.label(anc))
			return FireResult[S]{NewState: from, Trace: tr}
		}
		candidates := matchingTransitions(n.state, event)
		if len(candidates) == 0 {
			continue
		}
		for _, t := range candidates {
			passed := true
			for _, g := range t.Guards {
				tr.recordGuard(g.Name)
				ok, gErr := m.evalGuard(g, entity)
				if gErr != nil {
					tr.Outcome = OutcomeGuardPanic
					return FireResult[S]{NewState: from, Trace: tr, Err: gErr}
				}
				if !ok {
					passed = false
					sawGuardFail = true
					lastGuardErr = &GuardFailedError{GuardName: g.Name, Reason: "predicate returned false"}
					break
				}
			}
			// A composite guard expression is evaluated only when the plain guards
			// pass; the transition is enabled when both do. A leaf panic surfaces as
			// a guard panic; a clean false records which leaf(s) failed when cheap,
			// else the composite.
			if passed && t.GuardExpr != nil {
				res := i.evalGuardExpr(t.GuardExpr, entity, &tr)
				if res.err != nil {
					tr.Outcome = OutcomeGuardPanic
					return FireResult[S]{NewState: from, Trace: tr, Err: res.err}
				}
				if !res.ok {
					passed = false
					sawGuardFail = true
					lastGuardErr = &GuardFailedError{
						GuardName: joinLeafs(res.failedLeafs),
						Reason:    "composite guard failed",
					}
				}
			}
			if passed {
				tr.MatchedAt = m.label(anc)
				return i.commit(ctx, t, from, anc, entity, i.eventData(event), tr)
			}
		}
	}

	if sawGuardFail {
		tr.Outcome = OutcomeGuardFailed
		if lastGuardErr == nil {
			lastGuardErr = &GuardFailedError{Reason: "all candidate transitions failed their guards"}
		}
		return FireResult[S]{NewState: from, Trace: tr, Err: lastGuardErr}
	}

	err := &InvalidTransitionError{
		From:   fmtState(from),
		Event:  fmt.Sprint(event),
		Reason: "no transition declared for this state and event",
	}
	return FireResult[S]{NewState: from, Trace: tr, Err: err}
}

// matchingTransitions returns the event-triggered transitions of a state for an
// event, in priority order: every specific On-keyed match in declaration order
// first, then every wildcard catch-all in declaration order. Eventless and
// forbidden transitions are not returned here (forbidden is resolved separately,
// before candidates are tried). Specific
// events outrank the wildcard — and the wildcard outranks bubbling to an
// ancestor.
func matchingTransitions[S comparable, E comparable, C any](s *State[S, E, C], event E) []*Transition[S, E, C] {
	// The overwhelmingly common case is a state with one or more specific matches
	// and no wildcard. Collect specifics first; only allocate a second slice and
	// merge when a wildcard is actually present, so the steady-state path returns a
	// single slice with no merge allocation. A state with no matches returns nil.
	var specific, wild []*Transition[S, E, C]
	for ti := range s.Transitions {
		t := &s.Transitions[ti]
		if t.EventLess || t.Forbidden {
			continue
		}
		switch {
		case t.Wildcard:
			wild = append(wild, t)
		case t.On == event:
			specific = append(specific, t)
		}
	}
	if len(wild) == 0 {
		return specific
	}
	if len(specific) == 0 {
		return wild
	}
	out := make([]*Transition[S, E, C], 0, len(specific)+len(wild))
	out = append(out, specific...)
	return append(out, wild...)
}

// forbids reports whether state s explicitly forbids event: a transition marked
// Forbidden that keys on this event (a specific Forbidden) or a forbidden
// wildcard. A forbidden event is consumed at this state and must not bubble to
// ancestors.
func forbids[S comparable, E comparable, C any](s *State[S, E, C], event E) bool {
	for ti := range s.Transitions {
		t := &s.Transitions[ti]
		if !t.Forbidden {
			continue
		}
		if t.Wildcard || t.On == event {
			return true
		}
	}
	return false
}

// commit advances the configuration (before running actions, per the locked
// decision) and runs the exit cascade, the transition's bound actions, and the
// entry cascade — building effects and recording the trace. matchedAt is the
// ancestor whose transition fired (equal to the source leaf for a flat machine).
func (i *Instance[S, E, C]) commit(
	ctx context.Context,
	t *Transition[S, E, C],
	from S,
	matchedAt S,
	entity C,
	eventData any,
	tr Trace,
) FireResult[S] {
	_ = ctx
	m := i.machine

	// SelectedTransition calls projectTransition which allocates several slices;
	// skip it entirely in lite mode.
	if tr.full {
		tr.SelectedTransition = projectTransition(t)
	}

	// cur threads the context by value through the cascade. Actions in a phase read
	// cur as it stood at phase entry (read-only); the assigns of that phase then
	// fold cur, each reducer seeing the prior result. The folded value is committed
	// to the instance at the end — the sole context-mutation site (G1).
	cur := entity

	if i.isInternal(t, from) {
		// Internal transition: run effects without exiting/re-entering the source
		// or cascading. This is the v5 default for a self- or ancestor-targeted
		// transition without Reenter, and the explicit Internal form.
		effects, errName, err := i.runActions(t.Effects, cur, &tr)
		if err != nil {
			tr.Outcome = OutcomeEffectError
			// Effects are emitted only on a fully-successful Fire (the NTE
			// transactionality contract): a failed Fire returns no partial effects so
			// a host replaying it cannot double-apply the ones that ran before the
			// error. Config is intentionally not rolled back (out of scope); only the
			// effect emission is transactional.
			return FireResult[S]{
				NewState: i.current, Effects: nil, Trace: tr,
				Err: &ActionFailedError{
					TransitionName: fmt.Sprintf("%s->%s", fmtState(from), fmtState(from)),
					ActionName:     errName, Cause: err,
				},
			}
		}
		next, aName, aErr := i.applyAssigns(t.Assigns, cur, eventData, &tr)
		if aErr != nil {
			tr.Outcome = OutcomeAssignFailed
			return FireResult[S]{
				NewState: i.current, Effects: nil, Trace: tr,
				Err: &ActionFailedError{
					TransitionName: fmt.Sprintf("%s->%s", fmtState(from), fmtState(from)),
					ActionName:     aName, Cause: aErr,
				},
			}
		}
		i.entity = next
		i.enqueueRaised(t, &tr)
		tr.Outcome = OutcomeSuccess
		return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr}
	}

	to := t.To

	// A history pseudo-state target re-enters the remembered configuration of its
	// owning compound (or the declared default / the compound's initial when no
	// history is recorded yet). Resolve it to the concrete leaves and target the
	// owning compound for the cascade; restoreLeaves is non-nil for a history
	// target and pins the configuration after entry.
	var restoreLeaves []S
	if leaves, owner, isHist := i.resolveHistory(to); isHist {
		restoreLeaves = leaves
		to = owner
	}

	// Compute the exit/entry cascade across the hierarchy. A reentering self- or
	// ancestor-targeted transition is external on its own subtree: it exits from
	// the source up to and including the target, then re-enters the target (and
	// descends back into its initial children). The standard LCA cascade would
	// produce an empty set here because the target is its own least common
	// ancestor, so this case is computed explicitly.
	var exits, entries []S
	if reentersSelfOrAncestor(t, from, to, m) {
		exits, entries = m.reenterCascade(from, to)
	} else {
		exits, entries = m.cascade(from, to)
	}
	if restoreLeaves != nil {
		// Replace the compound's default descent with the remembered descent: the
		// entry chain up to and including the compound is kept, then the recorded
		// leaves (and their interior spine) are entered instead of InitialChild.
		entries = m.entryChainTo(from, to)
		entries = append(entries, m.restoreInterior(to, restoreLeaves)...)
	}

	var effects []Effect

	// Record the history of every compound being exited before the configuration
	// advances, so a later history-targeted entry can restore it.
	i.recordHistory(exits, i.config)

	// Exit actions then exit assigns: innermost -> outermost. Each state's exit
	// actions read cur (pre-assign), then its exit assigns fold cur.
	//
	// A cross-cutting transition that exits a parallel superstate names only the
	// matched state's spine in the structural cascade; the orthogonal regions'
	// active leaves leave the configuration too and must run their OnExit actions.
	// exitActionChain interleaves each exited parallel's active region-leaf spines
	// (region-declaration order, innermost-leaf-first) before the parallel state's
	// own OnExit, so the full exit-action set runs in the locked order.
	for _, s := range i.exitActionChain(exits) {
		tr.recordExit(m.label(s))
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		eff, errName, err := i.runActions(n.state.OnExit, cur, &tr)
		effects = append(effects, eff...)
		if err != nil {
			tr.Outcome = OutcomeEffectError
			return FireResult[S]{
				NewState: i.current, Effects: nil, Trace: tr,
				Err: &ActionFailedError{TransitionName: transName(from, to), ActionName: errName, Cause: err},
			}
		}
		next, aName, aErr := i.applyAssigns(n.state.OnExitAssign, cur, eventData, &tr)
		if aErr != nil {
			tr.Outcome = OutcomeAssignFailed
			return FireResult[S]{
				NewState: i.current, Effects: nil, Trace: tr,
				Err: &ActionFailedError{TransitionName: transName(from, to), ActionName: aName, Cause: aErr},
			}
		}
		cur = next
	}

	// Lifecycle cleanup spans every leaf actually leaving the configuration, not just
	// the from-chain in the structural exit cascade. Exiting a parallel superstate
	// abandons the orthogonal regions too, so their armed `after` timers, in-flight
	// services, and running actors must be canceled/stopped alongside the primary
	// leaf's — otherwise a cross-cutting exit from a parallel state would leak the
	// sibling regions' timers and drivers.
	cleanupExits := i.lifecycleExits(exits)

	// Auto-cancel-on-exit: every exited state that armed a delayed (`after`)
	// timer emits a CancelScheduled effect so the host drops the pending timer.
	effects = append(effects, i.afterEffectsOnExit(cleanupExits, &tr)...)

	// Auto-stop-on-exit: every exited state with an in-flight invoked service
	// emits a StopService effect so the host stops the service.
	effects = append(effects, i.invokeEffectsOnExit(cleanupExits, &tr)...)

	// Auto-stop-on-exit: every exited state running a child-machine actor emits a
	// StopActor effect so the host's ActorSystem stops the actor (and its children).
	effects = append(effects, i.actorEffectsOnExit(cleanupExits, &tr)...)

	// Advance the configuration before transition/entry actions run. A history
	// restore pins the configuration to the remembered leaves; otherwise descend
	// into the target's initial children.
	if restoreLeaves != nil {
		i.config = append([]S(nil), restoreLeaves...)
		i.current = i.config[0]
	} else {
		i.current = to
		i.config = m.descendToLeaves(to)
		if len(i.config) > 0 {
			i.current = i.config[0]
		} else {
			i.config = []S{to}
		}
	}

	// Transition effects (read cur, pre-assign) then transition assigns (fold cur).
	eff, errName, err := i.runActions(t.Effects, cur, &tr)
	effects = append(effects, eff...)
	if err != nil {
		tr.Outcome = OutcomeEffectError
		return FireResult[S]{
			NewState: i.current, Effects: nil, Trace: tr,
			Err: &ActionFailedError{TransitionName: transName(from, to), ActionName: errName, Cause: err},
		}
	}
	tnext, taName, taErr := i.applyAssigns(t.Assigns, cur, eventData, &tr)
	if taErr != nil {
		tr.Outcome = OutcomeAssignFailed
		return FireResult[S]{
			NewState: i.current, Effects: nil, Trace: tr,
			Err: &ActionFailedError{TransitionName: transName(from, to), ActionName: taName, Cause: taErr},
		}
	}
	cur = tnext

	// Entry actions then entry assigns: outermost -> innermost. Each state's entry
	// actions read cur (pre-assign), then its entry assigns fold cur.
	for _, s := range entries {
		tr.recordEntry(m.label(s))
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		eff, errName, err := i.runActions(n.state.OnEntry, cur, &tr)
		effects = append(effects, eff...)
		if err != nil {
			tr.Outcome = OutcomeEffectError
			return FireResult[S]{
				NewState: i.current, Effects: nil, Trace: tr,
				Err: &ActionFailedError{TransitionName: transName(from, to), ActionName: errName, Cause: err},
			}
		}
		next, aName, aErr := i.applyAssigns(n.state.OnEntryAssign, cur, eventData, &tr)
		if aErr != nil {
			tr.Outcome = OutcomeAssignFailed
			return FireResult[S]{
				NewState: i.current, Effects: nil, Trace: tr,
				Err: &ActionFailedError{TransitionName: transName(from, to), ActionName: aName, Cause: aErr},
			}
		}
		cur = next
	}

	// Delayed-transition scheduling: every entered state that declares an
	// `after` transition emits a ScheduleAfter effect so the host arms a timer.
	effects = append(effects, i.afterEffectsOnEntry(entries, &tr)...)

	// Invoked services: every entered state that declares an `invoke` emits a
	// StartService effect so the host runs the service and routes onDone/onError.
	effects = append(effects, i.invokeEffectsOnEntry(entries, &tr)...)

	// Child-machine actors: every entered state that invokes a child machine emits
	// a SpawnActor effect so the host's ActorSystem runs it and routes done/error.
	effects = append(effects, i.actorEffectsOnEntry(entries, &tr)...)

	// Done-event semantics: entering a final leaf may complete its parent. OnDone
	// actions read the folded context (cur), consistent with every other action
	// reading context read-only.
	doneEff, dname, derr := i.settleDone(to, cur, &tr)
	effects = append(effects, doneEff...)
	if derr != nil {
		tr.Outcome = OutcomeEffectError
		return FireResult[S]{
			NewState: i.current, Effects: nil, Trace: tr,
			Err: &ActionFailedError{TransitionName: transName(from, to), ActionName: dname, Cause: derr},
		}
	}

	// Commit the folded context to the instance — the sole context-mutation site.
	i.entity = cur
	i.enqueueRaised(t, &tr)
	tr.Outcome = OutcomeSuccess
	return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr}
}

// isInternal reports whether transition t executes as an internal transition
// from source leaf `from`: it runs effects without exiting and re-entering the
// source. A transition is internal when the explicit Internal flag is set, or —
// by default — when Reenter is unset and the target is the source
// itself or one of its ancestors. An external transition (Reenter set, or a
// target outside the source's own spine) always runs the exit/entry cascade.
func (i *Instance[S, E, C]) isInternal(t *Transition[S, E, C], from S) bool {
	if t.Internal {
		return true
	}
	if t.Reenter {
		return false
	}
	// Targetless wildcard/self edges: a transition whose target equals the source,
	// or whose target is an ancestor of the source, is internal by default.
	if t.To == from {
		return true
	}
	for _, anc := range i.machine.ancestors(from) {
		if anc == from {
			continue
		}
		if anc == t.To {
			return true
		}
	}
	return false
}

// enqueueRaised appends a transition's raised internal events to the instance's
// macrostep queue and records each in the trace. The queue is drained by Fire's
// run-to-completion loop within the same macrostep; it is local to that loop, so
// Fire performs no IO and stays pure.
func (i *Instance[S, E, C]) enqueueRaised(t *Transition[S, E, C], tr *Trace) {
	for _, ev := range t.Raise {
		i.raised = append(i.raised, ev)
		tr.note("raise." + fmt.Sprint(ev))
	}
}

// transName renders a transition label for diagnostics. Sites that hold a
// machine pointer should call m.label instead; this free function is retained
// for cascade.go and other files that pass already-formatted strings.
func transName[S comparable](from, to S) string {
	return fmt.Sprintf("%s->%s", fmtState(from), fmtState(to))
}

// runActions resolves and runs a list of action refs, appending effect names to
// the trace. On the first failure it returns the effects gathered so far, the
// failing action's name, and the cause.
func (i *Instance[S, E, C]) runActions(refs []Ref, entity C, tr *Trace) (effects []Effect, name string, err error) {
	for _, a := range refs {
		e, aerr := i.machine.evalAction(a, entity)
		if aerr != nil {
			tr.recordEffect(a.Name)
			return effects, a.Name, aerr
		}
		effects = append(effects, e)
		// recordEffect/note are no-ops in lite mode, so build the formatted effect
		// label and probe the comm microstep only when the full trace will keep them.
		// This keeps the default (lite) hot path free of the Sprintf and effectLabel
		// allocation on every action.
		if tr.full {
			tr.recordEffect(fmt.Sprintf("%s:%s", a.Name, effectLabel(e)))
			if ms, ok := commMicrostep(e); ok {
				tr.note(ms)
			}
		}
	}
	return effects, "", nil
}

// evalGuard resolves and runs a guard ref, recovering panics into GuardPanicError.
func (m *Machine[S, E, C]) evalGuard(g Ref, entity C) (ok bool, err error) {
	fn, found := m.guards[g.Name]
	if !found {
		// Unbound refs are caught at Quench; defensively treat as a guard panic.
		return false, &GuardPanicError{GuardName: g.Name, Recovered: "unbound guard at fire time"}
	}
	defer func() {
		if r := recover(); r != nil {
			ok = false
			err = &GuardPanicError{GuardName: g.Name, Recovered: r}
		}
	}()
	return fn(GuardCtx[C]{Entity: entity, Params: g.Params}), nil
}

// evalAction resolves and runs an action ref, recovering a panicking host action
// into a typed ActionPanicError so a faulty action fails the fire deterministically
// rather than crashing Fire. Kernel built-in actions (e.g. the Cancel built-in) are
// handled directly without consulting the host registry.
func (m *Machine[S, E, C]) evalAction(a Ref, entity C) (eff Effect, err error) {
	if isBuiltinAction(a.Name) {
		return evalBuiltinAction(a)
	}
	fn, found := m.actions[a.Name]
	if !found {
		return nil, fmt.Errorf("unbound action %q at fire time", a.Name)
	}
	defer func() {
		if r := recover(); r != nil {
			eff = nil
			err = &ActionPanicError{ActionName: a.Name, Recovered: r}
		}
	}()
	return fn(ActionCtx[C]{Entity: entity, Params: a.Params})
}

// applyAssigns folds a list of assign refs onto cur in declaration order, each
// reducer seeing the prior result, and records every reduced ref in the trace. It
// is the sole context-mutation path: the returned context replaces cur for the
// next phase of the cascade. A failing (panicking) reducer surfaces as a typed
// error that stops the commit, mirroring a guard panic.
func (i *Instance[S, E, C]) applyAssigns(refs []Ref, cur C, eventData any, tr *Trace) (C, string, error) {
	for _, a := range refs {
		next, err := i.machine.evalAssign(a, cur, eventData)
		if err != nil {
			tr.recordAssign(a.Name)
			return cur, a.Name, err
		}
		cur = next
		tr.recordAssign(a.Name)
	}
	return cur, "", nil
}

// evalAssign resolves and runs an assign ref, folding the prior context into the
// next. A reducer panic is recovered into a typed AssignPanicError so a faulty
// reducer fails the commit deterministically rather than corrupting context.
func (m *Machine[S, E, C]) evalAssign(a Ref, cur C, eventData any) (next C, err error) {
	fn, found := m.assigns[a.Name]
	if !found {
		return cur, &AssignPanicError{AssignName: a.Name, Recovered: "unbound assign at fire time"}
	}
	defer func() {
		if r := recover(); r != nil {
			next = cur
			err = &AssignPanicError{AssignName: a.Name, Recovered: r}
		}
	}()
	return fn(AssignCtx[C]{Entity: cur, Event: eventData, Params: a.Params}), nil
}

// projectTransition erases the generic parameters of a Transition into the
// any-typed shape the Trace exposes, preserving the observable fields.
func projectTransition[S comparable, E comparable, C any](t *Transition[S, E, C]) *Transition[any, any, any] {
	if t == nil {
		return nil
	}
	guards := append([]Ref(nil), t.Guards...)
	effects := append([]Ref(nil), t.Effects...)
	var raise []any
	for _, ev := range t.Raise {
		raise = append(raise, ev)
	}
	var guardExpr *GuardNode[any]
	if t.GuardExpr != nil {
		guardExpr = projectGuardNode(t.GuardExpr)
	}
	return &Transition[any, any, any]{
		From:      t.From,
		To:        t.To,
		On:        t.On,
		Guards:    guards,
		GuardExpr: guardExpr,
		Effects:   effects,
		WaitMode:  t.WaitMode,
		Internal:  t.Internal,
		EventLess: t.EventLess,
		After:     t.After,
		Wildcard:  t.Wildcard,
		Forbidden: t.Forbidden,
		Reenter:   t.Reenter,
		Raise:     raise,
		SrcFile:   t.SrcFile,
		SrcLine:   t.SrcLine,
	}
}

// FireSeq drives a sequence of events into one instance, threading intermediate
// state and merging the per-step traces into one ordered Trace. It is the
// many-events form of Fire; to fan a single event across many instances use the
// top-level FireEach.
func (i *Instance[S, E, C]) FireSeq(ctx context.Context, events []E, opts ...FireOption) BatchResult[S] {
	cfg := fireConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	var br BatchResult[S]
	merged := Trace{Machine: i.machine.name, Outcome: OutcomeSuccess, full: i.traceFull}
	for _, ev := range events {
		res := i.Fire(ctx, ev, opts...)
		br.Steps = append(br.Steps, res)
		mergeTrace(&merged, res.Trace)
		if res.Err != nil {
			if br.Err == nil {
				br.Err = res.Err
			}
			if merged.Outcome == OutcomeSuccess {
				merged.Outcome = res.Trace.Outcome
			}
			if !cfg.collectAll {
				break
			}
		}
	}
	br.Trace = merged
	return br
}

// mergeTrace appends one step's trace into the running merged trace, preserving
// order across the batch. In lite mode the per-step rich fields are empty so the
// early-return avoids spurious append calls.
func mergeTrace(dst *Trace, step Trace) {
	if !dst.full {
		return
	}
	if step.Event != "" {
		dst.Microsteps = append(dst.Microsteps, step.Event)
	}
	dst.GuardsEvaluated = append(dst.GuardsEvaluated, step.GuardsEvaluated...)
	dst.PoliciesEvaluated = append(dst.PoliciesEvaluated, step.PoliciesEvaluated...)
	dst.EffectsEmitted = append(dst.EffectsEmitted, step.EffectsEmitted...)
}

// FireEach fans one event across an explicit set of instances, preserving
// per-instance attribution. It is the many-instances counterpart to
// Instance.Fire (one event, one instance) and Instance.FireSeq (many events, one
// instance).
func FireEach[S comparable, E comparable, C any](
	ctx context.Context, instances []*Instance[S, E, C], event E, opts ...FireOption,
) []FireResult[S] {
	cfg := fireConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	out := make([]FireResult[S], 0, len(instances))
	for _, inst := range instances {
		res := inst.Fire(ctx, event, opts...)
		out = append(out, res)
		if res.Err != nil && !cfg.collectAll {
			break
		}
	}
	return out
}
