package state

import (
	"context"
	"fmt"
)

// Guards and actions receive the entity the Instance was Cast with; the kernel
// supplies it from Instance.entity at Fire time. The entity is never threaded
// through context.
//
// The kernel keeps Fire pure: it never reads the clock, never performs IO, and
// the only state it advances is the Instance.current field.

// Fire runs the full transition pipeline for a single event.
func (i *Instance[S, E, C]) Fire(ctx context.Context, event E, opts ...FireOption) FireResult[S] {
	cfg := fireConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return i.fireWithMiddleware(ctx, event)
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
	i.history = append(i.history, res.Trace)
	return res
}

// fireCore is the pure transition step. It resolves the event against the
// active configuration child-first, bubbling up through ancestors, and routes
// to every active orthogonal region. A flat machine collapses to a single leaf
// with no parent, so this reduces to the flat behavior.
func (i *Instance[S, E, C]) fireCore(ctx context.Context, event E) FireResult[S] {
	m := i.machine
	from := i.current

	tr := Trace{
		Machine:   m.name,
		Event:     fmt.Sprint(event),
		FromState: fmtState(from),
		MatchedAt: fmtState(from),
		Outcome:   OutcomeInvalidTransition,
	}

	if _, ok := m.stateByName(from); !ok {
		if _, ok := m.resolveNode(from); !ok {
			err := &ErrInvalidTransition{
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
		err := &ErrInvalidTransition{
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
		candidates := matchingTransitions(n.state, event)
		if len(candidates) == 0 {
			continue
		}
		for _, t := range candidates {
			passed := true
			for _, g := range t.Guards {
				tr.GuardsEvaluated = append(tr.GuardsEvaluated, g.Name)
				ok, gErr := m.evalGuard(g, entity)
				if gErr != nil {
					tr.Outcome = OutcomeGuardPanic
					return FireResult[S]{NewState: from, Trace: tr, Err: gErr}
				}
				if !ok {
					passed = false
					sawGuardFail = true
					lastGuardErr = &ErrGuardFailed{GuardName: g.Name, Reason: "predicate returned false"}
					break
				}
			}
			if passed {
				tr.MatchedAt = fmtState(anc)
				return i.commit(ctx, t, from, anc, entity, tr)
			}
		}
	}

	if sawGuardFail {
		tr.Outcome = OutcomeGuardFailed
		if lastGuardErr == nil {
			lastGuardErr = &ErrGuardFailed{Reason: "all candidate transitions failed their guards"}
		}
		return FireResult[S]{NewState: from, Trace: tr, Err: lastGuardErr}
	}

	err := &ErrInvalidTransition{
		From:   fmtState(from),
		Event:  fmt.Sprint(event),
		Reason: "no transition declared for this state and event",
	}
	return FireResult[S]{NewState: from, Trace: tr, Err: err}
}

// matchingTransitions returns the event-triggered (non-eventless) transitions of
// a state in declaration order.
func matchingTransitions[S comparable, E comparable, C any](s *State[S, E, C], event E) []*Transition[S, E, C] {
	var out []*Transition[S, E, C]
	for ti := range s.Transitions {
		t := &s.Transitions[ti]
		if t.EventLess {
			continue
		}
		if t.On == event {
			out = append(out, t)
		}
	}
	return out
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
	tr Trace,
) FireResult[S] {
	_ = ctx
	m := i.machine

	tr.SelectedTransition = projectTransition(t)

	if t.Internal {
		// Internal transitions run effects without changing state or cascading.
		effects, errName, err := i.runActions(t.Effects, entity, &tr)
		if err != nil {
			tr.Outcome = OutcomeEffectError
			return FireResult[S]{
				NewState: i.current, Effects: effects, Trace: tr,
				Err: &ErrActionFailed{
					TransitionName: fmt.Sprintf("%s->%s", fmtState(from), fmtState(from)),
					ActionName:     errName, Cause: err,
				},
			}
		}
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

	// Compute the exit/entry cascade across the hierarchy.
	exits, entries := m.cascade(from, to)
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

	// Exit actions: innermost -> outermost.
	for _, s := range exits {
		tr.ExitedStates = append(tr.ExitedStates, fmtState(s))
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		eff, errName, err := i.runActions(n.state.OnExit, entity, &tr)
		effects = append(effects, eff...)
		if err != nil {
			tr.Outcome = OutcomeEffectError
			return FireResult[S]{
				NewState: i.current, Effects: effects, Trace: tr,
				Err: &ErrActionFailed{TransitionName: transName(from, to), ActionName: errName, Cause: err},
			}
		}
	}

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

	// Transition effects.
	eff, errName, err := i.runActions(t.Effects, entity, &tr)
	effects = append(effects, eff...)
	if err != nil {
		tr.Outcome = OutcomeEffectError
		return FireResult[S]{
			NewState: i.current, Effects: effects, Trace: tr,
			Err: &ErrActionFailed{TransitionName: transName(from, to), ActionName: errName, Cause: err},
		}
	}

	// Entry actions: outermost -> innermost.
	for _, s := range entries {
		tr.EnteredStates = append(tr.EnteredStates, fmtState(s))
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		eff, errName, err := i.runActions(n.state.OnEntry, entity, &tr)
		effects = append(effects, eff...)
		if err != nil {
			tr.Outcome = OutcomeEffectError
			return FireResult[S]{
				NewState: i.current, Effects: effects, Trace: tr,
				Err: &ErrActionFailed{TransitionName: transName(from, to), ActionName: errName, Cause: err},
			}
		}
	}

	// Done-event semantics: entering a final leaf may complete its parent.
	doneEff, dname, derr := i.settleDone(to, entity, &tr)
	effects = append(effects, doneEff...)
	if derr != nil {
		tr.Outcome = OutcomeEffectError
		return FireResult[S]{
			NewState: i.current, Effects: effects, Trace: tr,
			Err: &ErrActionFailed{TransitionName: transName(from, to), ActionName: dname, Cause: derr},
		}
	}

	tr.Outcome = OutcomeSuccess
	return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr}
}

// transName renders a transition label for diagnostics.
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
			tr.EffectsEmitted = append(tr.EffectsEmitted, a.Name)
			return effects, a.Name, aerr
		}
		effects = append(effects, e)
		tr.EffectsEmitted = append(tr.EffectsEmitted, fmt.Sprintf("%s:%s", a.Name, typeName(e)))
	}
	return effects, "", nil
}

// evalGuard resolves and runs a guard ref, recovering panics into ErrGuardPanic.
func (m *Machine[S, E, C]) evalGuard(g Ref, entity C) (ok bool, err error) {
	fn, found := m.guards[g.Name]
	if !found {
		// Unbound refs are caught at Quench; defensively treat as a guard panic.
		return false, &ErrGuardPanic{GuardName: g.Name, Recovered: "unbound guard at fire time"}
	}
	defer func() {
		if r := recover(); r != nil {
			ok = false
			err = &ErrGuardPanic{GuardName: g.Name, Recovered: r}
		}
	}()
	return fn(GuardCtx[C]{Entity: entity, Params: g.Params}), nil
}

// evalAction resolves and runs an action ref.
func (m *Machine[S, E, C]) evalAction(a Ref, entity C) (Effect, error) {
	fn, found := m.actions[a.Name]
	if !found {
		return nil, fmt.Errorf("unbound action %q at fire time", a.Name)
	}
	return fn(ActionCtx[C]{Entity: entity, Params: a.Params})
}

// projectTransition erases the generic parameters of a Transition into the
// any-typed shape the Trace exposes, preserving the observable fields.
func projectTransition[S comparable, E comparable, C any](t *Transition[S, E, C]) *Transition[any, any, any] {
	if t == nil {
		return nil
	}
	guards := append([]Ref(nil), t.Guards...)
	effects := append([]Ref(nil), t.Effects...)
	return &Transition[any, any, any]{
		From:      t.From,
		To:        t.To,
		On:        t.On,
		Guards:    guards,
		Effects:   effects,
		WaitMode:  t.WaitMode,
		Internal:  t.Internal,
		EventLess: t.EventLess,
		After:     t.After,
		SrcFile:   t.SrcFile,
		SrcLine:   t.SrcLine,
	}
}

// FireSeq drives a sequence of events into one instance, threading intermediate
// state and merging the per-step traces into one ordered Trace.
func (i *Instance[S, E, C]) FireSeq(ctx context.Context, events []E, opts ...FireOption) BatchResult[S] {
	cfg := fireConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	var br BatchResult[S]
	merged := Trace{Machine: i.machine.name, Outcome: OutcomeSuccess}
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
// order across the batch.
func mergeTrace(dst *Trace, step Trace) {
	if step.Event != "" {
		dst.Microsteps = append(dst.Microsteps, step.Event)
	}
	dst.GuardsEvaluated = append(dst.GuardsEvaluated, step.GuardsEvaluated...)
	dst.PoliciesEvaluated = append(dst.PoliciesEvaluated, step.PoliciesEvaluated...)
	dst.EffectsEmitted = append(dst.EffectsEmitted, step.EffectsEmitted...)
}

// FireEach fans one event across an explicit set of instances, preserving
// per-instance attribution.
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
