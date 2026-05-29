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

// fireCore is the pure transition step.
func (i *Instance[S, E, C]) fireCore(ctx context.Context, event E) FireResult[S] {
	m := i.machine
	from := i.current
	entity := i.entity

	tr := Trace{
		Machine:   m.name,
		Event:     fmt.Sprint(event),
		FromState: fmtState(from),
		Outcome:   OutcomeInvalidTransition,
	}

	src, ok := m.stateByName(from)
	if !ok {
		err := &ErrInvalidTransition{
			From:   fmtState(from),
			Event:  fmt.Sprint(event),
			Reason: "current state is not declared",
		}
		return FireResult[S]{NewState: from, Trace: tr, Err: err}
	}

	// Collect candidates matching (current, event).
	var candidates []*Transition[S, E, C]
	for ti := range src.Transitions {
		t := &src.Transitions[ti]
		if t.EventLess {
			continue
		}
		if t.On == event {
			candidates = append(candidates, t)
		}
	}
	if len(candidates) == 0 {
		err := &ErrInvalidTransition{
			From:   fmtState(from),
			Event:  fmt.Sprint(event),
			Reason: "no transition declared for this state and event",
		}
		return FireResult[S]{NewState: from, Trace: tr, Err: err}
	}

	// Evaluate guards per candidate, left-to-right; first all-pass wins.
	var lastGuardErr error
	for _, t := range candidates {
		passed := true
		for _, g := range t.Guards {
			tr.GuardsEvaluated = append(tr.GuardsEvaluated, g.Name)
			ok, gErr := m.evalGuard(g, entity)
			if gErr != nil {
				// Guard panic recovered into a typed error.
				tr.Outcome = OutcomeGuardPanic
				return FireResult[S]{NewState: from, Trace: tr, Err: gErr}
			}
			if !ok {
				passed = false
				lastGuardErr = &ErrGuardFailed{
					GuardName: g.Name,
					Reason:    "predicate returned false",
				}
				break
			}
		}
		if passed {
			return i.commit(ctx, t, from, entity, tr)
		}
	}

	// No candidate passed its guards.
	tr.Outcome = OutcomeGuardFailed
	if lastGuardErr == nil {
		lastGuardErr = &ErrGuardFailed{Reason: "all candidate transitions failed their guards"}
	}
	return FireResult[S]{NewState: from, Trace: tr, Err: lastGuardErr}
}

// commit advances the state (before running actions, per the locked decision)
// and runs the transition's bound actions, building effects and recording the
// trace.
func (i *Instance[S, E, C]) commit(
	ctx context.Context,
	t *Transition[S, E, C],
	from S,
	entity C,
	tr Trace,
) FireResult[S] {
	_ = ctx
	to := t.To
	if t.Internal {
		to = from
	}

	tr.SelectedTransition = projectTransition(t)

	// State advances before actions run.
	if !t.Internal {
		i.current = to
	}

	var effects []Effect
	for _, a := range t.Effects {
		eff, err := i.machine.evalAction(a, entity)
		if err != nil {
			tr.Outcome = OutcomeEffectError
			tr.EffectsEmitted = append(tr.EffectsEmitted, a.Name)
			return FireResult[S]{
				NewState: i.current,
				Effects:  effects,
				Trace:    tr,
				Err: &ErrActionFailed{
					TransitionName: fmt.Sprintf("%s->%s", fmtState(from), fmtState(to)),
					ActionName:     a.Name,
					Cause:          err,
				},
			}
		}
		effects = append(effects, eff)
		tr.EffectsEmitted = append(tr.EffectsEmitted, fmt.Sprintf("%s:%s", a.Name, typeName(eff)))
	}

	tr.Outcome = OutcomeSuccess
	return FireResult[S]{NewState: i.current, Effects: effects, Trace: tr}
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
