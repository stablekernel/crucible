package main

import (
	"context"

	"github.com/stablekernel/crucible/state"
)

// stubRegistry walks an IR to enumerate every referenced behavior name by kind
// and registers a deterministic no-op for each into a fresh registry. A
// structural IR carries behavior references by name only (state.Ref); the kernel
// binds those names to implementations at Quench time and panics on any unbound
// ref. render, lint, and validate care only about a machine's structure, not its
// behavior, so a no-op stub for every referenced name lets Provide(reg).Quench()
// produce a *Machine without real implementations.
//
// The stubs are total and side-effect free: a guard returns false, an action
// returns a zero effect, a reducer returns its input context unchanged, and a
// service returns nil. None of these run during render/lint/validate (no instance
// is cast and no event is fired), so their bodies only need to satisfy binding.
func stubRegistry(ir *state.IR[string, string, any]) *state.Registry[any] {
	var b behaviorNames
	for i := range ir.States {
		b.walkState(&ir.States[i])
	}

	reg := state.NewRegistry[any]()
	for name := range b.guards {
		reg.Guard(name, func(state.GuardCtx[any]) bool { return false })
	}
	for name := range b.actions {
		reg.Action(name, func(state.ActionCtx[any]) (state.Effect, error) { return nil, nil })
	}
	for name := range b.reducers {
		reg.Reducer(name, func(in state.AssignCtx[any]) any { return in.Entity })
	}
	for name := range b.services {
		reg.Service(name, func(context.Context, state.ServiceCtx[any]) (any, error) { return nil, nil })
	}
	return reg
}

// behaviorNames accumulates the distinct behavior names referenced by an IR,
// bucketed by the registry slot each kind binds against.
type behaviorNames struct {
	guards   set
	actions  set
	reducers set
	services set
}

type set map[string]struct{}

func (s *set) add(name string) {
	if name == "" {
		return
	}
	if *s == nil {
		*s = set{}
	}
	(*s)[name] = struct{}{}
}

// walkState records every behavior reference on a state and recurses through its
// transitions, invocations, children, and regions. The kind mapping follows the
// engine's registries: entry/exit/done actions and transition effects are
// ACTIONS; entry/exit/transition assigns are REDUCERS; transition guards and the
// named-ref leaves of a composite guard expression are GUARDS; an invocation's
// Src is a SERVICE. An invocation's OnDone/OnError are events, not behaviors, so
// they are not enumerated.
func (b *behaviorNames) walkState(s *state.State[string, string, any]) {
	for _, r := range s.OnEntry {
		b.actions.add(r.Name)
	}
	for _, r := range s.OnExit {
		b.actions.add(r.Name)
	}
	for _, r := range s.OnDone {
		b.actions.add(r.Name)
	}
	for _, r := range s.OnEntryAssign {
		b.reducers.add(r.Name)
	}
	for _, r := range s.OnExitAssign {
		b.reducers.add(r.Name)
	}
	for i := range s.Transitions {
		b.walkTransition(&s.Transitions[i])
	}
	for i := range s.Invoke {
		b.services.add(s.Invoke[i].Src.Name)
	}
	for i := range s.Children {
		b.walkState(&s.Children[i])
	}
	for i := range s.Regions {
		for j := range s.Regions[i].States {
			b.walkState(&s.Regions[i].States[j])
		}
	}
}

// walkTransition records the guard, effect (action), and assign (reducer)
// references on one edge, including the named-ref leaves of a composite guard
// expression.
func (b *behaviorNames) walkTransition(t *state.Transition[string, string, any]) {
	for _, r := range t.Guards {
		b.guards.add(r.Name)
	}
	for _, r := range t.Effects {
		b.actions.add(r.Name)
	}
	for _, r := range t.Assigns {
		b.reducers.add(r.Name)
	}
	if t.GuardExpr != nil {
		for _, r := range t.GuardExpr.LeafRefs() {
			b.guards.add(r.Name)
		}
	}
}
