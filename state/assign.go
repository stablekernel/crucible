package state

import "context"

// This file defines the assign kind: the sole context-mutation site. Under the
// value-semantics context contract (G1), guards and actions receive a copy of the
// context and cannot change the instance; an Assign is a pure reducer that takes
// the prior context by value and returns the next context. The kernel folds the
// assigns declared on a transition's exit, transition, and entry phases — in that
// order, declaration order within each phase, each seeing the prior result — and
// the resulting value becomes the instance's context at the end of the commit.
//
// An Assign emits no effect and returns no error: it is a total reducer (a failing
// reducer is a programmer error, panic-recovered like a guard), so analysis and
// replay see a deterministic next-state computation. The triggering event is in
// scope as AssignCtx.Event so a reducer can fold event data into the context; for
// a service or actor onDone transition the host re-fires the routing event with
// the service/actor result as its payload, so the result reaches the reducer
// through AssignCtx.Event with no host side channel.
//
// The assign is the write half of the binding data boundary: AssignBinding mirrors
// GuardBinding/ActionBinding, and AssignResult.Context carries the new context
// value across the boundary — the honest realization of the channel ActionResult
// formerly reserved.

// AssignCtx is passed to a bound assign reducer at run time. Entity is the prior
// context by value (the reducer's input); the reducer returns the next context.
// Event is the triggering event payload — the runtime event for an ordinary
// transition, or the service/actor result for a service/actor onDone transition.
// Params is the assign ref's static configuration.
type AssignCtx[C any] struct {
	Entity C
	Event  any
	Params map[string]any
}

// AssignFn is the sole context writer: a total pure reducer producing the next
// context from the prior context (by value), the triggering event, and the ref's
// static params. It emits no effect and returns no error; it observes context
// read-only through the copy it receives and yields the new value as its return.
type AssignFn[C any] func(in AssignCtx[C]) C

// AssignRequest is the serializable invocation envelope for an assign: the named
// ref, its params, the triggering event, and the read-only context projection the
// reducer folds.
type AssignRequest[C any] struct {
	Name    string
	Params  map[string]any
	Event   any
	Context ContextView
}

// AssignResult is the assign's serializable result: the new context value. It is
// the write-side mirror of the read-only ContextView and carries the full folded
// context (delta encoding is a later additive optimization on this envelope).
type AssignResult[C any] struct {
	Context C
}

// AssignBinding turns an assign request into the next context value. The
// in-process binding wraps an AssignFn, reading the prior context off the
// in-process context projection; a future out-of-process binding marshals the
// request across its transport. EvalAssign is synchronous so the fold stays
// callable inside the pure commit step.
type AssignBinding[C any] interface {
	EvalAssign(ctx context.Context, req AssignRequest[C]) (AssignResult[C], error)
}

// assignFnBinding is the default in-process AssignBinding: it adapts an AssignFn,
// reading the prior context off the in-process context projection.
type assignFnBinding[C any] struct {
	fn AssignFn[C]
}

// inProcessAssign wraps an AssignFn in the default in-process AssignBinding.
func inProcessAssign[C any](fn AssignFn[C]) AssignBinding[C] {
	return assignFnBinding[C]{fn: fn}
}

// EvalAssign reads the prior context from the request's context view and invokes
// the wrapped AssignFn, returning the new context. The view is read-only; the
// reducer receives the context by value and its return is the only output.
func (b assignFnBinding[C]) EvalAssign(_ context.Context, req AssignRequest[C]) (AssignResult[C], error) {
	entity, _ := req.Context.Raw().(C)
	next := b.fn(AssignCtx[C]{Entity: entity, Event: req.Event, Params: req.Params})
	return AssignResult[C]{Context: next}, nil
}
