package state

import "context"

// This file makes behavior invocation a binding rather than a bare Go func
// pointer. A binding turns a serializable request envelope into a serializable
// result; the Go in-process binding is the trivial implementation that calls the
// registered func. The registry stores a per-kind binding in parallel with the
// existing bare-func maps, which remain the in-process fast path that the pure
// Fire step, Quench, and the palette already use unchanged.
//
// This is the load-bearing v1 lock: the registry binds a name to an interface, so
// a future out-of-process binding (a sandboxed component, a remote service) is an
// additive registration under the same name with no change to Fire, the IR, or the
// public registration sugar. No out-of-process transport is built here — only the
// interface and the default in-process binding that wraps a func.
//
// The guard binding is synchronous (it returns (bool, error)) so it stays callable
// inside the pure Fire step. Out-of-process and host-pre-resolved guards are
// reserved, not built. Actions return effects-as-data and never write context; a
// context change is expressed only through an AssignBinding, whose
// AssignResult.Context carries the new value — the sole context-writer seam.

// GuardRequest is the serializable invocation envelope for a guard: the named ref,
// its params, and the read-only context projection the guard evaluates against.
type GuardRequest[C any] struct {
	Name    string
	Params  map[string]any
	Context ContextView
}

// GuardResult is the guard's serializable result: a boolean verdict. It is
// deliberately minimal so a guard stays a pure predicate evaluable inside Fire.
type GuardResult struct {
	OK bool
}

// ActionRequest is the serializable invocation envelope for an action: the named
// ref, its params, and the read-only context projection.
type ActionRequest[C any] struct {
	Name    string
	Params  map[string]any
	Context ContextView
}

// ActionResult is the action's serializable result. Effects carries the emitted
// effects-as-data (today an action emits exactly one). Actions never write
// context: under the value-semantics contract a context change is expressed only
// through an Assign, whose AssignResult.Context carries the new value. The channel
// an action formerly reserved for a context delta now lives on the assign binding,
// the sole context writer.
type ActionResult struct {
	Effects []Effect
}

// ServiceRequest is the serializable invocation envelope for an invoked service.
// The service result is routed back through the kernel's existing onDone/onError
// event machinery (the StartService effect), so it needs no result envelope here.
type ServiceRequest[C any] struct {
	Name   string
	Params map[string]any
	Input  map[string]any
}

// GuardBinding turns a guard request into a verdict. The in-process binding wraps
// a GuardFn; a future out-of-process binding marshals the request across its
// transport. EvalGuard is synchronous so it remains callable inside the pure Fire
// step.
type GuardBinding[C any] interface {
	EvalGuard(ctx context.Context, req GuardRequest[C]) (GuardResult, error)
}

// ActionBinding turns an action request into emitted effects. The in-process
// binding wraps an ActionFn.
type ActionBinding[C any] interface {
	EvalAction(ctx context.Context, req ActionRequest[C]) (ActionResult, error)
}

// ServiceBinding runs an invoked service. The in-process binding wraps a
// ServiceFn; the result is shuttled by the runner through the invocation's
// onDone/onError event.
type ServiceBinding[C any] interface {
	RunService(ctx context.Context, req ServiceRequest[C]) (any, error)
}

// guardFnBinding is the default in-process GuardBinding: it adapts a GuardFn,
// reading the live entity off the in-process context projection.
type guardFnBinding[C any] struct {
	fn GuardFn[C]
}

// inProcessGuard wraps a GuardFn in the default in-process GuardBinding.
func inProcessGuard[C any](fn GuardFn[C]) GuardBinding[C] {
	return guardFnBinding[C]{fn: fn}
}

// EvalGuard reads the live entity from the request's context view and invokes the
// wrapped GuardFn. The view is read-only; the func receives the entity by value.
func (b guardFnBinding[C]) EvalGuard(_ context.Context, req GuardRequest[C]) (GuardResult, error) {
	entity, _ := req.Context.Raw().(C)
	return GuardResult{OK: b.fn(GuardCtx[C]{Entity: entity, Params: req.Params})}, nil
}

// actionFnBinding is the default in-process ActionBinding: it adapts an ActionFn.
type actionFnBinding[C any] struct {
	fn ActionFn[C]
}

// inProcessAction wraps an ActionFn in the default in-process ActionBinding.
func inProcessAction[C any](fn ActionFn[C]) ActionBinding[C] {
	return actionFnBinding[C]{fn: fn}
}

// EvalAction invokes the wrapped ActionFn and lifts its single effect into the
// ActionResult envelope. Actions emit effects only; context is written through an
// assign, not here.
func (b actionFnBinding[C]) EvalAction(_ context.Context, req ActionRequest[C]) (ActionResult, error) {
	entity, _ := req.Context.Raw().(C)
	eff, err := b.fn(ActionCtx[C]{Entity: entity, Params: req.Params})
	if err != nil {
		return ActionResult{}, err
	}
	return ActionResult{Effects: []Effect{eff}}, nil
}

// serviceFnBinding is the default in-process ServiceBinding: it adapts a ServiceFn.
type serviceFnBinding[C any] struct {
	fn ServiceFn[C]
}

// inProcessService wraps a ServiceFn in the default in-process ServiceBinding.
func inProcessService[C any](fn ServiceFn[C]) ServiceBinding[C] {
	return serviceFnBinding[C]{fn: fn}
}

// RunService invokes the wrapped ServiceFn. The entity is not projected through
// the context view here: a service runs off the pure Fire step on the host's
// runner, which already holds the live entity.
func (b serviceFnBinding[C]) RunService(ctx context.Context, req ServiceRequest[C]) (any, error) {
	return b.fn(ctx, ServiceCtx[C]{Params: req.Params, Input: req.Input})
}

// boundBehavior holds the per-kind binding interface value a registration records
// in parallel with the bare-func maps. Exactly one field is set per entry, matching
// the registration's kind; entries are namespaced by kind+name (bindingKey) so a
// guard and an action may share a name without colliding.
type boundBehavior[C any] struct {
	guard   GuardBinding[C]
	action  ActionBinding[C]
	service ServiceBinding[C]
	assign  AssignBinding[C]
}

// bindingKey namespaces a binding by kind and name, mirroring descriptorKey, so
// guards, actions, and services never collide on a shared name.
func bindingKey(kind DescriptorKind, name string) string {
	return string(kind) + "\x00" + name
}

// bindGuard records the in-process GuardBinding for a guard registration.
func (r *Registry[C]) bindGuard(name string, b GuardBinding[C]) {
	r.bindings[bindingKey(KindGuard, name)] = boundBehavior[C]{guard: b}
}

// BindGuard registers a guard under name from a GuardBinding directly, instead of
// from a plain GuardFn. It is the additive seam an opt-in expression module uses
// to register a guard whose verdict comes from a compiled expression program
// rather than a hand-written Go predicate: the module compiles its source once and
// hands the resulting evaluator in as the binding.
//
// The binding is wired into the same name path Guard uses, so a guard registered
// this way is indistinguishable to the kernel from a Go-func guard — it resolves
// by name at Provide/Quench, evaluates synchronously inside the pure Fire step, and
// surfaces a panic as the same typed GuardPanicError. The binding's EvalGuard is
// adapted to a GuardFn over the in-process context view so the fire-time fast path
// (which reads r.guards) finds it; the binding is also recorded on the parallel
// binding seam so a future out-of-process transport can swap it under the same name.
//
// EvalGuard is called with a background context and the in-process context view;
// an error it returns is treated as a false verdict, matching how a Go guard that
// cannot decide yields false rather than transitioning. An optional Describe option
// adds palette metadata exactly as Guard does.
func (r *Registry[C]) BindGuard(name string, b GuardBinding[C], opts ...DescribeOption) *Registry[C] {
	r.guards[name] = func(gctx GuardCtx[C]) bool {
		res, err := b.EvalGuard(context.Background(), GuardRequest[C]{
			Name:    name,
			Params:  gctx.Params,
			Context: newInProcessView(gctx.Entity),
		})
		if err != nil {
			return false
		}
		return res.OK
	}
	r.bindGuard(name, b)
	r.describe(KindGuard, name, opts)
	return r
}

// bindAction records the in-process ActionBinding for an action registration.
func (r *Registry[C]) bindAction(name string, b ActionBinding[C]) {
	r.bindings[bindingKey(KindAction, name)] = boundBehavior[C]{action: b}
}

// bindService records the in-process ServiceBinding for a service registration.
func (r *Registry[C]) bindService(name string, b ServiceBinding[C]) {
	r.bindings[bindingKey(KindService, name)] = boundBehavior[C]{service: b}
}

// bindAssign records the in-process AssignBinding for an assign registration.
func (r *Registry[C]) bindAssign(name string, b AssignBinding[C]) {
	r.bindings[bindingKey(KindAssign, name)] = boundBehavior[C]{assign: b}
}

// guardBinding returns the recorded in-process GuardBinding for name, or nil when
// no guard is registered under it.
func (r *Registry[C]) guardBinding(name string) GuardBinding[C] {
	return r.bindings[bindingKey(KindGuard, name)].guard
}

// actionBinding returns the recorded in-process ActionBinding for name, or nil.
func (r *Registry[C]) actionBinding(name string) ActionBinding[C] {
	return r.bindings[bindingKey(KindAction, name)].action
}

// serviceBinding returns the recorded in-process ServiceBinding for name, or nil.
func (r *Registry[C]) serviceBinding(name string) ServiceBinding[C] {
	return r.bindings[bindingKey(KindService, name)].service
}

// assignBinding returns the recorded in-process AssignBinding for name, or nil.
func (r *Registry[C]) assignBinding(name string) AssignBinding[C] {
	return r.bindings[bindingKey(KindAssign, name)].assign
}

// adoptBindings copies another registry's bindings into this one wholesale,
// mirroring the bare-func adoption ir.go performs so a Provide'd or Quench'd
// machine carries the same binding seam as a freshly registered one.
func (r *Registry[C]) adoptBindings(src *Registry[C]) {
	for name, b := range src.bindings {
		r.bindings[name] = b
	}
}
