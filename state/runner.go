package state

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
)

// This file ships the reusable host-driver harness for invoked services
// (`invoke`). The kernel emits StartService / StopService effects and stays pure;
// a ServiceRunner is the small, documented runtime that turns those effects into
// real service executions and feeds each service's result back through Fire via
// the invocation's onDone / onError event. A production host wires real service
// implementations that do IO; a test wires synchronous fakes and settles them
// deterministically, so invoke machines are fully testable.
//
// # Host-driver contract
//
// A host that wants invoked services to actually run wraps its instance in a
// ServiceRunner and routes every Fire's effects through it:
//
//	run := state.NewServiceRunner(inst, reg)
//	run.Absorb(ctx, inst.StartEffects())   // arm the initial state's services
//	res := inst.Fire(ctx, ev)
//	dispatch(res.Effects)                  // your own effect dispatch
//	run.Absorb(ctx, res.Effects)           // start/stop services from the same effects
//
// Absorb scans the effects for StartService (run the named service) and
// StopService (stop an in-flight service). When a service completes, the runner
// fires the invocation's OnDone event (carrying the result) back through Fire and
// recursively absorbs the resulting effects, so a chain of invoke states keeps
// running correctly; on failure it fires OnError (carrying the error). A
// production runner runs services on real goroutines; the deterministic harness
// runs synchronous services inline and settles them only when the test drives it.

// ServiceFn is a host-provided invoked-service implementation, bound by name into
// a Registry exactly like a guard or action. It receives the entity it is bound
// to and the StartService effect (Src params, Input) the kernel emitted, and
// returns its result on success or an error on failure. A one-shot
// (promise-style) service returns directly; a streaming service is a host-side
// wrapper that ultimately resolves to a single done/error through this contract.
// A ServiceFn never mutates the instance; it returns data, and the runner routes
// that data through the invocation's onDone / onError event via Fire — so Fire,
// not the service, owns every state change.
type ServiceFn[C any] func(ctx context.Context, in ServiceCtx[C]) (any, error)

// ServiceCtx is passed to a bound service at run time. It carries the entity the
// instance is bound to and the start contract the kernel emitted.
type ServiceCtx[C any] struct {
	Entity C
	Params map[string]any
	Input  map[string]any
}

// running is one in-flight invoked service tracked by a ServiceRunner.
type running[E comparable] struct {
	src     Ref
	input   map[string]any
	onDone  E
	onError E
	state   string
}

// ServiceRunner is the reusable host-driver that turns the kernel's StartService /
// StopService effects into real service executions and re-fires each result
// through its instance via the invocation's onDone / onError event. It is
// concurrency-safe. Construct one per instance with NewServiceRunner, binding the
// service registry that resolves Src refs; drive it by passing each Fire's effects
// (and the instance's StartEffects) to Absorb.
//
// In the deterministic form the runner records each started service as pending and
// settles it only when the test calls SettleDone / SettleError, so invoke machines
// are exercised with no real IO; a production host instead resolves and runs the
// bound ServiceFn on its own goroutine and calls SettleDone / SettleError (or the
// convenience Run) when it finishes.
type ServiceRunner[S comparable, E comparable, C any] struct {
	inst *Instance[S, E, C]
	reg  *Registry[C]

	mu      sync.Mutex
	running map[string]running[E]

	// lastResult holds the most recently settled service outcome so the host
	// action bound to the onDone / onError transition can read the result or error
	// the service produced. Fire carries only the routing event; the outcome rides
	// here, set immediately before the routing Fire and read synchronously by the
	// transition's action. It is atomic so a concurrent reader sees a consistent
	// snapshot.
	lastResult atomic.Value // serviceOutcome
}

// serviceOutcome is the result/error pair a settled service exposes through
// LastResult / LastError for the onDone / onError transition's action to read.
type serviceOutcome struct {
	result any
	err    error
}

// NewServiceRunner returns a ServiceRunner driving inst, resolving Src refs
// against reg's service palette. reg may be nil for a pure deterministic driver
// that never resolves a ServiceFn (the test settles services directly by ID).
func NewServiceRunner[S comparable, E comparable, C any](inst *Instance[S, E, C], reg *Registry[C]) *ServiceRunner[S, E, C] {
	return &ServiceRunner[S, E, C]{
		inst:    inst,
		reg:     reg,
		running: map[string]running[E]{},
	}
}

// Absorb scans effects, recording a running service for each StartService and
// dropping the running service for each StopService (auto-stop-on-exit). It is how
// a host wires Fire's output back into the runner; call it with the effects of
// every Fire (and once with the instance's StartEffects for the initial state). A
// StartService whose OnDone/OnError is not the instance's event type is ignored,
// since the kernel cannot have produced it.
func (r *ServiceRunner[S, E, C]) Absorb(ctx context.Context, effects []Effect) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, eff := range effects {
		switch e := eff.(type) {
		case StartService:
			done, ok := e.OnDone.(E)
			if !ok {
				continue
			}
			erev, ok := e.OnError.(E)
			if !ok {
				continue
			}
			r.running[e.ID] = running[E]{
				src:     e.Src,
				input:   e.Input,
				onDone:  done,
				onError: erev,
				state:   e.State,
			}
		case StopService:
			delete(r.running, e.ID)
		}
	}
}

// Pending reports the number of in-flight (started, not-yet-settled, not-stopped)
// services. A test asserts on it to confirm a service was started or auto-stopped
// on exit.
func (r *ServiceRunner[S, E, C]) Pending() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.running)
}

// HasPending reports whether a service with the given invoke id is in flight.
func (r *ServiceRunner[S, E, C]) HasPending(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.running[id]
	return ok
}

// SettleDone completes the in-flight service id successfully: it drops the service
// and fires its OnDone event (carrying result) through the instance, then absorbs
// the resulting effects so a chained invoke arms its successor. It returns the
// FireResult and true, or the zero result and false when id names no in-flight
// service (already stopped or settled). result is delivered to the onDone
// transition's effects through the instance entity by the host's actions — the
// kernel routes the event; the action reads the result.
func (r *ServiceRunner[S, E, C]) SettleDone(ctx context.Context, id string, result any) (FireResult[S], bool) {
	return r.settle(ctx, id, result, nil)
}

// SettleError fails the in-flight service id: it drops the service and fires its
// OnError event (carrying err) through the instance, then absorbs the resulting
// effects. It returns the FireResult and true, or the zero result and false when
// id names no in-flight service.
func (r *ServiceRunner[S, E, C]) SettleError(ctx context.Context, id string, err error) (FireResult[S], bool) {
	return r.settle(ctx, id, nil, err)
}

// settle is the shared body of SettleDone / SettleError: drop the running service
// under lock, then fire its routing event and absorb the follow-on effects.
func (r *ServiceRunner[S, E, C]) settle(ctx context.Context, id string, result any, err error) (FireResult[S], bool) {
	r.mu.Lock()
	rs, ok := r.running[id]
	if ok {
		delete(r.running, id)
	}
	r.mu.Unlock()
	if !ok {
		return FireResult[S]{}, false
	}
	r.lastResult.Store(serviceOutcome{result: result, err: err})
	var ev E
	if err != nil {
		ev = rs.onError
	} else {
		ev = rs.onDone
	}
	res := r.inst.Fire(ctx, ev)
	r.Absorb(ctx, res.Effects)
	return res, true
}

// Run resolves and runs the in-flight service id against the bound registry,
// settling it with the ServiceFn's result or error. It is the production
// convenience that couples resolve + run + settle: a host that arms services from
// Absorb and wants the runner to execute them calls Run(ctx, id) (typically from
// its own goroutine). It returns the routed FireResult and true, or false when id
// is not in flight or no registry / ServiceFn resolves it (in which case the
// service is settled as an error so the machine still routes onError rather than
// hanging).
func (r *ServiceRunner[S, E, C]) Run(ctx context.Context, id string) (FireResult[S], bool) {
	r.mu.Lock()
	rs, ok := r.running[id]
	r.mu.Unlock()
	if !ok {
		return FireResult[S]{}, false
	}
	fn := r.resolve(rs.src.Name)
	if fn == nil {
		return r.SettleError(ctx, id, &ErrUnboundRef{Kind: "service", Name: rs.src.Name})
	}
	out, err := fn(ctx, ServiceCtx[C]{Entity: r.inst.entity, Params: rs.src.Params, Input: rs.input})
	if err != nil {
		return r.SettleError(ctx, id, err)
	}
	return r.SettleDone(ctx, id, out)
}

// resolve returns the bound ServiceFn for name, or nil when no registry was wired
// or the name is unbound.
func (r *ServiceRunner[S, E, C]) resolve(name string) ServiceFn[C] {
	if r.reg == nil {
		return nil
	}
	return r.reg.services[name]
}

// PendingIDs returns the ids of all in-flight services, sorted, for deterministic
// host iteration (e.g. running every armed service in a stable order).
func (r *ServiceRunner[S, E, C]) PendingIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.running))
	for id := range r.running {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// LastResult returns the result the most recently settled service produced, and
// true when that settlement was a success (SettleDone). The host action bound to
// an onDone transition reads it to consume the service output; it is valid only
// during the synchronous Fire the settlement triggers. It returns false after a
// SettleError or before any settlement.
func (r *ServiceRunner[S, E, C]) LastResult() (any, bool) {
	v, ok := r.lastResult.Load().(serviceOutcome)
	if !ok || v.err != nil {
		return nil, false
	}
	return v.result, true
}

// LastError returns the error the most recently settled service produced, or nil
// when the last settlement was a success or none has occurred. The host action
// bound to an onError transition reads it to consume the failure.
func (r *ServiceRunner[S, E, C]) LastError() error {
	v, ok := r.lastResult.Load().(serviceOutcome)
	if !ok {
		return nil
	}
	return v.err
}
