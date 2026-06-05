package fooddelivery

import (
	"context"
	"time"

	"github.com/stablekernel/crucible/state"
)

// Rig is the example's host runtime: it casts an order instance and wires the three
// host drivers — a Scheduler (the SLA `after` timer), a ServiceRunner (the payment
// authorize/refund services), and an ActorSystem (the kitchen and courier actors).
// It is the realistic embedding a service would build around the pure decision core:
// Crucible decides, the Rig dispatches the resulting effects to the world.
//
// A Rig is created with NewRig and driven with its Submit/Authorize/... helpers,
// each of which fires an event (or settles a driver) and routes the resulting
// effects back through every driver, exactly as a production host loop would.
type Rig struct {
	machine *state.Machine[Stage, Signal, Order]
	inst    *state.Instance[Stage, Signal, Order]
	run     *state.ServiceRunner[Stage, Signal, Order]
	sch     *state.Scheduler[Stage, Signal, Order]
	sys     *state.ActorSystem[Stage, Signal, Order]
	clk     *state.FakeClock
}

// rigConfig holds the resolved NewRig options.
type rigConfig struct {
	order Order
	start time.Time
}

// RigOption configures a Rig at construction. Options follow the functional-options
// pattern so the rig gains capabilities additively: required inputs are none (a Rig
// has sensible defaults), and every knob arrives as an option.
type RigOption func(*rigConfig)

// WithOrder seeds the initial order context the Rig is cast with.
func WithOrder(o Order) RigOption {
	return func(c *rigConfig) { c.order = o }
}

// WithClockStart sets the fake clock's start instant, so SLA timing is deterministic
// across runs.
func WithClockStart(t time.Time) RigOption {
	return func(c *rigConfig) { c.start = t }
}

// NewRig forges the order machine, casts an instance around the supplied order, and
// wires all three host drivers, arming the initial configuration's effects. The
// returned Rig is ready to Submit. A build error from NewModel (e.g. a guard that
// fails to compile) is returned to the caller.
func NewRig(opts ...RigOption) (*Rig, error) {
	cfg := rigConfig{
		order: Order{Subtotal: 4200, Tip: 800, Priority: "fast"},
		start: time.Unix(0, 0).UTC(),
	}
	for _, o := range opts {
		o(&cfg)
	}

	m, err := NewModel()
	if err != nil {
		return nil, err
	}
	return newRigFrom(m, cfg.order, cfg.start), nil
}

// newRigFrom wires a Rig around an already-forged machine and a clock start. It is
// the shared path for NewRig and for restoring a snapshot into a fresh driver set.
func newRigFrom(m *state.Machine[Stage, Signal, Order], order Order, start time.Time) *Rig {
	clk := state.NewFakeClock(start)
	inst := m.Cast(order, state.WithInitialState(Placed), state.WithClock[Stage](clk))
	r := &Rig{
		machine: m,
		inst:    inst,
		run:     state.NewServiceRunner(inst, ServiceRegistry()),
		sch:     state.NewScheduler(inst),
		sys: state.NewActorSystem(inst).
			Register("kitchen", kitchenBehavior()).
			Register("courier", courierBehavior()),
		clk: clk,
	}
	r.absorb(context.Background(), inst.StartEffects())
	return r
}

// Order returns the instance's current context value (read-only).
func (r *Rig) Order() Order { return r.inst.Entity() }

// Configuration returns the instance's active state configuration.
func (r *Rig) Configuration() []Stage { return r.inst.Configuration() }

// InFinal reports whether the instance has reached a terminal state.
func (r *Rig) InFinal() bool { return r.inst.InFinal() }

// fire drives one event through the instance and routes the resulting effects
// through every driver, so timers arm, services start, and actors spawn as a real
// host would.
func (r *Rig) fire(ctx context.Context, ev Signal) state.FireResult[Stage] {
	res := r.inst.Fire(ctx, ev)
	r.absorb(ctx, res.Effects)
	return res
}

// absorb feeds an effect slice to all three drivers; each ignores effects not its
// own, so one call wires schedule/service/actor effects uniformly.
func (r *Rig) absorb(ctx context.Context, effects []state.Effect) {
	r.run.Absorb(ctx, effects)
	r.sch.Absorb(ctx, effects)
	r.sys.AbsorbFor(ctx, nil, effects)
}

// Submit places the order, arming the payment authorization service.
func (r *Rig) Submit(ctx context.Context) state.FireResult[Stage] {
	return r.fire(ctx, Submit)
}

// Cancel opens the cancellation saga from any active substate, arming the refund.
func (r *Rig) Cancel(ctx context.Context) state.FireResult[Stage] {
	return r.fire(ctx, Cancel)
}

// PickUp advances the Fulfillment region from AwaitingCourier to EnRoute, dispatching
// the courier actor.
func (r *Rig) PickUp(ctx context.Context) state.FireResult[Stage] {
	return r.fire(ctx, PickedUp)
}

// authorizeID is the stable id of the authorization service invoked while the order
// is Authorizing.
func (r *Rig) authorizeID() string { return state.InvokeID("order", Authorizing, 0) }

// refundID is the stable id of the refund service invoked while the order is
// Refunding.
func (r *Rig) refundID() string { return state.InvokeID("order", Refunding, 0) }

// SettleAuthorization settles the in-flight authorization service, routing Authorized
// with the hold token on success or Declined on failure, and absorbs the follow-on
// effects.
func (r *Rig) SettleAuthorization(ctx context.Context, ok bool) state.FireResult[Stage] {
	if ok {
		fr, _ := r.run.SettleDone(ctx, r.authorizeID(), "tok-001")
		r.absorb(ctx, fr.Effects)
		return fr
	}
	fr, _ := r.run.SettleError(ctx, r.authorizeID(), errOf("authorization declined"))
	r.absorb(ctx, fr.Effects)
	return fr
}

// SettleRefund settles the in-flight refund service, routing Refunded with the
// reversed amount, and absorbs the follow-on effects.
func (r *Rig) SettleRefund(ctx context.Context, amount int64) state.FireResult[Stage] {
	fr, _ := r.run.SettleDone(ctx, r.refundID(), amount)
	r.absorb(ctx, fr.Effects)
	return fr
}

// RunAuthorization runs the real authorize service synchronously through the runner
// (executing authorizeFn) and routes its outcome, rather than settling it with a
// fixed token. It returns the resulting FireResult and whether a service ran.
func (r *Rig) RunAuthorization(ctx context.Context) (state.FireResult[Stage], bool) {
	results := r.run.Tick(ctx, r.authorizeID())
	if len(results) == 0 {
		return state.FireResult[Stage]{}, false
	}
	fr := results[0]
	r.absorb(ctx, fr.Effects)
	return fr, true
}

// RunRefund runs the real refund service synchronously through the runner (executing
// refundFn) and routes its outcome. It returns the resulting FireResult and whether a
// service ran.
func (r *Rig) RunRefund(ctx context.Context) (state.FireResult[Stage], bool) {
	results := r.run.Tick(ctx, r.refundID())
	if len(results) == 0 {
		return state.FireResult[Stage]{}, false
	}
	fr := results[0]
	r.absorb(ctx, fr.Effects)
	return fr, true
}

// RunKitchen steps the kitchen actor to its final state, so its completion re-fires
// PlatedUp (carrying the prepared item) back through the order. It returns whether a
// kitchen actor was running to step.
func (r *Rig) RunKitchen(ctx context.Context) bool {
	return r.stepActor(ctx, kitchenStep)
}

// RunCourier steps the courier actor to its final state, so its completion re-fires
// DroppedOff (carrying the drop proof) back through the order. It returns whether a
// courier actor was running to step.
func (r *Rig) RunCourier(ctx context.Context) bool {
	return r.stepActor(ctx, courierStep)
}

// actorStep names the message that drives a child actor to completion.
type actorStep int

const (
	kitchenStep actorStep = iota
	courierStep
)

// stepActor delivers the completing message to every running actor whose source
// matches the requested step, driving it to its final state.
func (r *Rig) stepActor(ctx context.Context, step actorStep) bool {
	stepped := false
	for _, id := range r.sys.IDs() {
		ref, ok := r.sys.Ref(id)
		if !ok {
			continue
		}
		switch step {
		case kitchenStep:
			if ref.Src == "kitchen" {
				r.sys.Deliver(ctx, ref, kitchenCook)
				stepped = true
			}
		case courierStep:
			if ref.Src == "courier" {
				r.sys.Deliver(ctx, ref, courierDrive)
				stepped = true
			}
		}
	}
	return stepped
}

// BreachSLA advances the fake clock past the SLA window and ticks the Scheduler,
// firing the delayed SLABreached edge in the Watchdog region and absorbing its
// effects. It returns the timer-driven FireResults.
func (r *Rig) BreachSLA(ctx context.Context) []state.FireResult[Stage] {
	r.clk.Advance(SLAWindow)
	out := r.sch.Tick(ctx)
	for _, fr := range out {
		r.absorb(ctx, fr.Effects)
	}
	return out
}

// PendingTimers reports how many `after` timers the Scheduler currently holds.
func (r *Rig) PendingTimers() int { return r.sch.Pending() }

// RunningActors reports how many child actors are currently running.
func (r *Rig) RunningActors() int { return r.sys.Running() }

// Snapshot captures the instance's full runtime state (value context, active
// configuration, history, pending timers, and in-flight drivers) for persistence.
func (r *Rig) Snapshot() state.Snapshot[Stage, Signal, Order] { return r.inst.Snapshot() }

// RestoreRig rebuilds a Rig from a persisted snapshot, as a recovering host would
// after a process restart: it forges a fresh machine, restores the instance into it,
// re-wires a fresh driver set, and re-arms the resumed configuration's effects
// (pending SLA timers and in-flight actors). The restored Rig resumes exactly where
// the snapshot was taken — same configuration, same pending work.
func RestoreRig(snap state.Snapshot[Stage, Signal, Order], opts ...RigOption) (*Rig, error) {
	cfg := rigConfig{start: time.Unix(0, 0).UTC()}
	for _, o := range opts {
		o(&cfg)
	}

	m, err := NewModel()
	if err != nil {
		return nil, err
	}
	clk := state.NewFakeClock(cfg.start)
	inst, err := m.Restore(snap, state.WithRestoreClock[Stage](clk))
	if err != nil {
		return nil, err
	}
	r := &Rig{
		machine: m,
		inst:    inst,
		run:     state.NewServiceRunner(inst, ServiceRegistry()),
		sch:     state.NewScheduler(inst),
		sys: state.NewActorSystem(inst).
			Register("kitchen", kitchenBehavior()).
			Register("courier", courierBehavior()),
		clk: clk,
	}
	r.absorb(context.Background(), inst.ResumeEffects())
	return r, nil
}

// errOf is a tiny helper so the rig need not import errors at every call site.
func errOf(msg string) error { return rigError(msg) }

// rigError is a minimal error type for the rig's settle helpers.
type rigError string

// Error returns the message.
func (e rigError) Error() string { return string(e) }
