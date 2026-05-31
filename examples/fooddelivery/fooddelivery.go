// Package fooddelivery is the flagship Crucible example: a generic, textbook
// food-ordering lifecycle modeled as a single statechart that exercises the whole
// engine the way a real service would. It is invented from scratch and carries no
// coupling to any real delivery product — order, kitchen, courier, and payment are
// the neutral nouns of the domain.
//
// The model demonstrates the patterns Crucible recommends for v1:
//
//   - A value-semantics context (Order, passed and returned by value) whose only
//     writer is an Assign reducer. Guards and the kernel observe the context
//     read-only; a reducer takes the prior Order and returns the next.
//   - Decision logic authored as data: Core guard expressions over a ContextSchema
//     (compare/membership/boolean leaves the kernel evaluates directly), plus one
//     Rich (CEL) guard from state/expr to show the second tier.
//   - Hierarchy and parallel regions: an Active orthogonal superstate runs a
//     Fulfillment region (kitchen → courier spine) in parallel with a Watchdog
//     region (an SLA clock driven by an `after` timeout).
//   - Actors for the restaurant (kitchen) and the driver (courier), each a child
//     machine that messages the order on completion.
//   - An invoked payment service (authorize, then capture) with onDone/onError, and
//     a refund invocation that drives the cancellation saga's compensation.
//   - Deterministic time: SLA timeouts arm through the Scheduler and advance with a
//     fake clock in tests.
//   - Snapshot and restore across a simulated process restart mid-order.
//
// Build the machine with NewModel; drive it through the host runtime with NewRig
// (the example's wiring of Scheduler + ServiceRunner + ActorSystem), or embed the
// pieces directly. See README.md for the requirement-to-feature tutorial.
package fooddelivery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// Stage is the order's lifecycle state. The leaves of the Active superstate's two
// regions (the Fulfillment spine and the Watchdog clock) are separate Stage values
// so a parallel configuration names both at once.
type Stage int

// The order lifecycle stages. Placed is the resting start; Authorizing invokes the
// payment hold; Active is the orthogonal superstate where the kitchen cooks and the
// courier delivers while an SLA clock runs alongside; Settling captures the held
// payment; Delivered is the happy-path terminal. The cancellation saga runs
// Refunding (a compensating payment reversal) to the Canceled terminal, and a
// declined authorization lands in Rejected.
const (
	Placed Stage = iota
	Authorizing
	Active
	// Fulfillment region leaves.
	Cooking
	AwaitingCourier
	EnRoute
	// Watchdog region leaves.
	OnTime
	Overdue
	// Post-Active spine.
	Settling
	Delivered
	Refunding
	Canceled
	Rejected
)

// String renders a Stage by name so traces, diagrams, and analysis read
// symbolically.
func (s Stage) String() string {
	switch s {
	case Placed:
		return "Placed"
	case Authorizing:
		return "Authorizing"
	case Active:
		return "Active"
	case Cooking:
		return "Cooking"
	case AwaitingCourier:
		return "AwaitingCourier"
	case EnRoute:
		return "EnRoute"
	case OnTime:
		return "OnTime"
	case Overdue:
		return "Overdue"
	case Settling:
		return "Settling"
	case Delivered:
		return "Delivered"
	case Refunding:
		return "Refunding"
	case Canceled:
		return "Canceled"
	case Rejected:
		return "Rejected"
	default:
		return "Stage?"
	}
}

// Signal is the order machine's event type. Authorized/Declined route the payment
// authorization outcome; Refunded routes the refund outcome; Cancel opens the saga;
// the kitchen/courier signals advance the Fulfillment region; SLABreached is the
// Watchdog region's delayed timeout; PlatedUp/PickedUp/DroppedOff move the spine.
type Signal int

// The order lifecycle signals.
const (
	Submit Signal = iota
	Authorized
	Declined
	PlatedUp
	PickedUp
	DroppedOff
	SLABreached
	Cancel
	Captured
	Refunded
)

// String renders a Signal by name so traces and diagrams read symbolically.
func (e Signal) String() string {
	switch e {
	case Submit:
		return "Submit"
	case Authorized:
		return "Authorized"
	case Declined:
		return "Declined"
	case PlatedUp:
		return "PlatedUp"
	case PickedUp:
		return "PickedUp"
	case DroppedOff:
		return "DroppedOff"
	case SLABreached:
		return "SLABreached"
	case Cancel:
		return "Cancel"
	case Captured:
		return "Captured"
	case Refunded:
		return "Refunded"
	default:
		return "Signal?"
	}
}

// Order is the value-semantics context the machine is bound to. It is passed and
// returned by value; every change to it flows through an Assign reducer, so guards
// and the kernel only ever read it. The json tags name the fields for the
// ContextSchema and the Core/Rich guard expressions' field references.
type Order struct {
	// Subtotal is the order's pre-tip total, in whole currency minor units (cents).
	// The "expedite" Core guard compares it against a threshold.
	Subtotal int64 `json:"subtotal"`
	// Tip is the gratuity in cents. The Rich CEL guard reasons over subtotal+tip.
	Tip int64 `json:"tip"`
	// Priority marks an order the dispatcher flagged for the fast lane. The Core
	// membership guard reads it.
	Priority string `json:"priority"`
	// AuthHold is the payment-authorization token captured on Authorized. The
	// settlement and refund reducers fold the held amount through it.
	AuthHold string `json:"authHold"`
	// Refund is the amount reversed by the cancellation saga's compensating refund,
	// in cents.
	Refund int64 `json:"refund"`
	// Breached records that the Watchdog region's SLA timer fired before delivery.
	Breached bool `json:"breached"`
	// Log records the milestones the run folded, so a test can assert observable
	// behavior without a side channel.
	Log []string `json:"log"`
}

// SLAWindow is the delivery service-level window after which the Watchdog region's
// `after` timeout fires SLABreached. Tests advance a fake clock past it to drive the
// breach deterministically.
const SLAWindow = 30 * time.Minute

// expediteThreshold is the subtotal (in cents) at or above which the Core
// big-basket guard branch treats a fast-lane order as eligible for admission.
const expediteThreshold = 5000

// SchemaOf-derived context schema names the Order fields the Core and Rich guards
// type-check against. It is built once from the Order struct's reflection.
func orderSchema() state.ContextSchema { return state.SchemaOf[Order]() }

// kitchenMachine builds the restaurant child machine: a two-stage run-to-final
// worker that accepts a ticket and reaches Plated, exposing the prepared item as
// its actor output. It models the kitchen as an independent actor the order
// supervises rather than calls inline.
func kitchenMachine() *state.Machine[kitchenStage, kitchenSignal, kitchenTicket] {
	return state.Forge[kitchenStage, kitchenSignal, kitchenTicket]("kitchen").
		Reducer("plate", func(in state.AssignCtx[kitchenTicket]) kitchenTicket {
			c := in.Entity
			c.Item = "prepared-meal"
			return c
		}).
		State(kitchenPrepping).
		State(kitchenPlated).Final().OnEntryAssign("plate").
		Initial(kitchenPrepping).
		Transition(kitchenPrepping).On(kitchenCook).GoTo(kitchenPlated).
		Quench()
}

// courierMachine builds the driver child machine: a two-stage run-to-final worker
// that accepts a route and reaches Delivered, exposing the drop confirmation as its
// actor output.
func courierMachine() *state.Machine[courierStage, courierSignal, courierRoute] {
	return state.Forge[courierStage, courierSignal, courierRoute]("courier").
		Reducer("confirm", func(in state.AssignCtx[courierRoute]) courierRoute {
			c := in.Entity
			c.Proof = "drop-confirmed"
			return c
		}).
		State(courierRiding).
		State(courierDelivered).Final().OnEntryAssign("confirm").
		Initial(courierRiding).
		Transition(courierRiding).On(courierDrive).GoTo(courierDelivered).
		Quench()
}

// kitchenStage / kitchenSignal / kitchenTicket are the kitchen child machine's own
// types: a tiny prepping → plated run-to-final machine whose ticket carries the
// prepared item out as the actor's output.
type kitchenStage int

const (
	kitchenPrepping kitchenStage = iota
	kitchenPlated
)

func (s kitchenStage) String() string {
	if s == kitchenPlated {
		return "kitchenPlated"
	}
	return "kitchenPrepping"
}

type kitchenSignal int

const kitchenCook kitchenSignal = iota

func (kitchenSignal) String() string { return "kitchenCook" }

type kitchenTicket struct {
	Item string `json:"item"`
}

// courierStage / courierSignal / courierRoute are the courier child machine's own
// types: a riding → delivered run-to-final machine whose route carries the drop
// confirmation out as the actor's output.
type courierStage int

const (
	courierRiding courierStage = iota
	courierDelivered
)

func (s courierStage) String() string {
	if s == courierDelivered {
		return "courierDelivered"
	}
	return "courierRiding"
}

type courierSignal int

const courierDrive courierSignal = iota

func (courierSignal) String() string { return "courierDrive" }

type courierRoute struct {
	Proof string `json:"proof"`
}

// kitchenBehavior returns the ActorBehavior the order's ActorSystem registers under
// "kitchen": it casts a fresh kitchen actor per spawn and exposes the prepared item
// as the actor output, which reaches the order's onDone reducer via the event.
func kitchenBehavior() state.ActorBehavior {
	km := kitchenMachine()
	return func(map[string]any) (state.ActorInstance, error) {
		inst := km.Cast(kitchenTicket{}, state.WithInitialState(kitchenPrepping))
		return state.NewActor(inst, func(i *state.Instance[kitchenStage, kitchenSignal, kitchenTicket]) any {
			return i.Entity().Item
		}), nil
	}
}

// courierBehavior returns the ActorBehavior the order's ActorSystem registers under
// "courier": it casts a fresh courier actor per spawn and exposes the drop proof as
// the actor output.
func courierBehavior() state.ActorBehavior {
	cm := courierMachine()
	return func(map[string]any) (state.ActorInstance, error) {
		inst := cm.Cast(courierRoute{}, state.WithInitialState(courierRiding))
		return state.NewActor(inst, func(i *state.Instance[courierStage, courierSignal, courierRoute]) any {
			return i.Entity().Proof
		}), nil
	}
}

// payment authorization/refund are host-run services. authorizeFn holds the order's
// subtotal; in a real host it would call a payment provider. The example settles the
// in-flight authorization deterministically in tests to drive both the success and
// decline edges.
func authorizeFn(_ context.Context, in state.ServiceCtx[Order]) (any, error) {
	if in.Entity.Subtotal <= 0 {
		return nil, errors.New("authorization declined: empty basket")
	}
	return fmt.Sprintf("hold-%d", in.Entity.Subtotal), nil
}

// refundFn reverses a held authorization. It is the compensating action the
// cancellation saga invokes; its result (the refunded amount) reaches the onDone
// reducer through the event.
func refundFn(_ context.Context, in state.ServiceCtx[Order]) (any, error) {
	if in.Entity.AuthHold == "" {
		return nil, errors.New("nothing to refund: no authorization hold")
	}
	return in.Entity.Subtotal + in.Entity.Tip, nil
}

// The reducers below are the sole context writers. Each is a pure, total function
// from the prior Order value (and the triggering event) to the next Order value.
// They are package-level so the builder and the provide-time registry reference the
// same function identity.

// recordHold folds the authorization token from the Authorized event into the
// context, reading the held token from AssignCtx.Event (no host side channel).
func recordHold(in state.AssignCtx[Order]) Order {
	c := in.Entity
	if hold, ok := in.Event.(string); ok {
		c.AuthHold = hold
		c.Log = append(c.Log, "authorized:"+hold)
	}
	return c
}

// recordDecline logs a declined authorization.
func recordDecline(in state.AssignCtx[Order]) Order {
	c := in.Entity
	c.Log = append(c.Log, "declined")
	return c
}

// recordPrep folds the kitchen actor's plated-item output, delivered through the
// re-fired done event's payload.
func recordPrep(in state.AssignCtx[Order]) Order {
	c := in.Entity
	if item, ok := in.Event.(string); ok {
		c.Log = append(c.Log, "kitchen:"+item)
	}
	return c
}

// recordDrop folds the courier actor's drop confirmation, delivered through the
// re-fired done event's payload.
func recordDrop(in state.AssignCtx[Order]) Order {
	c := in.Entity
	if proof, ok := in.Event.(string); ok {
		c.Log = append(c.Log, "courier:"+proof)
	}
	return c
}

// markBreached records that the Watchdog region's SLA timer fired before delivery.
func markBreached(in state.AssignCtx[Order]) Order {
	c := in.Entity
	c.Breached = true
	c.Log = append(c.Log, "sla-breached")
	return c
}

// settleReducer records the payment capture as the order runs to its terminal.
func settleReducer(in state.AssignCtx[Order]) Order {
	c := in.Entity
	c.Log = append(c.Log, "captured")
	return c
}

// recordRefund folds the refund service's reversed amount, delivered through the
// onDone event's payload — the saga's compensation made observable.
func recordRefund(in state.AssignCtx[Order]) Order {
	c := in.Entity
	if amount, ok := in.Event.(int64); ok {
		c.Refund = amount
		c.Log = append(c.Log, fmt.Sprintf("refunded:%d", amount))
	}
	return c
}

// NewModel forges the order lifecycle machine and returns it alongside the registry
// the Rich (CEL) guard is compiled into. The machine is authored with the fluent
// builder, then round-tripped through its IR (ToJSON → LoadFromJSON → Provide) so
// the CEL-backed guard binds to the live machine the same way a host that loads a
// serialized definition would wire it — the in-repo demonstration of the rich tier.
func NewModel() (*state.Machine[Stage, Signal, Order], error) {
	schema := orderSchema()
	reg := state.NewRegistry[Order]()

	// Rich (CEL) guard: a generous-order check authored as a typed expression over
	// the context schema. It compiles once, here, and the kernel evaluates the
	// compiled program inside Fire. It composes with the Core/named leaves below.
	generous, err := expr.Guard[Stage](reg, "generousOrder", "subtotal + tip >= 6000", schema)
	if err != nil {
		return nil, fmt.Errorf("compile rich guard: %w", err)
	}

	// Register every behavior the live machine binds into the same registry as the
	// CEL guard: Provide adopts this registry wholesale, so it must carry the full
	// set of services, actors, and reducers — not just the rich guard.
	registerBindings(reg)

	def := buildModel(schema, generous)

	js, err := def.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("encode model IR: %w", err)
	}
	ir, err := state.LoadFromJSON[Stage, Signal, Order](js)
	if err != nil {
		return nil, fmt.Errorf("reload model IR: %w", err)
	}
	return ir.Provide(reg).Quench(), nil
}

// ServiceRegistry returns a registry holding the order's payment services
// (authorize and refund), so a host's ServiceRunner can actually run them rather
// than settling them deterministically. The example's Rig wires its runner with this
// registry; a host embedding the model would do the same.
func ServiceRegistry() *state.Registry[Order] {
	reg := state.NewRegistry[Order]()
	reg.Service("authorize", authorizeFn)
	reg.Service("refund", refundFn)
	return reg
}

// registerBindings populates reg with every service, actor, and reducer the order
// machine references, so the registry NewModel provides to the reloaded IR resolves
// all refs. The rich guard is registered separately, by expr.Guard, before this.
func registerBindings(reg *state.Registry[Order]) {
	reg.Service("authorize", authorizeFn)
	reg.Service("refund", refundFn)
	reg.Actor("kitchen")
	reg.Actor("courier")
	reg.Assign("recordHold", recordHold)
	reg.Assign("recordDecline", recordDecline)
	reg.Assign("recordPrep", recordPrep)
	reg.Assign("recordDrop", recordDrop)
	reg.Assign("markBreached", markBreached)
	reg.Assign("settle", settleReducer)
	reg.Assign("recordRefund", recordRefund)
}

// buildModel assembles the order lifecycle statechart. generous is the Rich (CEL)
// guard node compiled by NewModel; the named placeholder guard registered for it
// here is replaced by the real CEL binding when NewModel reprovides the registry.
func buildModel(schema state.ContextSchema, generous state.GuardNode[Stage]) *state.Machine[Stage, Signal, Order] {
	return state.Forge[Stage, Signal, Order]("order").
		WithContextSchema(schema).
		// Payment services: authorize holds funds; refund reverses the hold in the
		// cancellation saga. Both route their outcome through onDone/onError signals.
		Service("authorize", authorizeFn).
		Service("refund", refundFn).
		// Actors: the kitchen prepares the meal and the courier delivers it. Each is
		// a child machine the order supervises; its output reaches the order's onDone
		// reducer via the re-fired done event.
		Actor("kitchen").
		Actor("courier").
		// Placeholder for the Rich guard NewModel compiles; the CEL binding replaces
		// it on reprovide. Authoring it here keeps buildModel self-validating.
		Guard("generousOrder", func(state.GuardCtx[Order]) bool { return false }).
		// Reducers — the sole context writers — referenced by package-level identity
		// so the builder and the provide-time registry share one function.
		Reducer("recordHold", recordHold).
		Reducer("recordDecline", recordDecline).
		Reducer("recordPrep", recordPrep).
		Reducer("recordDrop", recordDrop).
		Reducer("markBreached", markBreached).
		Reducer("settle", settleReducer).
		Reducer("recordRefund", recordRefund).
		// Placed: the resting start. Submit begins authorization.
		State(Placed).
		Transition(Placed).On(Submit).GoTo(Authorizing).
		// Authorizing invokes the payment hold. On success it routes Authorized; on a
		// decline it routes Declined to the terminal Rejected. The Authorized edge is
		// admitted by a guard expression mixing a Core compare, a Core membership
		// test, and the Rich (CEL) guard: a generous order, OR a big basket flagged
		// for the fast lane.
		State(Authorizing).
		Invoke("authorize", Authorized, Declined).
		Transition(Authorizing).On(Authorized).
		WhenExpr(state.Or(
			generous,
			state.And(
				state.Field[Stage]("subtotal").Ge(state.Int[Stage](expediteThreshold)),
				state.Field[Stage]("priority").In(state.Str[Stage]("fast"), state.Str[Stage]("express")),
			),
		)).
		GoTo(Active).Assign("recordHold").
		Transition(Authorizing).On(Declined).GoTo(Rejected).Assign("recordDecline").
		// Active is the orthogonal superstate: a Fulfillment spine (kitchen → courier)
		// runs in parallel with a Watchdog SLA clock. The Watchdog is observational —
		// it records a breach if the delivery window elapses — while the Fulfillment
		// spine drives progress. The courier's drop is a cross-cutting transition on
		// the compound that exits the whole parallel configuration to Settling.
		SuperState(Active).
		Initial(Active).
		Region("Fulfillment").
		Initial(Cooking).
		// Cooking supervises the kitchen actor; its plated output advances the spine.
		SubState(Cooking).
		InvokeActor("kitchen", PlatedUp, Declined).
		On(PlatedUp).GoTo(AwaitingCourier).Assign("recordPrep").
		// The courier is dispatched once the meal is plated; PickedUp moves to EnRoute.
		SubState(AwaitingCourier).
		On(PickedUp).GoTo(EnRoute).
		// EnRoute supervises the courier actor; its drop confirmation (DroppedOff) is
		// handled cross-cuttingly by the Active compound, exiting the parallel state.
		SubState(EnRoute).
		InvokeActor("courier", DroppedOff, Declined).
		EndRegion().
		Region("Watchdog").
		Initial(OnTime).
		// OnTime runs an SLA clock: after the window elapses, SLABreached fires and
		// the region lands in its final Overdue leaf (the order is still delivered;
		// the breach is recorded for reporting).
		SubState(OnTime).
		After(SLAWindow).On(SLABreached).GoTo(Overdue).Assign("markBreached").
		SubState(Overdue).Final().
		EndRegion().
		// Cross-cutting transitions on the Active compound apply to any active
		// substate. DroppedOff (the courier actor's completion) exits the parallel
		// configuration to Settling and folds the drop proof into the context; Cancel
		// opens the compensation saga, exiting to Refunding.
		Transition(Active).On(DroppedOff).GoTo(Settling).Assign("recordDrop").
		Transition(Active).On(Cancel).GoTo(Refunding).
		EndSuperState().
		// Settling captures the held payment and runs to the Delivered terminal.
		State(Settling).
		Transition(Settling).Always().GoTo(Delivered).Assign("settle").
		State(Delivered).Final().
		// Refunding is the saga's compensation: it invokes the refund service and, on
		// its onDone, folds the reversed amount into the context via the event before
		// reaching the Canceled terminal.
		State(Refunding).
		Invoke("refund", Refunded, Declined).
		Transition(Refunding).On(Refunded).GoTo(Canceled).Assign("recordRefund").
		State(Canceled).Final().
		// Rejected is the declined-authorization terminal.
		State(Rejected).Final().
		Initial(Placed).
		CurrentStateFn(func(Order) Stage { return Placed }).
		Quench()
}
