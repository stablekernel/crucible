package state

import (
	"context"
	"errors"
	"fmt"
)

// This file ships the actor-failure escalation contract: the v1 default for what
// happens when a child-machine actor fails — errors, panics, or is settled with an
// error — and the parent declared no onError handler for it.
//
// The default is escalate-to-parent: an unhandled child failure surfaces to the
// parent as a typed, observable escalation rather than being silently swallowed.
// This is the least-surprising default and the one mature actor runtimes converge
// on — a silently-lost crash is the classic actor footgun. An explicit onError
// still handles the failure locally (unchanged); escalation is only the default
// when no onError was wired.
//
// The escalation is:
//
//   - Typed. The failure is carried as a *ActorEscalation error, discoverable with
//     errors.As, that wraps the underlying child error (errors.Unwrap) and names
//     the failed actor (its id, ref name, and optional systemId).
//   - Observable. Every escalation is recorded on the ActorSystem (LastEscalation),
//     and — when an inspector is wired — emitted as an InspectActor event with the
//     ActorEscalated phase, so a failure is never lost to a silent code path.
//   - Routable. A host may register an escalation handler with WithEscalationHandler
//     to receive each escalation and decide how the parent reacts (e.g. fire a
//     parent event, propagate further, or stop). A child actor's unhandled failure
//     additionally escalates to ITS parent actor's escalation, so a failure climbs
//     the supervision chain (child -> parent -> grandparent) until a handler or the
//     system root observes it.
//
// Supervision STRATEGIES (restart / resume / backoff / stop-trees) remain reserved
// for a later release; the escalation default frozen here is the seam they layer
// on. An EscalationHandler is the additive opt-in; the default — record + inspect —
// is what every system gets for free, so no failure is ever silent.

// ActorEscalation is the typed failure an unhandled child-machine actor error
// raises to its parent. It is produced when an actor fails (an error settlement, a
// behavior that could not start, a panic recovered while the actor stepped, or an
// explicit SettleError) and the spawning parent declared no onError event for that
// actor: rather than swallow the failure, the ActorSystem escalates it.
//
// It is the v1 default escalation signal — the actor-model analog of an unhandled
// crash propagating up a supervision hierarchy. It wraps the underlying child error
// (so errors.Unwrap and errors.As reach it) and identifies the failed actor.
type ActorEscalation struct {
	// ActorID is the registry id of the actor that failed.
	ActorID string
	// SystemID is the failed actor's system-scoped name, empty when it had none.
	SystemID string
	// Src is the actor ref name the failed actor was spawned from.
	Src string
	// ParentID is the id of the actor the failure escalated TO: the failed actor's
	// parent actor, or empty when it escalated to the parent instance (the system
	// root), which has no actor id of its own.
	ParentID string
	// Err is the underlying child failure that triggered the escalation.
	Err error
}

// Error renders the escalation, naming the failed actor and the wrapped cause.
func (e *ActorEscalation) Error() string {
	return fmt.Sprintf("crucible/state: actor %q escalated unhandled failure: %v", e.ActorID, e.Err)
}

// Unwrap returns the underlying child failure so errors.Is / errors.As reach the
// cause an escalation wraps.
func (e *ActorEscalation) Unwrap() error { return e.Err }

// EscalationHandler receives an actor failure that escalated to the parent because
// no onError was declared for it. It is the host-side opt-in for reacting to an
// unhandled child failure: a handler may fire a parent event, tear other actors
// down, propagate further, or record the failure. It is wired with
// WithEscalationHandler and is invoked once per escalation, outside the system
// mutex, so it may safely re-enter the ActorSystem.
//
// Returning no error acknowledges the escalation (the default record + inspect
// still occurred). The handler does not replace the typed record or the inspector
// event — those always happen — it adds host policy on top of the frozen default.
type EscalationHandler func(ctx context.Context, esc *ActorEscalation)

// WithEscalationHandler registers handler as the system's escalation handler,
// invoked for each child-actor failure that escalates because no onError was wired.
// It is off by default — the default escalation behavior (record on the system plus
// an InspectActor event when an inspector is present) needs no handler — so an
// unwired system still never swallows a failure. Registering returns the system for
// chaining.
func (s *ActorSystem[S, E, C]) WithEscalationHandler(handler EscalationHandler) *ActorSystem[S, E, C] {
	s.mu.Lock()
	s.escalationHandler = handler
	s.mu.Unlock()
	return s
}

// LastEscalation returns the most recent escalation the system recorded, or nil
// when no child failure has escalated. It is the always-on observable record of the
// escalate-to-parent default: even with no inspector and no handler wired, an
// unhandled child failure is retrievable here rather than silently lost.
//
// It is LAST-WRITTEN-WINS, including across a single escalation that climbs the
// supervision chain: a child -> parent -> grandparent climb rewrites this field at
// each level, so after the climb it holds the topmost level reached, not the
// originating failure. Wire an inspector (or an EscalationHandler) when you need the
// FULL record — the inspector stream observes every level of every escalation in
// order; LastEscalation is the convenience snapshot of the most recent one.
func (s *ActorSystem[S, E, C]) LastEscalation() *ActorEscalation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastEscalation
}

// escalate records and surfaces an unhandled child-actor failure. It is called from
// the settle path when an actor fails and the parent declared no onError for it. The
// failure is recorded on the system (LastEscalation), emitted to the inspector as an
// ActorEscalated event, climbed to the failed actor's own parent actor (so a nested
// failure propagates up the supervision chain), and finally handed to a registered
// EscalationHandler. None of these steps swallows the failure: the default with no
// handler is still a recorded, inspectable escalation.
//
// parentID is the id of the failed actor's parent actor, or empty when the failed
// actor was a direct child of the parent instance (the system root).
func (s *ActorSystem[S, E, C]) escalate(ctx context.Context, ra *runningActor[E], parentID string, err error) {
	esc := &ActorEscalation{
		ActorID:  ra.ref.ID,
		SystemID: ra.ref.SystemID,
		Src:      ra.ref.Src,
		ParentID: parentID,
		Err:      err,
	}

	s.mu.Lock()
	s.lastEscalation = esc
	handler := s.escalationHandler
	s.mu.Unlock()

	s.inspectActorEscalated(esc)

	// Climb the supervision chain: if the failed actor had a parent actor (not the
	// parent instance), the failure escalates to that parent actor too, so a nested
	// child -> parent -> grandparent escalation surfaces at every level.
	if parentID != "" {
		s.escalateToActorParent(ctx, parentID, err)
	}

	if handler != nil {
		handler(ctx, esc)
	}
}

// escalateToActorParent propagates a descendant's failure to the actor under
// parentID. When that parent actor itself declared no onError for the originating
// failure it re-escalates to ITS parent, so the failure climbs the chain until a
// handler or the root observes it. The parent actor is not torn down by the climb
// (escalation is observation/notification, not a stop); supervision STRATEGIES that
// act on the climb are reserved.
func (s *ActorSystem[S, E, C]) escalateToActorParent(ctx context.Context, parentID string, err error) {
	s.mu.Lock()
	parent, ok := s.actors[parentID]
	if !ok {
		s.mu.Unlock()
		return
	}
	// The grandparent the parent actor itself reports to, for a continued climb.
	grandparentID := s.parentOf(parentID)
	s.mu.Unlock()

	esc := &ActorEscalation{
		ActorID:  parent.ref.ID,
		SystemID: parent.ref.SystemID,
		Src:      parent.ref.Src,
		ParentID: grandparentID,
		Err:      err,
	}
	s.mu.Lock()
	s.lastEscalation = esc
	s.mu.Unlock()
	s.inspectActorEscalated(esc)

	if grandparentID != "" {
		s.escalateToActorParent(ctx, grandparentID, err)
	}
}

// parentOf returns the id of the actor that owns childID as a nested child, or
// empty when childID is a direct child of the parent instance (the system root).
// It must be called with the system mutex held.
//
// It scans every actor's children, so it is O(actors x children-per-actor) per
// call and a climb up a deep chain is quadratic in the system size. That is fine
// for the small actor counts a single instance supervises today; revisit (e.g. a
// child->parent index) when supervision trees grow large.
func (s *ActorSystem[S, E, C]) parentOf(childID string) string {
	for id, ra := range s.actors {
		for _, c := range ra.children {
			if c == childID {
				return id
			}
		}
	}
	return ""
}

// deliverFireGuarded fires event through inst, recovering any panic the actor raises
// while it steps so a panicking child never crashes the host driver. On a clean
// step it returns the actor's done flag and output with a nil error; on a panic it
// returns a non-nil *ActorPanicError carrying the recovered value, which the caller
// settles as a failure (routing onError or escalating).
func deliverFireGuarded(ctx context.Context, inst ActorInstance, event any) (done bool, output any, panicErr error) {
	defer func() {
		if r := recover(); r != nil {
			done, output, panicErr = false, nil, &ActorPanicError{Recovered: r}
		}
	}()
	done, output = inst.DeliverFire(ctx, event)
	// A clean Go step may still have returned a FireResult.Err: the kernel now
	// recovers a panicking host action/guard/assign into a typed error rather than
	// letting it unwind, so a child failure surfaces here instead of as a Go panic.
	// Treat it as a failure so it settles (routing onError or escalating). A panic
	// recovered into an *ActionPanicError is re-rendered as an *ActorPanicError
	// carrying the original recovered value, preserving the panic-failure surface.
	if fe, ok := inst.(fireErrer); ok {
		if err := fe.FireErr(); err != nil {
			var ap *ActionPanicError
			if errors.As(err, &ap) {
				return false, nil, &ActorPanicError{Recovered: ap.Recovered}
			}
			return false, nil, err
		}
	}
	return done, output, nil
}

// fireErrer is the optional interface a backing ActorInstance implements to expose
// the FireResult.Err of its most recent DeliverFire, so deliverFireGuarded can
// settle a child whose fire failed without that error having been a Go panic. The
// in-process actorAdapter implements it; a host's own ActorInstance need not.
type fireErrer interface {
	FireErr() error
}

// inspectActorEscalated feeds one actor-escalation inspection event to the system's
// inspector when one is registered. Like the other inspect helpers it short-circuits
// on the nil-inspector default and must be called without the system mutex held.
func (s *ActorSystem[S, E, C]) inspectActorEscalated(esc *ActorEscalation) {
	insp := s.inspectorRef()
	if insp == nil {
		return
	}
	insp.Inspect(InspectionEvent{
		Kind:       InspectActor,
		Machine:    s.parent.machine.name,
		ActorID:    esc.ActorID,
		ActorSrc:   esc.Src,
		ActorPhase: ActorEscalated,
	})
}
