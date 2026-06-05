package state

// This file adds the actor-communication action sugar on top of the actor
// runtime (actor.go, actor_system.go): the built-in send/stop actions a machine
// uses to message other actors — sendTo, sendParent, respond, and forwardTo.
// They follow the same shape as the spawn / stop /
// cancel built-ins: the kernel handles each ref directly at Fire time and emits a
// DATA effect (SendTo / SendParent / RespondToSender / ForwardEvent / StopActor)
// alongside the transition's other effects. The kernel never delivers a message,
// never owns a mailbox, and never starts a goroutine — Fire stays pure. A host's
// ActorSystem consumes these effects and performs the actual delivery into the
// target actor's mailbox (or stop), resolving parent / sender / child targets
// from the routing context it already tracks.
//
// Target addressing. A send action names its target by a stable string: an actor
// id (or system-scoped id). Refs are runtime, so they are never embedded in the
// effect or the IR; the structural target (an id / systemId / the literal event)
// serializes and round-trips, and the host resolves the live actor from it. This
// keeps the IR lossless for machines that use the send actions while leaving the
// running ActorRef a pure-runtime value.
//
// Sender tracking for respond. The "sender" of the event currently being handled
// is host runtime state, not kernel state: when an ActorSystem delivers an event
// into an actor, it records which actor (or the parent) originated it. While that
// actor steps the event, a RespondToSender effect it emits is routed back to that
// recorded origin. The kernel emits RespondToSender as data with no target — the
// host fills the target from its routing context. If there is no identifiable
// sender (e.g. the event was injected directly with no origin), respond is a
// no-op when there is no actor to reply to.

// SendTo is the effect the kernel emits for the sendTo built-in: deliver Event to
// the actor addressed by TargetID (or SystemID when TargetID is empty). The
// host's ActorSystem routes it into that actor's mailbox; addressing an unknown
// actor is a no-op. It delivers an event to a named actor.
type SendTo struct {
	// TargetID is the registry id of the actor to deliver Event to. Empty when the
	// target is addressed by SystemID instead.
	TargetID string `json:"targetId,omitempty"`
	// SystemID is the system-scoped name of the target actor (its systemId),
	// used when TargetID is empty so a sibling can be addressed by a well-known name.
	SystemID string `json:"systemId,omitempty"`
	// Event is the serializable event delivered to the target actor's mailbox,
	// type-erased for the abstract effect surface; an ActorSystem keeps it typed.
	Event any `json:"event,omitempty"`
}

// SendParent is the effect the kernel emits for the sendParent built-in: a child
// actor sends Event to its parent. The host's ActorSystem routes it to the parent
// instance (the one driving the system). Emitted by a top-level machine with no
// parent it is a host-side no-op. It routes an event to the actor's parent.
type SendParent struct {
	// Event is the serializable event delivered to the parent, type-erased for the
	// abstract effect surface; an ActorSystem keeps it typed.
	Event any `json:"event,omitempty"`
}

// RespondToSender is the effect the kernel emits for the respond built-in: reply
// with Event to the sender of the event the emitting actor is currently handling.
// The kernel cannot know the sender (it is host routing state), so it emits this
// effect with only the reply Event; the host's ActorSystem resolves the target
// from the routing context it recorded when it delivered the current event. When
// there is no identifiable sender the host treats it as a no-op. This realizes the
// reply-to-the-event's-origin semantic.
type RespondToSender struct {
	// Event is the serializable reply delivered to the current event's sender,
	// type-erased for the abstract effect surface; an ActorSystem keeps it typed.
	Event any `json:"event,omitempty"`
}

// ForwardEvent is the effect the kernel emits for the forwardTo built-in: forward
// the event the emitting actor is currently handling, verbatim, to the actor
// addressed by TargetID (or SystemID). The kernel does not embed the forwarded
// event — the host already has it as the event it just delivered — so this effect
// carries only the target. The host's ActorSystem routes the current event into
// the target's mailbox; addressing an unknown actor is a no-op. This realizes
// forwards the current event verbatim to another actor.
type ForwardEvent struct {
	// TargetID is the registry id of the actor to forward the current event to.
	// Empty when the target is addressed by SystemID instead.
	TargetID string `json:"targetId,omitempty"`
	// SystemID is the system-scoped name of the target actor, used when TargetID is
	// empty.
	SystemID string `json:"systemId,omitempty"`
}

// Reserved action ref names for the actor-communication built-ins. Like the spawn
// / stopActor / cancel built-ins, each is handled directly by the kernel at Fire
// time and needs no host registration, so it is exempt from the unbound-ref lint.
const (
	sendToBuiltinName     = "crucible.sendTo"
	sendParentBuiltinName = "crucible.sendParent"
	respondBuiltinName    = "crucible.respond"
	forwardToBuiltinName  = "crucible.forwardTo"
)

// Reserved params keys for the actor-communication built-ins. Targets are stable
// strings (ids), and the literal event is carried as data, so every param is
// JSON-serializable and round-trips through the IR.
const (
	sendToTargetParam   = "target"
	sendToSystemIDParam = "systemId"
	sendEventParam      = "event"
)

// commMicrostep returns the trace microstep label for an actor-communication or
// stop effect emitted by an action, and whether the effect is one of those. It
// mirrors the auto spawn/stop microstep convention (e.g. "actor.send.<target>") so
// a machine using the send/forward/stop actions leaves an observable record of each
// routing microstep in its trace.
func commMicrostep(e Effect) (string, bool) {
	switch v := e.(type) {
	case SendTo:
		return "actor.send." + commTarget(v.TargetID, v.SystemID), true
	case SendParent:
		return "actor.sendParent", true
	case RespondToSender:
		return "actor.respond", true
	case ForwardEvent:
		return "actor.forward." + commTarget(v.TargetID, v.SystemID), true
	case StopActor:
		return "actor.stop." + v.ID, true
	default:
		return "", false
	}
}

// commTarget renders a send target for a microstep label: the registry id, or the
// system-scoped id prefixed with "@" when addressing by systemId.
func commTarget(targetID, systemID string) string {
	if targetID != "" {
		return targetID
	}
	if systemID != "" {
		return "@" + systemID
	}
	return ""
}

// isCommBuiltinAction reports whether a ref names one of the actor-communication
// built-ins. They are exempt from the unbound-ref lint and handled directly by
// evalCommBuiltinAction.
func isCommBuiltinAction(name string) bool {
	switch name {
	case sendToBuiltinName, sendParentBuiltinName, respondBuiltinName,
		forwardToBuiltinName:
		return true
	default:
		return false
	}
}

// evalCommBuiltinAction runs an actor-communication built-in action ref, returning
// its effect. It is called only for refs isCommBuiltinAction reports true for. The
// effects are pure data; the host's ActorSystem performs the delivery / stop.
func evalCommBuiltinAction(a Ref) (Effect, error) {
	switch a.Name {
	case sendToBuiltinName:
		target, _ := a.Params[sendToTargetParam].(string)
		systemID, _ := a.Params[sendToSystemIDParam].(string)
		return SendTo{TargetID: target, SystemID: systemID, Event: a.Params[sendEventParam]}, nil
	case sendParentBuiltinName:
		return SendParent{Event: a.Params[sendEventParam]}, nil
	case respondBuiltinName:
		return RespondToSender{Event: a.Params[sendEventParam]}, nil
	case forwardToBuiltinName:
		target, _ := a.Params[sendToTargetParam].(string)
		systemID, _ := a.Params[sendToSystemIDParam].(string)
		return ForwardEvent{TargetID: target, SystemID: systemID}, nil
	default:
		return nil, &UnknownBuiltinError{Name: a.Name}
	}
}
