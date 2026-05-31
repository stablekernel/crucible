package state

// This file ships the inspection API — a live observer sink for an instance's
// runtime activity. The
// kernel stays pure: an inspector is an injected observer registered at Cast and
// off by default (a nil inspector costs nothing), and the notification call reads
// no clock and performs no IO. Each inspection event is largely a Trace step
// surfaced live, so the data is the same structured record Fire already produces;
// the inspector simply receives it as it happens rather than reading History()
// after the fact.
//
// The inspection event taxonomy:
//   - a transition taken                       (InspectTransition)
//   - an event received                       (InspectEvent)
//   - a snapshot update                       (InspectSnapshot)
//   - an actor spawned / stopped              (InspectActor)
//   - a message sent / delivered              (between actors)
//
// The kernel feeds the event/transition/snapshot stream from Fire (in-memory,
// pure-path-safe); the host's ActorSystem feeds the actor lifecycle and message
// stream, since spawning, stopping, and delivery are host concerns the pure Fire
// step never owns.

// InspectKind names a category of inspection event, covering the
// inspection event types.
type InspectKind string

const (
	// InspectEvent marks an event received by an instance.
	InspectEvent InspectKind = "event"
	// InspectTransition marks a transition taken — a macrostep that changed (or
	// re-entered) the configuration, carrying its from/to and the Trace detail
	// (guards, effects, exit/entry cascade). It is the kernel's microstep/transition
	// inspection surface.
	InspectTransition InspectKind = "transition"
	// InspectSnapshot marks a snapshot update: the instance's observable state after
	// an event settled.
	InspectSnapshot InspectKind = "snapshot"
	// InspectActor marks an actor lifecycle change — spawned or stopped
	// — an actor lifecycle change.
	InspectActor InspectKind = "actor"
	// InspectMessage marks a message sent from one actor to another and/or delivered
	// to its target (the actor-to-actor flavor of an event).
	InspectMessage InspectKind = "message"
)

// ActorPhase distinguishes the lifecycle point of an InspectActor event.
type ActorPhase string

const (
	// ActorSpawned marks an actor created and started.
	ActorSpawned ActorPhase = "spawned"
	// ActorStopped marks an actor stopped (completed, errored, or auto-stopped on
	// exit).
	ActorStopped ActorPhase = "stopped"
	// ActorEscalated marks an unhandled child-actor failure escalating to the parent
	// because no onError was wired for it (the escalate-to-parent default). The
	// event's ActorID/ActorSrc name the failed actor; the typed failure itself is
	// retrievable through the ActorSystem's LastEscalation.
	ActorEscalated ActorPhase = "escalated"
)

// MessagePhase distinguishes the lifecycle point of an InspectMessage event: a
// message is observed when it is sent, and again when the host delivers it.
type MessagePhase string

const (
	// MessageSent marks a message emitted toward a target actor (a SendTo /
	// SendParent / Respond / Forward effect being routed).
	MessageSent MessagePhase = "sent"
	// MessageDelivered marks a message handed to its target actor's mailbox.
	MessageDelivered MessagePhase = "delivered"
)

// InspectionEvent is one live observation of an instance's runtime activity. It
// is an inspection event: a tagged record whose populated
// fields depend on Kind. A field that does not apply to a Kind is left zero.
//
// The event is read-only; an Inspector must not retain references to mutable
// values it does not own. The Trace, when present, is the same structured record
// Fire records in History — surfaced live rather than after the fact.
type InspectionEvent struct {
	// Kind tags which observation this is and which fields are populated.
	Kind InspectKind

	// Machine names the machine the observed instance was cast from. Always set.
	Machine string

	// Event is the string rendering of the event that triggered this observation,
	// for InspectEvent, InspectTransition, and InspectSnapshot. Empty for actor
	// lifecycle events with no triggering instance event.
	Event string

	// From and To name the configuration's primary leaf before and after a
	// transition (InspectTransition) or the settled leaf (InspectSnapshot). For an
	// InspectEvent, From is the leaf the event was received in and To is empty.
	From string
	To   string

	// Trace is the structured Fire record for an InspectTransition — the live twin
	// of the entry History() later reports. Nil for non-transition kinds.
	Trace *Trace

	// Configuration is every active leaf after the observed step settled, for
	// InspectSnapshot and InspectTransition. It is a copy; an Inspector may retain
	// it.
	Configuration []string

	// Status is the instance's lifecycle status for InspectSnapshot
	// (running/done/error), so an inspector can observe completion without polling.
	Status Status

	// ActorID, ActorSrc, and ActorPhase describe an InspectActor lifecycle event:
	// the actor's registry id, the ref name it was spawned from, and whether it was
	// spawned or stopped.
	ActorID    string
	ActorSrc   string
	ActorPhase ActorPhase

	// SenderID, TargetID, MessagePhase, and Message describe an InspectMessage
	// event: the originating actor (empty for a host-injected send), the target
	// actor, whether the message was observed on send or on delivery, and the
	// string rendering of the message event.
	SenderID     string
	TargetID     string
	MessagePhase MessagePhase
	Message      string
}

// Inspector is the observer sink an instance (and its ActorSystem) feeds live
// inspection events to. It is registered at Cast with WithInspector and is off by
// default — a nil inspector is never called, so an un-inspected instance pays
// nothing. An Inspector must not mutate the instance or perform blocking IO on the
// hot path; it is the telemetry-style sink the kernel notifies synchronously, in
// the same spirit as the existing Trace/observer ethos.
//
// All methods receive a by-value InspectionEvent so an implementation can retain
// it safely. Implement only the methods that matter and embed BaseInspector to
// no-op the rest.
type Inspector interface {
	// Inspect receives every inspection event. The event's Kind selects the
	// populated fields. A single entry point keeps the interface stable as new
	// kinds are added — a new InspectKind never changes this signature.
	Inspect(ev InspectionEvent)
}

// InspectorFunc adapts a plain function to the Inspector interface, for the common
// case of a single closure sink.
type InspectorFunc func(ev InspectionEvent)

// Inspect calls the underlying function.
func (f InspectorFunc) Inspect(ev InspectionEvent) { f(ev) }

// emitInspection surfaces a settled Fire result to the inspector as an event
// (received), a transition (taken), and a snapshot (the new configuration) — the
// three kernel-owned inspection kinds. It is called once per Fire, after the
// result is produced, only when an inspector is registered, so it adds nothing to
// an un-inspected Fire. Actor lifecycle and message events come from the
// ActorSystem, not here.
func (i *Instance[S, E, C]) emitInspection(res FireResult[S]) {
	if i.inspector == nil {
		return
	}
	tr := res.Trace
	from := tr.FromState
	to := fmtState(res.NewState)

	i.inspector.Inspect(InspectionEvent{
		Kind:    InspectEvent,
		Machine: i.machine.name,
		Event:   tr.Event,
		From:    from,
	})

	traceCopy := tr
	i.inspector.Inspect(InspectionEvent{
		Kind:          InspectTransition,
		Machine:       i.machine.name,
		Event:         tr.Event,
		From:          from,
		To:            to,
		Trace:         &traceCopy,
		Configuration: fmtStates(i.config),
	})

	i.inspector.Inspect(InspectionEvent{
		Kind:          InspectSnapshot,
		Machine:       i.machine.name,
		Event:         tr.Event,
		To:            to,
		Configuration: fmtStates(i.config),
		Status:        i.status(),
	})
}

// fmtStates renders a configuration to its string leaves for an inspection event.
func fmtStates[S comparable](cfg []S) []string {
	if len(cfg) == 0 {
		return nil
	}
	out := make([]string, len(cfg))
	for k, s := range cfg {
		out[k] = fmtState(s)
	}
	return out
}

// status derives the instance's lifecycle status from its configuration: done
// when the whole configuration is final, otherwise running. The host owns
// StatusError, so the kernel never reports it here.
func (i *Instance[S, E, C]) status() Status {
	if i.InFinal() {
		return StatusDone
	}
	return StatusRunning
}
