package state

import "context"

// This file defines the actor-model contract: the declarative shape an actor
// invocation takes on a state, the effects the kernel emits so a host runtime
// can run child-machine actors, the runtime actor reference a machine stores in
// its context, and the entry/exit effect emission that drives the actor
// semantics. The kernel itself never runs an actor, never starts a goroutine,
// never owns a mailbox, and never routes a message — Fire stays pure. Entering a
// state that invokes a child MACHINE emits a SpawnActor effect; exiting that
// state before the child reaches its final state emits a StopActor effect
// (auto-stop-on-exit). A built-in `spawn` action emits a SpawnActor
// effect at transition time so a machine can create an actor dynamically. A
// host's ActorSystem consumes these effects, runs the child machine as an actor
// with its own mailbox, routes delivered events into that mailbox, steps the
// actor via Fire, and on the child's done/error re-fires the parent's onDone /
// onError back through the parent's Fire.
//
// Scope: this file ships the actor RUNTIME — child-machine actors, the actor
// system, mailboxes, delivery, and lifecycle. The message-SEND action sugar
// (sendTo / sendParent / respond / forwardTo) lives in actor_comms.go
// and rides on top of the mailbox and Deliver mechanism defined here.
// An actor ref is a runtime value (created when the actor is spawned), so it
// is never part of the IR; the invoke/spawn declarations that produce actors are
// IR and round-trip losslessly.

// ActorKind tags an Invocation as either a host-run service or a child-machine
// actor. The default (ActorKindService) preserves the invoked-services contract
// verbatim; ActorKindMachine marks the invocation as spawning a child MACHINE
// actor, so entering the owning state emits a SpawnActor effect instead of a
// StartService effect, and the host's ActorSystem (not a ServiceRunner) runs it.
//
// It serializes as a bare omitempty integer, so its numeric values are part of
// the FROZEN v1.0 wire contract — a recorded Invocation encodes its kind by the
// integer. The mapping is append-only: existing values may never be reordered or
// repurposed; a new actor kind may only be added with the next unused integer.
// The frozen value -> meaning mapping is:
//
//	0 = ActorKindService (the invoked-services default, a host-run unit of work)
//	1 = ActorKindMachine (invoke a child machine as an actor)
type ActorKind int

// Actor kinds. ActorKindService is the invoked-services default (a host-run unit
// of work); ActorKindMachine invokes a child machine as an actor. The integers
// are a frozen, append-only wire contract (see ActorKind).
const (
	ActorKindService ActorKind = iota
	ActorKindMachine
)

// SpawnActor is the effect the kernel emits when an instance enters a state that
// invokes a child MACHINE actor, or when the built-in spawn action runs. The
// host's ActorSystem is expected to create the actor named by Src (resolved to a
// child machine factory against the system's actor palette), run it with Input,
// register it under ID, and — when the child reaches its final state — re-fire
// OnDone (carrying the child's output) through the PARENT's Fire, or on the
// child's failure re-fire OnError. ID is stable per (instance, owning state,
// invoke index) for a static invoke, or carried explicitly for a dynamic spawn,
// so a later StopActor with the same ID stops exactly this actor.
//
// The kernel never runs the actor itself: it emits this as data alongside the
// transition's other effects, keeping Fire pure (no goroutine, no mailbox, no IO).
type SpawnActor struct {
	// ID identifies the spawned actor. It is stable across the spawn/stop pair for
	// one owning state on one instance (static invoke) or supplied explicitly (a
	// dynamic spawn), so a host keys its actor registry by ID.
	ID string `json:"id"`
	// Src is the actor ref (name + params) the host resolves against its actor
	// palette to obtain the child machine to run.
	Src Ref `json:"src"`
	// Input is the serializable input passed to the child actor at spawn. It
	// is data only; the kernel never inspects it.
	Input map[string]any `json:"input,omitempty"`
	// OnDone is the event the host re-fires through the PARENT's Fire (carrying the
	// child's output) when the child actor reaches its final state, type-erased for
	// the abstract effect surface; an ActorSystem keeps it typed.
	OnDone any `json:"onDone,omitempty"`
	// OnError is the event the host re-fires through the PARENT's Fire (carrying the
	// error) when the child actor fails, type-erased for the abstract effect
	// surface.
	OnError any `json:"onError,omitempty"`
	// State names the owning state whose entry spawned this actor, for diagnostics
	// and host bookkeeping. Empty for a dynamic spawn emitted from a transition.
	State string `json:"state,omitempty"`
	// SystemID is the optional, stable system-scoped identifier the actor registers
	// under in the ActorSystem (its systemId), so a sibling can address it
	// by a well-known name rather than by ref. Empty when unset.
	SystemID string `json:"systemId,omitempty"`
}

// StopActor is the effect the kernel emits when an instance exits a state that
// had a running child-machine actor (auto-stop-on-exit), or when the built-in
// stop action runs. The host's ActorSystem stops the actor registered under ID
// (and, transitively, that actor's own children); stopping an unknown ID is a
// no-op. A state's invoked actors are
// auto-stopped when the state is exited before they complete.
type StopActor struct {
	// ID identifies the actor to stop. It matches the ID of the SpawnActor that
	// began it (auto-stop-on-exit), or an ID supplied to the stop built-in.
	ID string `json:"id"`
}

// ActorRef is the runtime handle a machine stores in its context to address a
// spawned actor later (an actor ref). It is created by the ActorSystem
// when the actor is spawned and surfaced to the spawning machine through the
// system's API, never through the IR — refs are runtime, not serializable
// definition. A ref carries the actor's ID (and optional system-scoped SystemID)
// so the holder can Deliver events to it or read its snapshot through the system.
//
// A ref is an OPAQUE, structured handle, not a raw index or positional slot: a
// holder must treat it as opaque and resolve it only through the ActorSystem API
// (Ref / RefBySystemID / Deliver / Stop), never by constructing one from a slice
// position or relying on its ID as an externally-meaningful integer. Construction
// stays the system's job. This keeps the ref remote-ready: a future ref that
// denotes an actor in another system, process, or host carries additional locator
// data (a system name, a transport address) additively, without breaking any
// holder that already treats the ref opaquely. {ID, SystemID, Node} is the
// in-process projection of that fuller locator shape; Node is empty for a
// local actor and names the owning node for a remote one.
type ActorRef struct {
	// ID is the actor's registry key in the ActorSystem.
	ID string
	// SystemID is the optional system-scoped name the actor registered under
	// (its systemId); empty when the actor was spawned without one.
	SystemID string
	// Src is the actor ref name the actor was spawned from, for diagnostics.
	Src string
	// Node is the locator of the host that owns the actor: empty for an actor in
	// the holder's own in-process ActorSystem, and the owning node's identifier
	// for an actor on another host. The in-process ActorSystem leaves it empty;
	// a distributed host (crucible/cluster) stamps it when it mints a remote ref
	// and routes delivery by it. It is the additive locator the opaque-ref
	// contract reserves, so adding it breaks no holder that treats the ref
	// opaquely.
	Node string
}

// spawnBuiltinName is the reserved action ref name for the spawn built-in. Like
// the Cancel built-in, it needs no host registration: the kernel handles it
// directly at Fire time, emitting a SpawnActor effect from its params, and the
// host's ActorSystem creates and runs the actor.
const spawnBuiltinName = "crucible.spawn"

// stopActorBuiltinName is the reserved action ref name for the stop-actor
// built-in: it emits a StopActor effect from its params so a machine can
// explicitly stop a spawned actor before its natural completion.
const stopActorBuiltinName = "crucible.stopActor"

// Reserved params keys for the spawn / stop-actor built-ins.
const (
	spawnSrcParam      = "src"
	spawnIDParam       = "id"
	spawnInputParam    = "input"
	spawnSystemIDParam = "systemId"
	spawnOnDoneParam   = "onDone"
	spawnOnErrorParam  = "onError"
	stopActorIDParam   = "id"
)

// isActorBuiltinAction reports whether a ref names one of the kernel actor
// built-ins (spawn / stopActor) that the host registry need not provide. They are
// exempt from the unbound-ref lint and handled directly by evalBuiltinAction.
func isActorBuiltinAction(name string) bool {
	return name == spawnBuiltinName || name == stopActorBuiltinName
}

// evalActorBuiltinAction runs a kernel actor built-in action ref, returning its
// effect. It is called only for refs isActorBuiltinAction reports true for.
func evalActorBuiltinAction(a Ref) (Effect, error) {
	switch a.Name {
	case spawnBuiltinName:
		src, _ := a.Params[spawnSrcParam].(string)
		id, _ := a.Params[spawnIDParam].(string)
		input, _ := a.Params[spawnInputParam].(map[string]any)
		systemID, _ := a.Params[spawnSystemIDParam].(string)
		return SpawnActor{
			ID:       id,
			Src:      Ref{Name: src},
			Input:    input,
			OnDone:   a.Params[spawnOnDoneParam],
			OnError:  a.Params[spawnOnErrorParam],
			SystemID: systemID,
		}, nil
	case stopActorBuiltinName:
		id, _ := a.Params[stopActorIDParam].(string)
		return StopActor{ID: id}, nil
	default:
		return nil, &UnknownBuiltinError{Name: a.Name}
	}
}

// actorID builds the stable per-instance identifier for the actor invocation at
// index idx on owning state `from`. The same (machine, from, idx) always yields
// the same ID within a process, so the spawn emitted on entry and the stop
// emitted on exit pair up without per-instance bookkeeping in the kernel.
func actorID[S comparable](machine string, from S, idx int) string {
	return machine + ":" + fmtState(from) + ":actor:" + itoa(idx)
}

// ActorID returns the stable identifier the kernel assigns to the child-machine
// actor invocation at index idx on owning state `from` of machine `machine` when
// the invocation declares no explicit ID. A host or test uses it to correlate a
// SpawnActor with a later StopActor, to Deliver an event to the actor, or to
// assert which actor a StopActor targets.
func ActorID[S comparable](machine string, from S, idx int) string {
	return actorID(machine, from, idx)
}

// actorInvocationID resolves the identifier for an actor invocation: its explicit
// ID when the author supplied one, else the derived stable actor ID for its
// position (the `:actor:` namespace, distinct from a service invoke's `:invoke:`).
func actorInvocationID[S comparable, E comparable, C any](machine string, from S, idx int, inv *Invocation[S, E, C]) string {
	if inv.ID != "" {
		return inv.ID
	}
	return actorID(machine, from, idx)
}

// actorEffectsOnEntry returns the SpawnActor effects for every child-machine
// actor invocation declared on the entered states, in entry order. It reads no
// clock, runs no actor, and performs no IO: it only translates declared actor
// invocations into spawn effects for the host to act on. Service invocations are
// skipped here — they are handled by invokeEffectsOnEntry.
func (i *Instance[S, E, C]) actorEffectsOnEntry(entries []S, tr *Trace) []Effect {
	var out []Effect
	m := i.machine
	for _, s := range entries {
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		for ix := range n.state.Invoke {
			inv := &n.state.Invoke[ix]
			if inv.Kind != ActorKindMachine {
				continue
			}
			id := actorInvocationID(m.name, s, ix, inv)
			out = append(out, SpawnActor{
				ID:       id,
				Src:      inv.Src,
				Input:    inv.Input,
				OnDone:   inv.OnDone,
				OnError:  inv.OnError,
				State:    fmtState(s),
				SystemID: inv.SystemID,
			})
			tr.note("actor.spawn." + id)
		}
	}
	return out
}

// actorEffectsOnExit returns the StopActor effects for every child-machine actor
// invocation declared on the exited states, in exit order. Emitting a stop for an
// actor that may already have completed is safe: the host treats an unknown ID as
// a no-op; this is auto-stop-on-exit.
func (i *Instance[S, E, C]) actorEffectsOnExit(exits []S, tr *Trace) []Effect {
	var out []Effect
	m := i.machine
	for _, s := range exits {
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		for ix := range n.state.Invoke {
			inv := &n.state.Invoke[ix]
			if inv.Kind != ActorKindMachine {
				continue
			}
			id := actorInvocationID(m.name, s, ix, inv)
			out = append(out, StopActor{ID: id})
			tr.note("actor.stop." + id)
		}
	}
	return out
}

// actorAdapter wraps a Cast child *Instance as an ActorInstance, erasing the
// child's own (S, E, C) generic parameters so an ActorSystem parameterized over
// the PARENT's types can host it. It buffers the SpawnActor / StopActor effects
// the child emits (so the system runs the child's own nested actors) and derives
// the child's completion output through an optional extractor.
type actorAdapter[S comparable, E comparable, C any] struct {
	inst    *Instance[S, E, C]
	output  func(*Instance[S, E, C]) any
	pending []Effect
	// fireErr holds the FireResult.Err from the most recent DeliverFire, so the
	// ActorSystem's guarded step can settle a child whose fire failed (e.g. a
	// recovered action/guard/assign panic surfaced as a typed error) as a failure
	// rather than silently swallowing it. It is read once via FireErr and cleared
	// at the start of each DeliverFire.
	fireErr error
}

// NewActor adapts a Cast child *Instance into an ActorInstance an ActorSystem can
// run as a child-machine actor. output, when non-nil, extracts the actor's
// v5 `output` from the child instance once it reaches its final state (typically
// reading the child entity); pass nil for an actor whose completion carries no
// output. The returned ActorInstance is what an ActorBehavior returns. The child's
// initial-entry actor effects (StartEffects) are buffered immediately, so the
// system spawns any actors the child invokes on entry.
func NewActor[S comparable, E comparable, C any](inst *Instance[S, E, C], output func(*Instance[S, E, C]) any) ActorInstance {
	a := &actorAdapter[S, E, C]{inst: inst, output: output}
	for _, eff := range inst.StartEffects() {
		if isActorEffect(eff) {
			a.pending = append(a.pending, eff)
		}
	}
	return a
}

// inheritObservability puts the actor's backing child instance into the parent's
// trace mode: full trace, and (for a durable parent) unbounded history retention so
// the actor's own snapshot round-trips its Traces. The ActorSystem calls it (via an
// optional-interface assertion) when its parent runs that way, so a journal/replay
// host that drives the parent — the durable runner — records each actor's rich
// per-step trace and EventPayload too. It is an internal observability gate, not
// part of the ActorInstance contract, so a host's own ActorInstance implementation
// is unaffected.
func (a *actorAdapter[S, E, C]) inheritObservability(full, unbounded bool) {
	a.inst.traceFull = full
	a.inst.histUnbounded = unbounded
}

// isActorEffect reports whether an effect is one the ActorSystem acts on: a
// lifecycle effect (SpawnActor / StopActor) or an actor-communication send effect
// (SendTo / SendParent / RespondToSender / ForwardEvent). Other effects (e.g. a
// host service effect) are left for the host's own effect dispatch.
func isActorEffect(eff Effect) bool {
	switch eff.(type) {
	case SpawnActor, StopActor, SendTo, SendParent, RespondToSender, ForwardEvent:
		return true
	default:
		return false
	}
}

// DeliverFire fires one event through the wrapped child instance. An event that is
// not the child's event type is ignored (a no-op), mirroring the kernel's
// effect-type guards. It buffers the child's SpawnActor / StopActor effects for
// ChildEffects and reports whether the child reached its final state plus its
// output.
func (a *actorAdapter[S, E, C]) DeliverFire(ctx context.Context, event any) (bool, any) {
	a.fireErr = nil
	ev, ok := event.(E)
	if !ok {
		return a.inst.InFinal(), a.outputIfDone()
	}
	res := a.inst.Fire(ctx, ev)
	a.fireErr = res.Err
	for _, eff := range res.Effects {
		if isActorEffect(eff) {
			a.pending = append(a.pending, eff)
		}
	}
	done := a.inst.InFinal()
	return done, a.outputIfDone()
}

// FireErr returns the FireResult.Err from the most recent DeliverFire, or nil
// when that step fired cleanly. The ActorSystem reads it (via the unexported
// fireErrer interface) to settle a child whose fire failed — for instance a host
// action that panicked and was recovered into a typed ActionPanicError — as a
// failure that routes onError or escalates, rather than swallowing it. It is not
// part of the public ActorInstance contract.
func (a *actorAdapter[S, E, C]) FireErr() error { return a.fireErr }

// ChildEffects returns and drains the buffered child actor effects — the
// SpawnActor / StopActor lifecycle effects and the SendTo / SendParent /
// RespondToSender / ForwardEvent communication effects the child emitted — so the
// ActorSystem can run nested actors and route the child's outbound messages.
func (a *actorAdapter[S, E, C]) ChildEffects() []Effect {
	out := a.pending
	a.pending = nil
	return out
}

// Output returns the child's completion output once it has reached its final
// state, or nil before then.
func (a *actorAdapter[S, E, C]) Output() any { return a.outputIfDone() }

// outputIfDone returns the extracted output when the child is final, else nil.
func (a *actorAdapter[S, E, C]) outputIfDone() any {
	if a.output == nil || !a.inst.InFinal() {
		return nil
	}
	return a.output(a.inst)
}

// InFinal reports whether the instance's current primary leaf is a final state —
// the signal an ActorSystem reads to learn that a child-machine actor has reached
// completion and its parent's onDone should be routed. It is a pure read of the
// active configuration against the machine definition; it never mutates the
// instance and consults no clock or IO. For a parallel active configuration it
// reports whether the whole configuration is complete (every region's active leaf
// final), so a child whose root is parallel completes only when all regions do.
func (i *Instance[S, E, C]) InFinal() bool {
	m := i.machine
	cfg := i.config
	if len(cfg) == 0 {
		cfg = []S{i.current}
	}
	for _, leaf := range cfg {
		n, ok := m.resolveNode(leaf)
		if !ok || !n.state.IsFinal {
			return false
		}
	}
	return true
}
