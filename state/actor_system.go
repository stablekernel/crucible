package state

import (
	"context"
	"sort"
	"sync"
)

// This file ships the reusable host-driver harness for the actor model. The
// kernel emits SpawnActor / StopActor effects and stays pure; an ActorSystem is
// the small, documented runtime that turns those effects into running child
// machines (actors), owns each actor's mailbox, routes events into mailboxes,
// steps actors via Fire, and on a child actor's completion re-fires the parent's
// onDone / onError event back through the parent's Fire. A production host runs
// actors on real goroutines; the deterministic harness steps actors synchronously
// so actor machines are fully testable without real concurrency.
//
// # Host-driver contract
//
// A host that wants child-machine actors to actually run wraps its (parent)
// instance in an ActorSystem, registers the child-machine behaviors in the actor
// palette, and routes every Fire's effects through it:
//
//	sys := state.NewActorSystem(parent)
//	sys.Register("child", childBehavior)        // bind a child machine by name
//	sys.Absorb(ctx, parent.StartEffects())      // spawn the initial state's actors
//	res := parent.Fire(ctx, ev)
//	dispatch(res.Effects)                        // your own effect dispatch
//	sys.Absorb(ctx, res.Effects)                 // spawn/stop actors from the effects
//
// Absorb scans the effects for SpawnActor (create + run the named child actor) and
// StopActor (stop a running actor and its children). Deliver routes an event into
// a running actor's mailbox; Step drains an actor's mailbox, firing each event
// through that actor and — when the actor reaches its final state — re-firing the
// parent's onDone (carrying the child's output) back through the parent's Fire, or
// on the child's failure onError. A production system runs Step on the actor's own
// goroutine off its mailbox; the deterministic harness drains mailboxes only when
// the test drives it, so actor machines are exercised with no real concurrency.

// ActorBehavior creates a fresh child-machine actor instance bound to the given
// input. It is the actor-palette analog of a ServiceFn: a host registers one per
// child-machine src name, and the ActorSystem calls it to spawn an actor when it
// absorbs a SpawnActor effect for that src. The returned ActorInstance erases the
// child's own (S, E, C) generic parameters behind the ActorInstance interface, so
// a parent of any type can host children of any type. The input is the SpawnActor
// Input is the actor input; a behavior typically Casts its child machine with a
// WithInitialState derived from input.
type ActorBehavior func(input map[string]any) (ActorInstance, error)

// ActorInstance is a running child actor as the ActorSystem sees it, with the
// child's own (S, E, C) generic parameters erased. A host obtains one by wrapping
// a Cast child *Instance with NewActor; the deterministic test driver and the
// production driver both drive actors purely through this interface.
type ActorInstance interface {
	// DeliverFire fires one event through the actor, returning whether the actor
	// reached its final state and the output it exposes on completion. The event is
	// the actor's own event type, passed type-erased; an implementation type-asserts
	// it and ignores an event of the wrong type (a no-op, mirroring the kernel's
	// effect-type guards). A backing *Instance implementation also surfaces the
	// SpawnActor / StopActor effects the child itself emitted, so the system can run
	// nested actors — those are returned via ChildEffects.
	DeliverFire(ctx context.Context, event any) (done bool, output any)
	// ChildEffects returns the actor effects the actor emitted on its most recent
	// DeliverFire (and on its initial entry): the SpawnActor / StopActor lifecycle
	// effects so the ActorSystem can spawn or stop the actor's own children, and the
	// SendTo / SendParent / RespondToSender / ForwardEvent communication effects so
	// the system can route the actor's outbound messages. It returns a fresh slice
	// each call and drains the buffer.
	ChildEffects() []Effect
	// Output returns the actor's completion output once it has reached its final
	// state, or nil before then. It lets a host expose a snapshot's output.
	Output() any
}

// envelope is a queued message: the event plus the id of the actor that sent it
// (the sentinel parentActorID for the parent, empty for a host-injected event with
// no origin). The sender is what a RespondToSender resolves against.
type envelope struct {
	event  any
	sender string
}

// running is one running child actor tracked by an ActorSystem.
type runningActor[E comparable] struct {
	inst     ActorInstance
	mailbox  []envelope
	ref      ActorRef
	onDone   E
	onError  E
	hasDone  bool // onDone is a usable parent event (routes completion)
	hasError bool // onError is a usable parent event (routes failure)
	state    string
	done     bool // the actor has reached its final state and been settled
	children []string
	// sender records the id of the actor whose message this actor is currently
	// handling (the origin of the event most recently delivered into its mailbox), so
	// a RespondToSender this actor emits resolves back to that origin. Empty when the
	// current event was injected with no actor origin (then respond is a no-op). The
	// sentinel parentActorID identifies the parent instance as the origin.
	sender string
}

// parentActorID is the sentinel id the routing layer uses for the parent instance
// (which has no registry entry of its own). A SendParent routes to it, and it is
// recorded as the sender when the parent injects a message into a child, so a
// child's RespondToSender routes back to the parent.
const parentActorID = "\x00parent"

// ActorSystem is the reusable host-driver that turns the kernel's SpawnActor /
// StopActor effects into running child-machine actors, owns each actor's mailbox,
// routes delivered events into mailboxes, steps actors via Fire, and re-fires the
// parent's onDone / onError when a child completes or fails. It is
// concurrency-safe. Construct one per parent instance with NewActorSystem, then
// Register the child-machine behaviors that resolve SpawnActor Src refs; drive it
// by passing each Fire's effects (and the parent's StartEffects) to Absorb, and
// step actors with Deliver / Step.
//
// In the deterministic form the system records each spawned actor and steps it
// only when the test calls Deliver / Step, so actor machines are exercised with no
// real concurrency; a production host instead runs each actor's Step on its own
// goroutine fed by the mailbox.
type ActorSystem[S comparable, E comparable, C any] struct {
	parent *Instance[S, E, C]

	mu       sync.Mutex
	palette  map[string]ActorBehavior
	actors   map[string]*runningActor[E]
	bySystem map[string]string // systemId -> id

	// lastOutcome holds the most recently settled actor's output/error so the
	// parent action bound to the onDone / onError transition can read it. It is set
	// under mu immediately before the routing parent Fire and read by the
	// transition's action through LastOutput / LastError.
	lastOutcome actorOutcome

	// lastEscalation holds the most recent unhandled child-actor failure that
	// escalated to the parent because no onError was wired for it (the G3
	// escalate-to-parent default). It is the always-on observable record a failure
	// surfaces through even when no inspector and no handler are wired, so a crash is
	// never silently swallowed. Read through LastEscalation.
	lastEscalation *ActorEscalation

	// escalationHandler is the optional host policy invoked for each escalation. It
	// is nil by default — the default escalation behavior (record + inspect) needs no
	// handler — and wired with WithEscalationHandler.
	escalationHandler EscalationHandler

	// parentSender records the id of the actor whose message the parent instance is
	// currently handling, so a RespondToSender the parent emits resolves back to that
	// actor. Empty when the parent is handling a host-injected event with no actor
	// origin (then respond is a no-op). It is the parent-side twin of
	// runningActor.sender.
	parentSender string

	// inspector is the optional live observer sink fed actor-lifecycle (spawned /
	// stopped) and inter-actor message (sent / delivered) inspection events. It is
	// nil by default — the system never calls one unless WithActorInspector wired it
	// — so actor inspection is zero-overhead off. It is the host-driver complement to
	// the per-instance Inspector wired with WithInspector: the ActorSystem owns
	// spawning, stopping, and message delivery, which the pure Fire step never sees.
	inspector Inspector
}

// WithActorInspector wires a live observer sink fed the ActorSystem's
// actor-lifecycle and inter-actor message inspection events — actor spawned /
// stopped, and message sent / delivered (the actor-to-actor
// flavor of an event). Pass the same Inspector also wired to the parent
// instance (WithInspector) to observe the whole system on one sink. It is off by
// default; an un-inspected system pays nothing.
func (s *ActorSystem[S, E, C]) WithActorInspector(insp Inspector) *ActorSystem[S, E, C] {
	s.mu.Lock()
	s.inspector = insp
	s.mu.Unlock()
	return s
}

// inspectActor feeds one actor-lifecycle inspection event to the system's
// inspector when one is registered. The nil-inspector default short-circuits, so
// an un-inspected system never allocates or calls. It must be called without the
// system mutex held, since the inspector is host code that may re-enter the system.
func (s *ActorSystem[S, E, C]) inspectActor(id, src string, phase ActorPhase) {
	insp := s.inspectorRef()
	if insp == nil {
		return
	}
	insp.Inspect(InspectionEvent{
		Kind:       InspectActor,
		Machine:    s.parent.machine.name,
		ActorID:    id,
		ActorSrc:   src,
		ActorPhase: phase,
	})
}

// inspectMessage feeds one inter-actor message inspection event to the system's
// inspector when one is registered. Like inspectActor it must be called without the
// system mutex held.
func (s *ActorSystem[S, E, C]) inspectMessage(senderID, targetID string, phase MessagePhase, message any) {
	insp := s.inspectorRef()
	if insp == nil {
		return
	}
	insp.Inspect(InspectionEvent{
		Kind:         InspectMessage,
		Machine:      s.parent.machine.name,
		SenderID:     senderID,
		TargetID:     targetID,
		MessagePhase: phase,
		Message:      fmtState(message),
	})
}

// inspectorRef reads the registered inspector under the system mutex, so a
// concurrent WithActorInspector is observed safely.
func (s *ActorSystem[S, E, C]) inspectorRef() Inspector {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inspector
}

// NewActorSystem returns an ActorSystem driving parent: the instance whose
// SpawnActor / StopActor effects spawn and stop child actors, and through whose
// Fire a completed child's onDone / onError is routed. Register child-machine
// behaviors with Register before absorbing spawn effects.
func NewActorSystem[S comparable, E comparable, C any](parent *Instance[S, E, C]) *ActorSystem[S, E, C] {
	return &ActorSystem[S, E, C]{
		parent:   parent,
		palette:  map[string]ActorBehavior{},
		actors:   map[string]*runningActor[E]{},
		bySystem: map[string]string{},
	}
}

// Register binds a child-machine behavior under src in the system's actor palette,
// so a SpawnActor whose Src.Name is src resolves to behavior. It is the actor-model
// analog of Registry.Service: a host registers each child machine it can spawn.
// Registering returns the system for chaining.
func (s *ActorSystem[S, E, C]) Register(src string, behavior ActorBehavior) *ActorSystem[S, E, C] {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.palette[src] = behavior
	return s
}

// Absorb scans effects, spawning an actor for each SpawnActor (resolving its Src
// against the palette and running the child machine) and stopping the actor for
// each StopActor (auto-stop-on-exit, recursively stopping the actor's children).
// It is how a host wires Fire's output back into the system; call it with the
// effects of every Fire (and once with the parent's StartEffects for the initial
// state). A SpawnActor whose OnDone/OnError is not the parent's event type still
// spawns the actor (a fire-and-forget child) but routes no completion event.
//
// A SpawnActor whose Src does not resolve against the palette is settled
// immediately as an error: its OnError (when usable) is fired through the parent so
// the parent routes onError rather than hanging, mirroring the ServiceRunner's
// unbound-service handling.
func (s *ActorSystem[S, E, C]) Absorb(ctx context.Context, effects []Effect) {
	s.absorb(ctx, effects, nil)
}

// AbsorbFor is Absorb for the effects of a host-driven parent Fire(event): it
// additionally lets a ForwardEvent the parent emits forward event verbatim to a
// child. Use it (rather than Absorb) when the parent itself runs forwardTo on a
// host-injected event; Absorb suffices for sendTo / sendParent / respond and all
// lifecycle effects.
func (s *ActorSystem[S, E, C]) AbsorbFor(ctx context.Context, event any, effects []Effect) {
	s.absorb(ctx, effects, event)
}

// absorb scans effects emitted by the parent instance while handling curEvent,
// spawning/stopping actors and routing the parent's outbound messages (the parent
// is the sender). curEvent is nil for the plain Absorb path (a parent ForwardEvent
// is then a no-op, as there is no event to forward).
func (s *ActorSystem[S, E, C]) absorb(ctx context.Context, effects []Effect, curEvent any) {
	for _, eff := range effects {
		switch e := eff.(type) {
		case SpawnActor:
			s.spawn(ctx, e)
		case StopActor:
			s.stop(e.ID)
		case SendTo, SendParent, RespondToSender, ForwardEvent:
			s.routeComm(ctx, parentActorID, curEvent, eff)
		}
	}
}

// spawn creates and registers a running actor from a SpawnActor effect. On an
// unbound Src it fires the parent's onError (when usable) so completion still
// routes.
// propagateTrace puts a freshly spawned or restored child actor into full trace
// mode when the parent instance runs in full mode, so the parent's observability
// choice (notably a journal/replay host such as the durable runner) reaches the
// whole actor subtree. It uses an optional-interface assertion so a host's own
// ActorInstance implementation is left untouched.
func (s *ActorSystem[S, E, C]) propagateTrace(inst ActorInstance) {
	if s.parent == nil || !s.parent.traceFull {
		return
	}
	if ft, ok := inst.(interface {
		inheritObservability(full, unbounded bool)
	}); ok {
		ft.inheritObservability(s.parent.traceFull, s.parent.histUnbounded)
	}
}

func (s *ActorSystem[S, E, C]) spawn(ctx context.Context, e SpawnActor) {
	s.mu.Lock()
	behavior, ok := s.palette[e.Src.Name]
	if _, exists := s.actors[e.ID]; exists {
		s.mu.Unlock()
		return // idempotent: a re-absorbed spawn is a no-op
	}
	s.mu.Unlock()

	if !ok {
		s.routeError(ctx, e, &ErrUnboundActor{Name: e.Src.Name})
		return
	}
	inst, err := behavior(e.Input)
	if err != nil {
		s.routeError(ctx, e, err)
		return
	}
	s.propagateTrace(inst)

	ra := &runningActor[E]{
		inst:  inst,
		ref:   ActorRef{ID: e.ID, SystemID: e.SystemID, Src: e.Src.Name},
		state: e.State,
	}
	if d, ok := e.OnDone.(E); ok {
		ra.onDone = d
		ra.hasDone = true
	}
	if er, ok := e.OnError.(E); ok {
		ra.onError = er
		ra.hasError = true
	}

	s.mu.Lock()
	s.actors[e.ID] = ra
	if e.SystemID != "" {
		s.bySystem[e.SystemID] = e.ID
	}
	s.mu.Unlock()

	s.inspectActor(e.ID, e.Src.Name, ActorSpawned)

	// Run the actor's own initial-entry child effects (nested actors), tracked
	// under this actor so stopping it cascades to them. There is no event in flight
	// on initial entry, so a ForwardEvent here has nothing to forward.
	s.absorbChildren(ctx, e.ID, nil, inst.ChildEffects())
}

// routeError fires the parent's onError for a spawn that could not start (an
// unbound Src or a behavior that returned an error). When the parent declared a
// usable onError the failure routes there (unchanged); when it did not, the spawn
// failure escalates to the parent as a typed ActorEscalation rather than being
// silently swallowed (the G3 default), so a spawn that cannot start is never lost.
func (s *ActorSystem[S, E, C]) routeError(ctx context.Context, e SpawnActor, err error) {
	ev, ok := e.OnError.(E)
	if !ok {
		s.escalate(ctx, &runningActor[E]{ref: ActorRef{ID: e.ID, SystemID: e.SystemID, Src: e.Src.Name}}, "", err)
		return
	}
	res := s.parent.Fire(ctx, ev)
	s.Absorb(ctx, res.Effects)
}

// stop removes the actor under id and recursively stops its children. Stopping an
// unknown id is a no-op (auto-stop-on-exit safety).
func (s *ActorSystem[S, E, C]) stop(id string) {
	s.mu.Lock()
	ra, ok := s.actors[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	delete(s.actors, id)
	if ra.ref.SystemID != "" {
		delete(s.bySystem, ra.ref.SystemID)
	}
	children := append([]string(nil), ra.children...)
	src := ra.ref.Src
	s.mu.Unlock()
	s.inspectActor(id, src, ActorStopped)
	for _, c := range children {
		s.stop(c)
	}
}

// Stop stops the actor identified by ref (and its children), so a machine that
// holds an ActorRef can explicitly tear an actor down. Stopping an unknown actor
// is a no-op.
func (s *ActorSystem[S, E, C]) Stop(ref ActorRef) { s.stop(ref.ID) }

// Ref returns the ActorRef for the running actor under id, and whether such an
// actor is running. The spawning machine stores the ref in its context (the host's
// spawn action reads it from the system after Absorb) to address the actor later.
func (s *ActorSystem[S, E, C]) Ref(id string) (ActorRef, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ra, ok := s.actors[id]
	if !ok {
		return ActorRef{}, false
	}
	return ra.ref, true
}

// RefBySystemID returns the ActorRef for the actor registered under the given
// its systemId, and whether one is running. It lets a sibling address an
// actor by its well-known system name rather than by spawn id.
func (s *ActorSystem[S, E, C]) RefBySystemID(systemID string) (ActorRef, bool) {
	s.mu.Lock()
	id, ok := s.bySystem[systemID]
	if !ok {
		s.mu.Unlock()
		return ActorRef{}, false
	}
	s.mu.Unlock()
	return s.Ref(id)
}

// Running reports the number of live (spawned, not-stopped, not-completed) actors.
// A test asserts on it to confirm an actor spawned or was auto-stopped on exit.
func (s *ActorSystem[S, E, C]) Running() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, ra := range s.actors {
		if !ra.done {
			n++
		}
	}
	return n
}

// IsRunning reports whether an actor with the given id is live.
func (s *ActorSystem[S, E, C]) IsRunning(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ra, ok := s.actors[id]
	return ok && !ra.done
}

// Deliver routes event into the mailbox of the actor identified by ref, then
// drains the actor (Step) so the delivered event is processed and any resulting
// completion is routed to the parent. It returns whether the actor was found
// running. It is the delivery mechanism the sendTo / sendParent / respond /
// forwardTo action sugar routes through; a host (or a test) may also call it
// directly to inject an event into an actor from outside.
func (s *ActorSystem[S, E, C]) Deliver(ctx context.Context, ref ActorRef, event any) bool {
	return s.deliver(ctx, ref.ID, event, "")
}

// deliver enqueues event into the actor under id, tagged with senderID (the origin
// the actor's RespondToSender resolves against), then drains the actor. It reports
// whether the actor was found running; delivering to an unknown or settled actor is
// a no-op returning false.
func (s *ActorSystem[S, E, C]) deliver(ctx context.Context, id string, event any, senderID string) bool {
	s.mu.Lock()
	ra, ok := s.actors[id]
	if !ok || ra.done {
		s.mu.Unlock()
		return false
	}
	ra.mailbox = append(ra.mailbox, envelope{event: event, sender: senderID})
	s.mu.Unlock()
	s.Step(ctx, id)
	return true
}

// DeliverByID is Deliver keyed by raw actor id, for a host that tracks ids rather
// than refs.
func (s *ActorSystem[S, E, C]) DeliverByID(ctx context.Context, id string, event any) bool {
	return s.Deliver(ctx, ActorRef{ID: id}, event)
}

// Step drains the mailbox of the actor under id, firing each queued event through
// the actor in order. When the actor reaches its final state it is settled: the
// parent's onDone event (carrying the child's output) is fired through the parent
// and the resulting effects absorbed; nested-child effects the actor emits are
// absorbed too. It returns the parent FireResults produced by completion routing,
// in order (empty when the actor did not complete). Step is safe to call with an
// empty mailbox (a no-op) and is how the deterministic driver advances an actor;
// a production driver runs it from the actor's own goroutine.
func (s *ActorSystem[S, E, C]) Step(ctx context.Context, id string) []FireResult[S] {
	var out []FireResult[S]
	for {
		s.mu.Lock()
		ra, ok := s.actors[id]
		if !ok || ra.done || len(ra.mailbox) == 0 {
			s.mu.Unlock()
			return out
		}
		env := ra.mailbox[0]
		ra.mailbox = ra.mailbox[1:]
		ra.sender = env.sender
		inst := ra.inst
		s.mu.Unlock()

		done, output, panicErr := deliverFireGuarded(ctx, inst, env.event)
		if panicErr != nil {
			if p, ok := panicErr.(*ErrActorPanic); ok {
				p.ActorID = id
			}
			// A child that panicked while stepping is a failure: settle it as an error
			// so its onError routes, or — with no onError — escalate to the parent. The
			// panic never crashes the host driver.
			if res, ok := s.settle(ctx, id, nil, panicErr); ok {
				out = append(out, res)
			}
			return out
		}
		// Spawn/stop the actor's own children and route the messages it sent. The
		// actor is the sender for any message it emits, and the event it is handling
		// is what a ForwardEvent forwards verbatim.
		s.absorbChildren(ctx, id, env.event, inst.ChildEffects())
		if done {
			if res, ok := s.settle(ctx, id, output, nil); ok {
				out = append(out, res)
			}
			return out
		}
	}
}

// absorbChildren spawns/stops an actor's nested children (tracking the child ids
// under the actor so stopping it stops them) and routes the SendTo / SendParent /
// RespondToSender / ForwardEvent messages the actor emitted while handling
// curEvent. The emitting actor (selfID) is recorded as the sender of every message
// it sends; a ForwardEvent forwards curEvent verbatim.
func (s *ActorSystem[S, E, C]) absorbChildren(ctx context.Context, selfID string, curEvent any, effects []Effect) {
	for _, eff := range effects {
		switch e := eff.(type) {
		case SpawnActor:
			s.spawn(ctx, e)
			s.mu.Lock()
			if ra, ok := s.actors[selfID]; ok {
				if _, live := s.actors[e.ID]; live {
					ra.children = append(ra.children, e.ID)
				}
			}
			s.mu.Unlock()
		case StopActor:
			s.stop(e.ID)
		case SendTo, SendParent, RespondToSender, ForwardEvent:
			s.routeComm(ctx, selfID, curEvent, eff)
		}
	}
}

// routeComm delivers one actor-communication effect emitted by the actor selfID
// while it handled curEvent. SendTo / ForwardEvent address a target by id or
// systemId; SendParent routes to the parent instance; RespondToSender routes to the
// origin of curEvent (selfID's recorded sender). The emitting actor is the sender of
// every message it sends; an unresolved or absent target is a no-op.
func (s *ActorSystem[S, E, C]) routeComm(ctx context.Context, selfID string, curEvent any, eff Effect) {
	switch e := eff.(type) {
	case SendTo:
		if target, ok := s.resolveTarget(e.TargetID, e.SystemID); ok {
			s.dispatch(ctx, target, e.Event, selfID)
		}
	case ForwardEvent:
		if target, ok := s.resolveTarget(e.TargetID, e.SystemID); ok {
			s.dispatch(ctx, target, curEvent, selfID)
		}
	case SendParent:
		// The parent instance has no parent of its own: its sendParent is a no-op,
		// mirroring a top-level machine.
		if selfID == parentActorID {
			return
		}
		s.dispatch(ctx, parentActorID, e.Event, selfID)
	case RespondToSender:
		s.mu.Lock()
		var origin string
		if selfID == parentActorID {
			origin = s.parentSender
		} else if ra, ok := s.actors[selfID]; ok {
			origin = ra.sender
		}
		s.mu.Unlock()
		if origin != "" {
			s.dispatch(ctx, origin, e.Event, selfID)
		}
	}
}

// resolveTarget resolves a send target to a concrete actor id: targetID directly,
// or the actor registered under systemID when targetID is empty. It reports false
// when neither resolves to a known actor (the send is then a no-op). The parent
// sentinel addresses the parent instance.
func (s *ActorSystem[S, E, C]) resolveTarget(targetID, systemID string) (string, bool) {
	if targetID == parentActorID {
		return parentActorID, true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if targetID != "" {
		if _, ok := s.actors[targetID]; ok {
			return targetID, true
		}
		return "", false
	}
	if systemID != "" {
		if id, ok := s.bySystem[systemID]; ok {
			return id, true
		}
	}
	return "", false
}

// dispatch delivers event to targetID on behalf of senderID. The parent sentinel
// routes the event into the parent instance's Fire (recording senderID so a parent
// RespondToSender resolves back); any other id routes into that actor's mailbox and
// drains it. This is the single path every routed message flows through, so parent
// and child delivery share their sender bookkeeping.
func (s *ActorSystem[S, E, C]) dispatch(ctx context.Context, targetID string, event any, senderID string) {
	s.inspectMessage(senderID, targetID, MessageSent, event)
	if targetID == parentActorID {
		s.fireParentFrom(ctx, event, senderID)
		s.inspectMessage(senderID, targetID, MessageDelivered, event)
		return
	}
	if s.deliver(ctx, targetID, event, senderID) {
		s.inspectMessage(senderID, targetID, MessageDelivered, event)
	}
}

// fireParentFrom fires event through the parent instance, recording senderID as the
// origin so a RespondToSender the parent emits resolves back to it, then absorbs the
// parent's resulting effects (spawns/stops and any messages the parent sends). The
// event must be the parent's event type; an event of another type is a no-op.
func (s *ActorSystem[S, E, C]) fireParentFrom(ctx context.Context, event any, senderID string) {
	ev, ok := event.(E)
	if !ok {
		return
	}
	s.mu.Lock()
	prev := s.parentSender
	s.parentSender = senderID
	s.mu.Unlock()

	res := s.parent.Fire(ctx, ev)
	s.Absorb(ctx, res.Effects)

	s.mu.Lock()
	s.parentSender = prev
	s.mu.Unlock()
}

// settle marks the actor done and routes its completion through the parent: on
// success the parent's onDone fires (carrying output via LastOutput), on failure
// onError. It returns the parent FireResult and true when a routing event fired,
// or false when the actor routes no completion or is unknown.
//
// When the actor FAILED and no onError was wired, settle does not silently swallow
// the failure: it escalates to the parent as a typed, observable ActorEscalation
// (the G3 escalate-to-parent default), so an unhandled child crash always surfaces.
// A clean completion with no onDone remains a fire-and-forget no-op.
func (s *ActorSystem[S, E, C]) settle(ctx context.Context, id string, output any, err error) (FireResult[S], bool) {
	s.mu.Lock()
	ra, ok := s.actors[id]
	if !ok {
		s.mu.Unlock()
		return FireResult[S]{}, false
	}
	ra.done = true
	children := append([]string(nil), ra.children...)
	parentID := s.parentOf(id)
	var ev E
	var route bool
	if err != nil {
		ev, route = ra.onError, ra.hasError
	} else {
		ev, route = ra.onDone, ra.hasDone
	}
	s.lastOutcome = actorOutcome{output: output, err: err, set: true}
	s.mu.Unlock()

	// A completed actor's children are torn down with it.
	for _, c := range children {
		s.stop(c)
	}
	if !route {
		// G3 default: an unhandled child FAILURE (no onError wired) escalates to the
		// parent as a typed, observable ActorEscalation rather than being silently
		// swallowed. A clean completion with no onDone is still a fire-and-forget
		// no-op — only a failure escalates.
		if err != nil {
			s.escalate(ctx, ra, parentID, err)
		}
		return FireResult[S]{}, false
	}
	// Deliver the child's output/error to the parent's onDone/onError transition's
	// Assign through the done-event payload (AssignCtx.Event), not a side channel:
	// the reducer reads it from AssignCtx.Event. LastOutput/LastError remain for
	// parent actions that still read the outcome read-only.
	var payload any
	if err != nil {
		payload = err
	} else {
		payload = output
	}
	res := s.parent.Fire(ctx, ev, WithEventData(payload))
	s.Absorb(ctx, res.Effects)
	return res, true
}

// SettleError fails the running actor under id explicitly (e.g. a host-detected
// child crash), routing the parent's onError. It returns the parent FireResult and
// true, or false when id is not running or routes no onError. When no onError was
// wired, the failure escalates to the parent as a typed ActorEscalation rather than
// being swallowed (the G3 default), so the returned false still means "no onError
// event fired" — not "the failure was lost".
func (s *ActorSystem[S, E, C]) SettleError(ctx context.Context, id string, err error) (FireResult[S], bool) {
	return s.settle(ctx, id, nil, err)
}

// actorOutcome is the output/error of the most recently settled actor, exposed
// through LastOutput / LastError for the parent's onDone / onError action to read.
// set distinguishes "no settlement yet" from a zero output.
type actorOutcome struct {
	output any
	err    error
	set    bool
}

// IDs returns the ids of all live actors, sorted, for deterministic host
// iteration (e.g. delivering to or stepping every actor in a stable order).
func (s *ActorSystem[S, E, C]) IDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.actors))
	for id, ra := range s.actors {
		if !ra.done {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// LastOutput returns the output the most recently settled actor produced, and true
// when that settlement was a success. The parent action bound to an actor's onDone
// transition reads it to consume the child's output; it is valid only during the
// synchronous parent Fire the settlement triggers. It returns false after a failure
// or before any settlement.
func (s *ActorSystem[S, E, C]) LastOutput() (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastOutcome.err != nil {
		return nil, false
	}
	return s.lastOutcome.output, s.lastOutcome.set
}

// LastError returns the error the most recently settled actor produced, or nil
// when the last settlement was a success or none has occurred.
func (s *ActorSystem[S, E, C]) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastOutcome.err
}
