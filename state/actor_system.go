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
// Input (xstate v5 `input`); a behavior typically Casts its child machine with a
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
	// ChildEffects returns the SpawnActor / StopActor effects the actor emitted on
	// its most recent DeliverFire (and on its initial entry), so the ActorSystem can
	// spawn or stop the actor's own children. It returns a fresh slice each call and
	// drains the buffer.
	ChildEffects() []Effect
	// Output returns the actor's completion output once it has reached its final
	// state, or nil before then. It lets a host expose a snapshot's output.
	Output() any
}

// running is one running child actor tracked by an ActorSystem.
type runningActor[E comparable] struct {
	inst     ActorInstance
	mailbox  []any
	ref      ActorRef
	onDone   E
	onError  E
	hasDone  bool // onDone is a usable parent event (routes completion)
	hasError bool // onError is a usable parent event (routes failure)
	state    string
	done     bool // the actor has reached its final state and been settled
	children []string
}

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
	for _, eff := range effects {
		switch e := eff.(type) {
		case SpawnActor:
			s.spawn(ctx, e)
		case StopActor:
			s.stop(e.ID)
		}
	}
}

// spawn creates and registers a running actor from a SpawnActor effect. On an
// unbound Src it fires the parent's onError (when usable) so completion still
// routes.
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

	// Run the actor's own initial-entry child effects (nested actors), tracked
	// under this actor so stopping it cascades to them.
	s.absorbChildren(ctx, e.ID, inst.ChildEffects())
}

// routeError fires the parent's onError for a spawn that could not start.
func (s *ActorSystem[S, E, C]) routeError(ctx context.Context, e SpawnActor, _ error) {
	ev, ok := e.OnError.(E)
	if !ok {
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
	s.mu.Unlock()
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
// xstate v5 `systemId`, and whether one is running. It lets a sibling address an
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
// running. This is the delivery mechanism the next build's sendTo / sendParent
// action sugar will sit on top of; here a host (or a test) calls it directly.
func (s *ActorSystem[S, E, C]) Deliver(ctx context.Context, ref ActorRef, event any) bool {
	s.mu.Lock()
	ra, ok := s.actors[ref.ID]
	if !ok || ra.done {
		s.mu.Unlock()
		return false
	}
	ra.mailbox = append(ra.mailbox, event)
	s.mu.Unlock()
	s.Step(ctx, ref.ID)
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
		ev := ra.mailbox[0]
		ra.mailbox = ra.mailbox[1:]
		inst := ra.inst
		s.mu.Unlock()

		done, output := inst.DeliverFire(ctx, ev)
		// Spawn/stop the actor's own children from what it emitted.
		s.absorbChildren(ctx, id, inst.ChildEffects())
		if done {
			if res, ok := s.settle(ctx, id, output, nil); ok {
				out = append(out, res)
			}
			return out
		}
	}
}

// absorbChildren spawns/stops a parent actor's nested children, tracking the child
// ids under the parent so stopping the parent stops them.
func (s *ActorSystem[S, E, C]) absorbChildren(ctx context.Context, parentID string, effects []Effect) {
	for _, eff := range effects {
		switch e := eff.(type) {
		case SpawnActor:
			s.spawn(ctx, e)
			s.mu.Lock()
			if ra, ok := s.actors[parentID]; ok {
				if _, live := s.actors[e.ID]; live {
					ra.children = append(ra.children, e.ID)
				}
			}
			s.mu.Unlock()
		case StopActor:
			s.stop(e.ID)
		}
	}
}

// settle marks the actor done and routes its completion through the parent: on
// success the parent's onDone fires (carrying output via LastOutput), on failure
// onError. It returns the parent FireResult and true when a routing event fired,
// or false when the actor routes no completion (fire-and-forget) or is unknown.
func (s *ActorSystem[S, E, C]) settle(ctx context.Context, id string, output any, err error) (FireResult[S], bool) {
	s.mu.Lock()
	ra, ok := s.actors[id]
	if !ok {
		s.mu.Unlock()
		return FireResult[S]{}, false
	}
	ra.done = true
	children := append([]string(nil), ra.children...)
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
		return FireResult[S]{}, false
	}
	res := s.parent.Fire(ctx, ev)
	s.Absorb(ctx, res.Effects)
	return res, true
}

// SettleError fails the running actor under id explicitly (e.g. a host-detected
// child crash), routing the parent's onError. It returns the parent FireResult and
// true, or false when id is not running or routes no onError.
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
