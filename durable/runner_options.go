package durable

import "github.com/stablekernel/crucible/state"

// This file defines the functional-options surface for the durable Runner.
// Required arguments (the machine, the Store, the instance id, the input/event)
// stay positional; every tunable arrives as an additive ...Option so the Runner
// signatures never have to break to grow. Options default to a working baseline:
// no periodic checkpoint (event-sourced from the start baseline) and JSON event
// encoding.

// Option configures a Runner (and, through Recover, a one-shot reconstruction).
// It is generic over the machine's state, event, and context types so an event
// codec can be type-safe.
type Option[S comparable, E comparable, C any] func(*runnerConfig[S, E, C])

// runnerConfig holds resolved Option state for a Runner.
type runnerConfig[S comparable, E comparable, C any] struct {
	// checkpointEvery is the checkpoint policy: persist a full Snapshot and
	// compact the journal tail every N fired steps. Zero (the default) takes no
	// periodic checkpoint, so recovery replays from the start baseline.
	checkpointEvery int
	// eventCodec reconstructs a recorded event for replay; it defaults to JSON.
	eventCodec EventCodec[E]
	// clock is the real time seam wrapped by the recording clock on the live path
	// and replaced by the replay clock on recovery. It defaults to
	// state.SystemClock(); a test supplies a deterministic fake.
	clock state.Clock
	// serviceReg binds the invoked-service implementations (ServiceFn) the Runner
	// resolves and runs on the live path. It is nil by default: a purely
	// event-driven or timer-driven machine invokes no service and needs none. A
	// machine that declares `invoke` states supplies it with WithServiceRegistry.
	serviceReg *state.Registry[C]
	// actorPalette binds the child-machine actor behaviors (ActorBehavior) the
	// Runner spawns and runs on the live path, keyed by the actor src name. It is
	// nil by default: a machine that spawns no child actor needs none. A machine
	// that declares `InvokeActor` (or spawns actors dynamically) supplies it with
	// WithActorPalette.
	actorPalette map[string]state.ActorBehavior
	// effectHandler applies each emitted domain effect exactly once. It is nil by
	// default: without WithEffectHandler the Runner records effect ids but
	// dispatches nothing, so a purely event-driven user is unaffected. A machine
	// whose transitions emit side-effecting domain effects supplies it with
	// WithEffectHandler.
	effectHandler EffectHandler
}

// WithCheckpointEvery sets the checkpoint policy: the Runner persists a full
// Snapshot and compacts the journal tail every n fired steps, bounding both the
// stored tail and the replay length on recovery. A non-positive n disables
// periodic checkpointing (the default), so recovery replays the whole run from
// the start baseline.
func WithCheckpointEvery[S comparable, E comparable, C any](n int) Option[S, E, C] {
	return func(c *runnerConfig[S, E, C]) {
		if n > 0 {
			c.checkpointEvery = n
		}
	}
}

// WithEventCodec overrides the event codec the Runner uses to reconstruct a
// recorded event for replay, for events the default encoding/json codec cannot
// round-trip. The default codec decodes through encoding/json, the inverse of
// the kernel's Trace.EventPayload marshaling.
func WithEventCodec[S comparable, E comparable, C any](codec EventCodec[E]) Option[S, E, C] {
	return func(c *runnerConfig[S, E, C]) {
		if codec != nil {
			c.eventCodec = codec
		}
	}
}

// WithRunnerClock injects the real time seam the Runner records on the live path
// and replays on recovery. A durable instance reads time only through its host
// scheduler (which arms and ticks delayed `after` transitions); the Runner wraps
// this clock in a recording clock so every reading is journaled, and substitutes
// a replay clock on recovery so timers fire at their recorded instants
// independent of the wall clock at recovery time. It defaults to
// state.SystemClock(); supply a deterministic fake (state.NewFakeClock) in a test.
func WithRunnerClock[S comparable, E comparable, C any](c state.Clock) Option[S, E, C] {
	return func(cfg *runnerConfig[S, E, C]) {
		if c != nil {
			cfg.clock = c
		}
	}
}

// WithServiceRegistry binds the invoked-service implementations the Runner runs on
// the live path and resolves on recovery. A durable service runs exactly once —
// live — and its result is recorded; on recovery the recorded result is replayed
// through the same settle seam and the service is never re-invoked, so the registry
// is consulted only to execute a service the first time. Supply it for any machine
// that declares `invoke` states; a purely event-driven or timer-driven machine
// needs none. The registry binds the same ServiceFns the machine declares, by name
// (state.NewRegistry().Service(name, fn)).
func WithServiceRegistry[S comparable, E comparable, C any](reg *state.Registry[C]) Option[S, E, C] {
	return func(c *runnerConfig[S, E, C]) {
		if reg != nil {
			c.serviceReg = reg
		}
	}
}

// WithActorPalette binds the child-machine actor behaviors the Runner spawns and
// runs on the live path and resolves on recovery, keyed by the actor src name (the
// name passed to InvokeActor or the Spawn built-in). A durable actor's behavior
// runs exactly once — live — and its done-data, error, or parent-driving message is
// recorded; on recovery the recorded result is replayed back through the same parent
// transition and the behavior is never re-instantiated, so the palette is consulted
// only to run an actor the first time. Supply it for any machine that spawns child
// actors; a machine that spawns none needs none. Each behavior is the actor-model
// analog of a ServiceFn, registered by the same src name the machine declares.
func WithActorPalette[S comparable, E comparable, C any](palette map[string]state.ActorBehavior) Option[S, E, C] {
	return func(c *runnerConfig[S, E, C]) {
		if len(palette) > 0 {
			c.actorPalette = palette
		}
	}
}

// WithEffectHandler binds the seam the Runner calls to apply each emitted domain
// effect — an email, a charge, a published message: any at-most-once side effect
// a transition emits as a domain Effect value. A durable effect is applied
// exactly once over the whole lifetime of an instance (the live run plus any
// number of recoveries), deduped by a deterministic EffectID: the Runner stamps
// each emitted effect, write-ahead persists the step Record carrying those ids,
// then invokes the handler for every id not already marked dispatched, marking
// each as it succeeds. A handler error is surfaced (wrapped in ErrEffectDispatch)
// and the failing effect is left un-marked, so a later recovery retries it
// (at-least-once until it succeeds; exactly-once once it does).
//
// The handler receives the stamped EffectID and the live effect value. It is nil
// by default: without it the Runner records effect ids but dispatches nothing, so
// an event-driven user is unaffected. Kernel driver effects (services, timers,
// actors) are NOT routed here — they are absorbed by the ServiceRunner,
// Scheduler, and ActorSystem; only domain effects reach the handler.
func WithEffectHandler[S comparable, E comparable, C any](h EffectHandler) Option[S, E, C] {
	return func(c *runnerConfig[S, E, C]) {
		if h != nil {
			c.effectHandler = h
		}
	}
}

// resolveRunner folds opts over the working-baseline defaults.
func resolveRunner[S comparable, E comparable, C any](opts ...Option[S, E, C]) runnerConfig[S, E, C] {
	c := runnerConfig[S, E, C]{
		eventCodec: jsonEventCodec[E]{},
		clock:      state.SystemClock(),
	}
	for _, opt := range opts {
		opt(&c)
	}
	if c.eventCodec == nil {
		c.eventCodec = jsonEventCodec[E]{}
	}
	if c.clock == nil {
		c.clock = state.SystemClock()
	}
	return c
}
