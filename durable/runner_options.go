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
