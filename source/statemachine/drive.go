// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"context"
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/telemetry"
)

// Router resolves an inbound message to the instance key it targets and the
// event to fire. A decode/route failure returns a non-nil error, which the
// binding treats as poison ([source.Term]): a message that cannot be routed
// cannot be retried into legibility. The decoded event is the typed event the
// machine fires on.
//
// A Router typically delegates to a [source.Registry] (DecodeTyped) to recover
// the domain value, then projects it to a key and event.
type Router[K comparable, E any] func(m source.Message) (key K, event E, err error)

// Drive binds a durable state-machine instance to a [source.Handler]: each
// message is routed to an instance key and event, the instance is loaded through
// the [Store], the event is fired, the emitted effects are handed to the
// configured [Sink], the new state is persisted, and only then is the message
// acked (ack-after-durable-commit). The event type E must be the machine's event
// type ([state.Machine] is generic over the same E).
//
// Outcomes:
//
//   - Route/decode failure → [source.Term] (poison): unroutable, never retried.
//   - Redelivery already applied (the message id equals the persisted
//     LastEventID) → [source.Skip]: acked, never re-fired (exactly-once).
//   - Fire rejected as illegal for the current state (no transition, or a failing
//     guard) → [source.Reject] (Term, InvalidForState) carrying a
//     [*source.GuardRejection].
//   - Sink emit, persist, or load failure → [source.Nak] (Retryable): a transient
//     infrastructure error, redelivered and re-applied. A persist that lost an
//     optimistic-concurrency race ([ErrConflict]) is also a nak.
//   - Success → [source.Ack] after the transition is durably persisted.
//
// Drive is safe for concurrent use; the [source.Hopper] invokes it from per-key
// ordered lanes, so one key's load→fire→save never interleaves with itself.
func Drive[K comparable, E comparable, C any](
	machine *state.Machine[K, E, C],
	store Store[K, K, E, C],
	router Router[K, E],
	opts ...Option,
) source.Handler {
	return driveOn(machine, store, router, newConfig(opts...))
}

// driveOn is the generic core, split out so the state-key type parameter and the
// instance-key type parameter can both be K (a machine keyed by its own state
// type is the common case) while keeping one implementation. The machine's state
// type S and the instance key K are independent in general; this binding stores
// one instance per key K and uses the machine's own S for snapshots.
func driveOn[K comparable, E comparable, C any](
	machine *state.Machine[K, E, C],
	store Store[K, K, E, C],
	router Router[K, E],
	cfg config,
) source.Handler {
	return func(ctx context.Context, m source.Message) source.Result {
		ctx, span := cfg.tracer.Start(ctx, cfg.spanName)
		defer span.End()

		key, event, err := router(m)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "route failed")
			return source.Term(fmt.Errorf("statemachine: route message: %w", err))
		}

		rec, ok, err := store.Load(ctx, key)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "load failed")
			return source.Nak(fmt.Errorf("statemachine: load instance: %w", err))
		}

		eventID := cfg.eventID(m)
		if ok && eventID != "" && rec.LastEventID == eventID {
			// Exactly-once into the machine: this id was already folded into the
			// persisted version, so re-firing would double-apply. Ack and discard.
			span.SetStatus(telemetry.StatusOK, "")
			return source.Skip()
		}

		inst, err := instanceFor(machine, rec, ok)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "restore failed")
			return source.Nak(fmt.Errorf("statemachine: restore instance: %w", err))
		}

		fromState := inst.Current()
		res := inst.Fire(ctx, event)
		if res.Err != nil {
			if reject, ok := classifyFire(res.Err, event, fromState); ok {
				span.RecordError(reject)
				span.SetStatus(telemetry.StatusError, "invalid for state")
				return source.Reject(reject)
			}
			span.RecordError(res.Err)
			span.SetStatus(telemetry.StatusError, "fire failed")
			return source.Nak(fmt.Errorf("statemachine: fire %v: %w", event, res.Err))
		}

		// consume → transition → emit: hand the transition's effects to the sink in
		// the same step, before the ack, so a failed emit nak's rather than acking a
		// transition whose outputs were lost.
		if err := emitEffects(ctx, cfg.sink, res.Effects); err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "emit failed")
			return source.Nak(fmt.Errorf("statemachine: emit effects: %w", err))
		}

		next := Record[K, E, C]{
			Snapshot:    inst.Snapshot(),
			Version:     rec.Version + 1,
			LastEventID: eventID,
		}
		if err := store.Save(ctx, key, next, rec.Version); err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "save failed")
			return source.Nak(fmt.Errorf("statemachine: persist transition: %w", err))
		}

		span.SetStatus(telemetry.StatusOK, "")
		return source.Ack()
	}
}

// instanceFor restores a persisted instance or casts a fresh one from the
// machine's initial configuration when the key has no record yet.
func instanceFor[K comparable, E comparable, C any](
	machine *state.Machine[K, E, C],
	rec Record[K, E, C],
	loaded bool,
) (*state.Instance[K, E, C], error) {
	if loaded {
		return machine.Restore(rec.Snapshot)
	}
	var zero C
	return machine.Cast(zero), nil
}

// emitEffects hands every effect to the sink in order, stopping on the first
// error so a partial emit nak's the message for redelivery.
func emitEffects(ctx context.Context, sink Sink, effects []state.Effect) error {
	for _, eff := range effects {
		if err := sink.Emit(ctx, eff); err != nil {
			return err
		}
	}
	return nil
}

// classifyFire reports whether a Fire error means the event was illegal for the
// current state — an unmatched transition or a failing guard, the state-aware
// rejection that is poison-by-state rather than transient. When it is, it returns
// a [*source.GuardRejection] wrapping the cause so a consumer recognizes it with
// errors.Is(err, source.ErrInvalidForState); otherwise it reports false and the
// caller treats the error as transient.
func classifyFire[K comparable, E any](err error, event E, from K) (*source.GuardRejection, bool) {
	var invalid *state.InvalidTransitionError
	if errors.As(err, &invalid) {
		return &source.GuardRejection{
			Event: fmt.Sprint(event),
			State: invalid.From,
			Err:   err,
		}, true
	}
	var guard *state.GuardFailedError
	if errors.As(err, &guard) {
		return &source.GuardRejection{
			Event: fmt.Sprint(event),
			State: fmt.Sprint(from),
			Err:   err,
		}, true
	}
	return nil, false
}
