// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/telemetry"
)

// FireFunc fires a routed event against a caller-owned instance and returns the
// transition result. It is the stateless mode's escape from the [Store]: the
// host owns instance lifecycle (a transient instance, an externally-persisted
// one, or one resolved from the message itself) and only the transition outcome
// flows back to the binding.
//
// A nil error and a result whose Err is nil is a successful transition; a result
// carrying a [state.ErrInvalidTransition] or [state.ErrGuardFailed] is the
// state-aware rejection. Returning a non-nil error (not the FireResult.Err) is a
// transient failure resolving the instance — a nak.
type FireFunc[K comparable, E comparable] func(ctx context.Context, key K, event E) (state.FireResult[K], error)

// DriveFunc binds a stateless state-machine to a [source.Handler]: each message
// is routed to a key and event, then fired through a caller-supplied [FireFunc]
// with no persistence. It is the mode for a transient or externally-owned machine
// where there is no durable [Store] to commit against.
//
// The emit hand-off, state-aware rejection, and the ack outcome match [Drive],
// minus the load/save and minus version idempotency (a stateless binding has no
// persisted version to dedup against, so redelivery re-fires; supply a [Deduper]
// or use [Drive] for exactly-once):
//
//   - Route/decode failure → [source.Term] (poison).
//   - Fire rejected as illegal for the current state → [source.Reject]
//     (InvalidForState) carrying a [*source.GuardRejection].
//   - FireFunc resolution error, or sink emit failure → [source.Nak] (Retryable).
//   - Success → [source.Ack].
func DriveFunc[K comparable, E comparable](
	fire FireFunc[K, E],
	router Router[K, E],
	opts ...Option,
) source.Handler {
	cfg := newConfig(opts...)
	return func(ctx context.Context, m source.Message) source.Result {
		ctx, span := cfg.tracer.Start(ctx, cfg.spanName)
		defer span.End()

		key, event, err := router(m)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "route failed")
			return source.Term(fmt.Errorf("statemachine: route message: %w", err))
		}

		res, err := fire(ctx, key, event)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "fire-func failed")
			return source.Nak(fmt.Errorf("statemachine: fire func: %w", err))
		}
		if res.Err != nil {
			if reject, ok := classifyFire(res.Err, event, key); ok {
				span.RecordError(reject)
				span.SetStatus(telemetry.StatusError, "invalid for state")
				return source.Reject(reject)
			}
			span.RecordError(res.Err)
			span.SetStatus(telemetry.StatusError, "fire failed")
			return source.Nak(fmt.Errorf("statemachine: fire %v: %w", event, res.Err))
		}

		if err := emitEffects(ctx, cfg.sink, res.Effects); err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "emit failed")
			return source.Nak(fmt.Errorf("statemachine: emit effects: %w", err))
		}

		span.SetStatus(telemetry.StatusOK, "")
		return source.Ack()
	}
}
