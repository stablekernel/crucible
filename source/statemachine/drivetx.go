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

// TxSink translates a transition's emitted effects into records produced on a
// transactional [source.Tx], so the emitted records and the consumed offset are
// committed exactly once. It is the transactional mirror of [Sink]: where Sink
// fires-and-forgets an effect at an external destination, a TxSink produces the
// effect into the open transaction the consume-process-produce cycle is fenced
// by, via [source.Tx.Produce].
//
// EmitTx is called once per emitted effect, in order, inside the transaction. A
// non-nil error aborts the transaction (so the consumed offset is not committed
// and the input is redelivered) and is returned by the produce side of [DriveTx].
// An effect a TxSink does not recognize should return an error rather than
// silently dropping it, so a missing mapping fails loudly rather than losing an
// output.
type TxSink interface {
	// EmitTx produces the records that realize effect into tx. The effect's
	// concrete type is a crucible/state effect value; a TxSink type-switches on it
	// and calls tx.Produce with the resulting [source.ProducedRecord]s. Returning
	// an error aborts the transaction.
	EmitTx(ctx context.Context, tx source.Tx, effect any) error
}

// TxSinkFunc adapts a plain function to a [TxSink].
type TxSinkFunc func(ctx context.Context, tx source.Tx, effect any) error

// EmitTx calls the underlying function.
func (f TxSinkFunc) EmitTx(ctx context.Context, tx source.Tx, effect any) error {
	return f(ctx, tx, effect)
}

// DriveTx binds a durable state-machine instance to a [source.Handler] with
// exactly-once consume-process-produce on a [source.Transactional] subscription
// (Kafka EOS): the records a transition emits and the consumed offset of the
// inbound message are committed in one atomic unit, so neither the emitted output
// nor the ack can survive without the other.
//
// It is the transactional counterpart of [Drive]. The route, load, fire,
// version-idempotency, and state-aware-rejection semantics are identical; the
// difference is the commit boundary. On a successful fire, DriveTx opens a
// transaction around the message with [source.Transactional.Begin] and, inside
// it, produces the emitted effects through the [TxSink], persists the new state
// through the [Store], and lets Begin commit the produced records together with
// the consumed offset. It then returns [source.Manual]: the [source.Hopper] takes
// no further settle action because Begin already committed the offset.
//
// Outcomes:
//
//   - Route/decode failure → [source.Term] (poison), settled by the Hopper (no
//     transaction is opened).
//   - Redelivery already applied (message id equals the persisted LastEventID) →
//     [source.Skip] (acked, never re-fired). The Hopper settles it; the skip is a
//     plain offset advance, not a transaction.
//   - Fire rejected as illegal for the current state → [source.Reject] (Term,
//     InvalidForState), settled by the Hopper.
//   - Begin/emit/persist failure, or a broker abort (a rebalance fences the
//     producer) → [source.Nak] (Retryable): the transaction aborted, the offset
//     was not committed, and the message is redelivered.
//   - Success → [source.Manual] after Begin commits the emitted records and the
//     consumed offset atomically.
//
// A note on persist ordering: the [Store.Save] runs inside the transaction, so a
// broker abort after a successful Save leaves the instance advanced but the
// offset uncommitted. The redelivery is then deduplicated by version
// (LastEventID), so it acks as a no-op rather than double-applying — the
// exactly-once-into-the-machine guarantee covers the gap the offset abort opens.
//
// Use DriveTx only with a subscription that satisfies [source.Transactional]
// (Kafka built with kafka.WithTransactional). On any other backend the capability
// is absent; use [Drive] for the at-least-once path. The Hopper must run the
// handler with [source.WithConcurrency](1) per key so one key's transactions do
// not interleave (its default per-key ordering already provides this).
func DriveTx[K comparable, E comparable, C any](
	machine *state.Machine[K, E, C],
	store Store[K, K, E, C],
	router Router[K, E],
	tx source.Transactional,
	sink TxSink,
	opts ...Option,
) source.Handler {
	cfg := newConfig(opts...)
	if tx == nil || sink == nil {
		// Fail loudly on a wiring error rather than panicking on the first message:
		// every message naks with the configuration error so the misconfiguration
		// surfaces in logs and metrics without losing data.
		return func(context.Context, source.Message) source.Result {
			return source.Nak(ErrNotTransactional)
		}
	}
	return func(ctx context.Context, m source.Message) source.Result {
		ctx, span := cfg.tracer.Start(ctx, cfg.spanName)
		defer span.End()

		key, event, err := router(m)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "route failed")
			return source.Term(fmt.Errorf("statemachine: route message: %w", err))
		}

		rec, loaded, err := store.Load(ctx, key)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "load failed")
			return source.Nak(fmt.Errorf("statemachine: load instance: %w", err))
		}

		eventID := cfg.eventID(m)
		// Signal when the message carries no event id: without one the version
		// dedup that covers a broker abort after Save cannot fire, so a redelivery
		// would re-fire. Surface it on the span so the lost guarantee is visible.
		span.SetAttributes(telemetry.Bool("statemachine.exactly_once", eventID != ""))
		if loaded && eventID != "" && rec.LastEventID == eventID {
			// Already folded into the persisted version: ack as a no-op. This is a
			// plain offset advance, not a transactional produce, so it is settled by
			// the Hopper rather than through Begin.
			span.SetStatus(telemetry.StatusOK, "")
			return source.Skip()
		}

		inst, err := instanceFor(machine, rec, loaded)
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

		next := Record[K, E, C]{
			Snapshot:    inst.Snapshot(),
			Version:     rec.Version + 1,
			LastEventID: eventID,
		}

		// One atomic unit: emit the effects as produced records, persist the new
		// state, then let Begin commit the produced records and the consumed offset
		// together. Any error inside fn aborts the transaction, so nothing is
		// committed and the message is redelivered.
		beginErr := tx.Begin(ctx, m, func(ctx context.Context, txn source.Tx) error {
			for _, eff := range res.Effects {
				if err := sink.EmitTx(ctx, txn, eff); err != nil {
					return fmt.Errorf("statemachine: emit effect in transaction: %w", err)
				}
			}
			if err := store.Save(ctx, key, next, rec.Version); err != nil {
				return fmt.Errorf("statemachine: persist transition: %w", err)
			}
			return nil
		})
		if beginErr != nil {
			span.RecordError(beginErr)
			span.SetStatus(telemetry.StatusError, "transaction failed")
			return source.Nak(beginErr)
		}

		span.SetStatus(telemetry.StatusOK, "")
		// Begin committed the offset transactionally; the Hopper must not settle
		// again.
		return source.Manual()
	}
}

// ErrNotTransactional reports that [DriveTx] was given a [source.Transactional]
// that is nil, or a [TxSink] that is nil. It is a wiring error surfaced at the
// first message rather than a runtime data condition. Match it with errors.Is.
var ErrNotTransactional = errors.New("statemachine: DriveTx requires a non-nil Transactional and TxSink")
