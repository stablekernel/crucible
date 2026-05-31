package durable

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// This file implements idempotent effect dispatch: applying each emitted domain
// effect exactly once despite the at-least-once replay/retry the durable runtime
// relies on. The seam, the deterministic identity, the write-ahead ordering, and
// the dedup model are documented inline, because the exactly-once contract lives
// in the interaction between them rather than in any single piece.
//
// # The effect-handler seam
//
// A transition emits a domain effect — a side effect a state machine cannot
// perform itself (send an email, charge a card, publish a message) — as an Effect
// value returned from an Action. The kernel never applies it; it only routes the
// value out through FireResult.Effects in emission order. The durable Runner owns
// applying it, through the caller-supplied EffectHandler (WithEffectHandler). The
// kernel stays pure: it neither stamps the identity nor dedups; the Runner does
// both, so the exactly-once machinery is entirely host-side.
//
// Kernel driver effects (StartService / ScheduleAfter / SpawnActor and their
// kin) are NOT domain effects: the kernel emits them as data and the existing
// host drivers — ServiceRunner, Scheduler, ActorSystem — absorb them. The
// dispatcher filters those out (dispatchableEffects) so the handler only ever
// sees the host's own domain effects.
//
// # Deterministic EffectID
//
// Each dispatchable effect is stamped with an EffectID that is a pure function of
// the step it was emitted at, its ordinal among the dispatchable effects of that
// step, and its kind: "<step>#<ordinal>#<kind>". Every component is deterministic
// — the step is the Fire ordinal, emission order is fixed by the machine
// definition, and the kind is the effect's stable discriminant — so the same
// effect is stamped with the identical id on the live run and on every recovery
// replay, which re-fires the same events and re-emits the same effects in the
// same order. That stable identity is what makes the dedup set meaningful across
// the crash boundary.
//
// # Write-ahead, exactly-once
//
// The ordering is the contract:
//
//  1. Append the step Record — carrying the stamped effect ids — to the Store
//     (write-ahead: the intent to dispatch is durable before any dispatch).
//  2. Dispatch each effect whose id is not already in the dispatched set, calling
//     the handler, and mark each id dispatched in the Store as it succeeds.
//
// This yields exactly-once effect application over the at-least-once delivery the
// runtime is built on:
//
//   - Crash BETWEEN append and dispatch: the Record (with the ids) survived but
//     the effect was never marked. Recovery replays the step, re-emits the effect,
//     finds its id un-marked, and dispatches it — so the effect lands (at least
//     once) and, because it is then marked, never again.
//   - Crash AFTER dispatch (id marked): recovery re-emits the effect but finds its
//     id already marked and skips it — so it is not redispatched (exactly once).
//   - Handler error: the effect is left un-marked and the error surfaces wrapped in
//     ErrEffectDispatch, so a later recovery retries it (at-least-once until it
//     succeeds; exactly-once thereafter).
//
// The dedup set is held by the Store (DispatchStore) so it shares the Record's
// durability and survives checkpoint compaction — a delayed redispatch of an
// already-applied, since-compacted effect is still recognized and skipped.

// EffectHandler applies one emitted domain effect, identified by its stamped
// EffectID. The Runner calls it exactly once per EffectID over the lifetime of an
// instance (see WithEffectHandler). A non-nil error leaves the effect un-marked
// for a later retry and is surfaced to the caller wrapped in ErrEffectDispatch.
type EffectHandler func(ctx context.Context, effectID string, effect state.Effect) error

// DispatchStore is the dedup seam for idempotent effect dispatch: the Store
// tracks which effect ids an instance has applied, so a (re)dispatch skips the
// ones that already landed. It is an additive, optional capability — the core
// Store interface is unchanged; a backend opts in by implementing these two
// methods, and the Runner dispatches effects only against a Store that does (the
// in-tree MemStore does). A persistent backend marks an id in the same
// transaction that records its side-effect acknowledgement to keep the guarantee
// tight.
type DispatchStore interface {
	// MarkDispatched records that the effects named by effectIDs have been applied
	// for the instance. It is idempotent: re-marking an id is a no-op.
	MarkDispatched(ctx context.Context, id InstanceID, effectIDs ...string) error
	// Dispatched returns the set of effect ids already applied for the instance as
	// a membership map. An instance with none reports an empty (non-nil) map.
	Dispatched(ctx context.Context, id InstanceID) (map[string]bool, error)
}

// dispatchableEffect pairs a stamped EffectID with the live effect value the
// handler applies. Stamping happens on the live path and on replay alike, from
// the same (step, ordinal, kind) inputs, so the id is stable.
type dispatchableEffect struct {
	id     string
	effect state.Effect
}

// effectKind reports the discriminant used in an effect's EffectID and recorded
// envelope: a KindedEffect's stable Kind, falling back to the effect's label for
// a bare domain value, so every dispatchable effect has a deterministic kind
// component.
func effectKind(e state.Effect) string {
	if ke, ok := e.(state.KindedEffect); ok {
		return ke.Kind()
	}
	return fmt.Sprintf("%T", e)
}

// isDriverEffect reports whether e is a kernel driver effect the existing host
// drivers (ServiceRunner / Scheduler / ActorSystem) absorb, rather than a domain
// effect the EffectHandler applies. Driver effects carry a reserved crucible.
// built-in kind; everything else is a domain effect.
func isDriverEffect(e state.Effect) bool {
	ke, ok := e.(state.KindedEffect)
	if !ok {
		return false
	}
	switch ke.Kind() {
	case state.EffectKindSpawnActor,
		state.EffectKindStopActor,
		state.EffectKindStartService,
		state.EffectKindStopService,
		state.EffectKindScheduleAfter,
		state.EffectKindCancelScheduled,
		state.EffectKindSendTo,
		state.EffectKindSendParent,
		state.EffectKindRespondToSender,
		state.EffectKindForwardEvent:
		return true
	default:
		return false
	}
}

// dispatchableEffects filters a step's emitted effects to the domain effects the
// handler applies and stamps each with its deterministic EffectID, preserving
// emission order. The ordinal counts only dispatchable effects, so a domain
// effect's id is independent of how many interleaved driver effects the step also
// emitted — keeping it stable if the driver-effect set ever changes around it.
func dispatchableEffects(step int, effects []state.Effect) []dispatchableEffect {
	var out []dispatchableEffect
	ordinal := 0
	for _, e := range effects {
		if e == nil || isDriverEffect(e) {
			continue
		}
		id := fmt.Sprintf("%d#%d#%s", step, ordinal, effectKind(e))
		out = append(out, dispatchableEffect{id: id, effect: e})
		ordinal++
	}
	return out
}

// recordEffects returns the persisted form of a step's dispatchable effects: an
// EffectEnvelope per effect carrying its stamped EffectID, so the write-ahead
// Record durably names every effect that must be dispatched. A KindedEffect
// marshals into a full envelope (kind + payload); a bare domain effect is named
// by id and kind alone (its value is re-derived by replay, not reconstructed from
// the envelope). A marshal failure is reported so a non-serializable effect never
// silently drops from the durable record.
func recordEffects(des []dispatchableEffect) ([]state.EffectEnvelope, error) {
	if len(des) == 0 {
		return nil, nil
	}
	out := make([]state.EffectEnvelope, 0, len(des))
	for _, de := range des {
		env := state.EffectEnvelope{Kind: effectKind(de.effect), EffectID: de.id}
		if ke, ok := de.effect.(state.KindedEffect); ok {
			marshaled, err := state.MarshalEffect(ke)
			if err != nil {
				return nil, fmt.Errorf("durable: recording effect %q: %w", de.id, err)
			}
			marshaled.EffectID = de.id
			env = marshaled
		}
		out = append(out, env)
	}
	return out, nil
}

// dispatchEffects applies a step's domain effects exactly once. It is called
// AFTER the step Record (carrying the effect ids) is durably appended, realizing
// the write-ahead ordering: for each effect whose id is not already in the
// dispatched set it invokes the handler and, on success, marks the id dispatched.
// A handler error stops dispatch and is returned wrapped in ErrEffectDispatch
// with the failing effect left un-marked for a later recovery to retry. With no
// handler wired, or a Store that is not a DispatchStore, dispatch is a no-op (the
// ids are still recorded for a handler-equipped recovery).
func (h *Handle[S, E, C]) dispatchEffects(ctx context.Context, des []dispatchableEffect) error {
	return h.runner.dispatch(ctx, h.id, des)
}

// dispatchReplayEffects re-dispatches a replayed step's domain effects during
// recovery: it re-stamps the re-emitted effects with the same deterministic ids
// (step and emission order reproduce exactly) and applies only those not already
// marked, so an effect that landed before the crash is skipped and one recorded
// but un-dispatched is applied now. This is the recovery half of the exactly-once
// contract.
func (r *Runner[S, E, C]) dispatchReplayEffects(ctx context.Context, id InstanceID, step int, effects []state.Effect) error {
	return r.dispatch(ctx, id, dispatchableEffects(step, effects))
}

// dispatch is the shared exactly-once apply loop used by the live path and by
// recovery replay: it reads the instance's dispatched set, invokes the handler
// for each not-yet-dispatched effect, and marks each id as it succeeds. It is a
// no-op without a handler or a DispatchStore.
func (r *Runner[S, E, C]) dispatch(ctx context.Context, id InstanceID, des []dispatchableEffect) error {
	if len(des) == 0 || r.cfg.effectHandler == nil {
		return nil
	}
	ds, ok := r.store.(DispatchStore)
	if !ok {
		return nil
	}

	dispatched, err := ds.Dispatched(ctx, id)
	if err != nil {
		return fmt.Errorf("durable: loading dispatched set for %q: %w", id, err)
	}

	for _, de := range des {
		if dispatched[de.id] {
			continue
		}
		if err := r.cfg.effectHandler(ctx, de.id, de.effect); err != nil {
			return fmt.Errorf("%w: effect %q for %q: %v", ErrEffectDispatch, de.id, id, err)
		}
		if err := ds.MarkDispatched(ctx, id, de.id); err != nil {
			return fmt.Errorf("durable: marking effect %q dispatched for %q: %w", de.id, id, err)
		}
	}
	return nil
}
