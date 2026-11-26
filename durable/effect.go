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
// — the step is its journal ordinal (-1 for Start's entry effects), emission order is
// fixed by the machine definition, and the kind is the effect's stable discriminant — so the same
// effect is stamped with the identical id on the live run and on every recovery
// replay, which re-fires the same events and re-emits the same effects in the
// same order. That stable identity is what makes the dedup set meaningful across
// the crash boundary.
//
// A step an actor system drove — a Fire, a Tick, an actor delivery, or a service
// settle — dispatches the domain effects of the parent Fires the actor drove as its
// own: the step's own effects come first, then each actor-driven parent Fire's, in
// fire order (see drivenFireEffects). Replay re-fires the recorded Fires after the
// step's own work, in the same order, so the ordinal component places a driven
// Fire's effect identically on the live run and on every recovery.
//
// # Write-ahead, exactly-once
//
// The ordering is the contract:
//
//  1. Append the step Record — carrying the stamped effect ids — to the Store
//     (write-ahead: the intent to dispatch is durable before any dispatch).
//  2. Dispatch each effect whose id is not already in the dispatched set, calling
//     the handler, and mark each id dispatched in the Store as it succeeds.
//  3. Checkpoint, if one is due. This comes last so a checkpoint never compacts an
//     effect that hasn't landed.
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
//   - Crash or failed mark AFTER the handler returned: the effect was applied but its
//     id isn't marked, so recovery applies it again. The handler should be idempotent
//     on EffectInstance plus the id for this window, and for a handoff, when two owners
//     can run it at once.
//   - Handler error: the effect is left un-marked, and the Handle fails with
//     ErrHandleFailed wrapping ErrEffectDispatch, so Recover retries it
//     (at-least-once until it succeeds; exactly-once thereafter). A DispatchStore
//     whose Dispatched or MarkDispatched fails leaves it un-marked too, and fails the
//     same way.
//
// The dedup set is held by the Store (DispatchStore) so it shares the Record's
// durability and survives checkpoint compaction — a delayed redispatch of an
// already-applied, since-compacted effect is still recognized and skipped.

// EffectHandler applies one emitted domain effect, identified by its stamped
// EffectID. The Runner calls it once per EffectID over the lifetime of an instance
// (see WithEffectHandler), with three exceptions: a repeated Start can apply the initial
// entry's effects again (see Runner.Start), an effect applies again if the handler
// returned but the mark didn't land, and two owners can run it at once during a handoff.
// An id is unique only within its instance: EffectInstance reads the instance from ctx.
// A non-nil error leaves the effect un-marked for Recover to retry, and fails the Handle
// with ErrHandleFailed wrapping ErrEffectDispatch and the error.
type EffectHandler func(ctx context.Context, effectID string, effect state.Effect) error

// effectInstanceKey is the ctx key for the instance an EffectHandler call belongs to.
type effectInstanceKey struct{}

// withEffectInstance returns ctx carrying id for the handler.
func withEffectInstance(ctx context.Context, id InstanceID) context.Context {
	return context.WithValue(ctx, effectInstanceKey{}, id)
}

// EffectInstance returns the instance an EffectHandler call belongs to, from the ctx the
// handler received. A handler that keeps its own dedupe record must key on this plus the
// effect id, because effect ids repeat across instances.
func EffectInstance(ctx context.Context) (InstanceID, bool) {
	id, ok := ctx.Value(effectInstanceKey{}).(InstanceID)
	return id, ok
}

// DispatchStore is the dedup seam for idempotent effect dispatch: the Store
// tracks which effect ids an instance has applied, so a (re)dispatch skips the
// ones that already landed. It is an additive, optional capability — the core
// Store interface is unchanged; a backend opts in by implementing these two
// methods. A Runner with an effect handler (WithEffectHandler) requires it: Start,
// Recover and the stateless Runner.Fire refuse a Store that doesn't implement it with
// ErrNoDispatchStore, before any Store write or handler call. A Runner with no handler
// dispatches nothing and needs neither method. The in-tree MemStore and FileStore
// implement it. A persistent backend marks an id in the same
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
	step   int // the step that produced the effect, for error messages
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
// emission order. A step that an actor system drove names the driven Fires' effects
// after its own, in fire order (see drivenFireEffects), so their ordinals follow the
// step's own. The ordinal counts only dispatchable effects, so a domain
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
		out = append(out, dispatchableEffect{id: id, step: step, effect: e})
		ordinal++
	}
	return out
}

// metaMarshalError is the EffectEnvelope.Meta key under which recordEffects records
// why a KindedEffect's payload is missing: the message of the error its marshal
// returned. It follows the kernel's "crucible." namespace for reserved names.
const metaMarshalError = "crucible.marshalError"

// recordEffects returns the persisted form of a step's dispatchable effects: an
// EffectEnvelope per effect carrying its stamped EffectID, so the write-ahead
// Record durably names every effect that must be dispatched. A KindedEffect
// marshals into a full envelope (kind + payload); a bare domain effect is named
// by id and kind alone (its value is re-derived by replay, not reconstructed from
// the envelope). A KindedEffect whose payload won't marshal, such as one holding a
// chan, is recorded as a bare effect is, by id and kind with no payload, and its
// envelope's Meta holds the marshal error's message under "crucible.marshalError"
// (metaMarshalError). Replay re-derives its value, so it dispatches, live and on
// Recover, as any effect does, and the step doesn't fail. A MarshalJSON that panics is
// recorded the same way, with the panic's message in Meta.
func recordEffects(des []dispatchableEffect) []state.EffectEnvelope {
	if len(des) == 0 {
		return nil
	}
	out := make([]state.EffectEnvelope, 0, len(des))
	for _, de := range des {
		env := state.EffectEnvelope{Kind: effectKind(de.effect), EffectID: de.id}
		if ke, ok := de.effect.(state.KindedEffect); ok {
			marshaled, err := marshalEffect(ke)
			if err != nil {
				env.Meta = map[string]any{metaMarshalError: validUTF8(err.Error())}
				out = append(out, env)
				continue
			}
			marshaled.EffectID = de.id
			env = marshaled
		}
		out = append(out, env)
	}
	return out
}

// marshalEffect is state.MarshalEffect with a panic in the effect's MarshalJSON
// returned as an error, so recordEffects records the effect by id and kind rather than
// let the panic out of a step the kernel has applied.
func marshalEffect(ke state.KindedEffect) (env state.EffectEnvelope, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("effect payload panicked while encoding: %v", p)
		}
	}()
	return state.MarshalEffect(ke)
}

// checkDispatchStore refuses an effect handler wired with a Store that isn't a
// DispatchStore, which couldn't track which effects were applied. Start and recover,
// which Recover and the stateless Fire share, call it before they touch the Store, so
// every Handle that dispatches has dispatch tracking. The error wraps
// ErrNoDispatchStore, not ErrEffectDispatch: retrying doesn't help.
func (r *Runner[S, E, C]) checkDispatchStore(id InstanceID) error {
	if r.cfg.effectHandler == nil {
		return nil
	}
	if _, ok := r.store.(DispatchStore); !ok {
		return fmt.Errorf("%w: instance %q: a Store that implements DispatchStore tracks the effects the handler applied, and %T doesn't; wire one, or drop WithEffectHandler", ErrNoDispatchStore, id, r.store)
	}
	return nil
}

// dispatching reports whether des has effects to apply: a handler is wired and des
// isn't empty. Otherwise dispatch is a no-op.
func (r *Runner[S, E, C]) dispatching(des []dispatchableEffect) bool {
	return len(des) > 0 && r.cfg.effectHandler != nil
}

// applyStartEffects invokes the handler for the initial entry's domain effects. Like
// dispatch it is a no-op without a handler. It marks nothing, because the instance
// doesn't exist yet and nothing replays these effects.
func (r *Runner[S, E, C]) applyStartEffects(ctx context.Context, id InstanceID, des []dispatchableEffect) error {
	if !r.dispatching(des) {
		return nil
	}
	for _, de := range des {
		if err := r.applyEffect(ctx, id, de); err != nil {
			return err
		}
	}
	return nil
}

// applyEffect calls the effect handler for de, with the instance on its ctx. It wraps a
// handler error, or a panic in the handler, in ErrEffectDispatch, so Start, a step and
// Recover all fail the same way, rather than let a panic out.
func (r *Runner[S, E, C]) applyEffect(ctx context.Context, id InstanceID, de dispatchableEffect) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: effect %q for %q: handler panicked: %v", ErrEffectDispatch, de.id, id, p)
		}
	}()
	if herr := r.cfg.effectHandler(withEffectInstance(ctx, id), de.id, de.effect); herr != nil {
		return fmt.Errorf("%w: effect %q for %q: %w", ErrEffectDispatch, de.id, id, herr)
	}
	return nil
}

// dispatch applies a step's domain effects exactly once. It is the shared
// exactly-once apply loop used by the live path and by recovery replay: the live
// path calls it from finishStep, AFTER the step Record (carrying the effect ids)
// is durably appended, realizing the write-ahead ordering; recovery replay calls
// it over the effects the re-fired step re-emitted, re-stamped with the same
// deterministic ids at its call site. For each effect whose id is not already in
// the dispatched set it invokes the handler and, on success, marks the id
// dispatched. A handler error stops dispatch and is returned wrapped in
// ErrEffectDispatch with the failing effect left un-marked for a later recovery
// to retry. An error or a panic in the Store's Dispatched or MarkDispatched
// becomes an ErrEffectDispatch error wrapping it, as applyEffect makes one in the
// handler, so a step and Recover fail the same way. With no handler wired,
// dispatch is a no-op (the ids are still recorded for a handler-equipped
// recovery). A handler is only ever wired with a DispatchStore: Start and Recover
// refuse one without (see checkDispatchStore).
func (r *Runner[S, E, C]) dispatch(ctx context.Context, id InstanceID, des []dispatchableEffect) (err error) {
	if !r.dispatching(des) {
		return nil
	}
	// Start and recover refuse a handler without dispatch tracking, so this holds on
	// every path today. Should a new one skip them, refuse here too, with the permanent
	// ErrNoDispatchStore rather than a retryable ErrEffectDispatch.
	ds, ok := r.store.(DispatchStore)
	if !ok {
		return r.checkDispatchStore(id)
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: step %d for %q: effect dispatch panicked: %v", ErrEffectDispatch, des[0].step, id, p)
		}
	}()

	dispatched, err := ds.Dispatched(ctx, id)
	if err != nil {
		return fmt.Errorf("%w: loading dispatched set for %q: %w", ErrEffectDispatch, id, err)
	}

	for _, de := range des {
		if dispatched[de.id] {
			continue
		}
		if err := r.applyEffect(ctx, id, de); err != nil {
			return err
		}
		if err := ds.MarkDispatched(ctx, id, de.id); err != nil {
			return fmt.Errorf("%w: marking effect %q dispatched for %q: %w", ErrEffectDispatch, de.id, id, err)
		}
	}
	return nil
}
