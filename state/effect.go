package state

import (
	"encoding/json"
	"fmt"
)

// KindedEffect is an effect that reports a stable, serializable discriminant
// without a Go type assertion. Every kernel-emitted built-in effect implements
// it, and a host effect opts in by adding a Kind() method, so effects can be
// journaled, deduped, rendered, and routed across a serialization boundary by
// kind rather than by Go type. The Effect alias stays free-form (Effect = any)
// so a domain may still emit bare values; only KindedEffect participates in the
// envelope round-trip and dispatch-time kind checks.
type KindedEffect interface {
	// Kind returns the stable string discriminant for this effect. It is part of
	// the wire contract: two builds must agree on the kind for an effect to route
	// across a serialization boundary, so a kind is never renamed once shipped.
	Kind() string
}

// Built-in effect kinds. Each is the stable discriminant the matching kernel
// effect reports from Kind() and carries on its serialized envelope. They share
// the reserved crucible. namespace so a host's own effect kinds never collide
// with the kernel's. These are part of the wire contract and are closed-enum
// extended per the unknown-variant policy: a decoder that meets an unrecognized
// kind preserves it (see UnknownEffect) and rejects it only at dispatch.
const (
	EffectKindSpawnActor      = "crucible.spawnActor"
	EffectKindStopActor       = "crucible.stopActor"
	EffectKindStartService    = "crucible.startService"
	EffectKindStopService     = "crucible.stopService"
	EffectKindScheduleAfter   = "crucible.scheduleAfter"
	EffectKindCancelScheduled = "crucible.cancelScheduled"
	EffectKindSendTo          = "crucible.sendTo"
	EffectKindSendParent      = "crucible.sendParent"
	EffectKindRespondToSender = "crucible.respondToSender"
	EffectKindForwardEvent    = "crucible.forwardEvent"
)

// Kind reports the spawn-actor effect discriminant.
func (SpawnActor) Kind() string { return EffectKindSpawnActor }

// Kind reports the stop-actor effect discriminant.
func (StopActor) Kind() string { return EffectKindStopActor }

// Kind reports the start-service effect discriminant.
func (StartService) Kind() string { return EffectKindStartService }

// Kind reports the stop-service effect discriminant.
func (StopService) Kind() string { return EffectKindStopService }

// Kind reports the schedule-after effect discriminant.
func (ScheduleAfter) Kind() string { return EffectKindScheduleAfter }

// Kind reports the cancel-scheduled effect discriminant.
func (CancelScheduled) Kind() string { return EffectKindCancelScheduled }

// Kind reports the send-to effect discriminant.
func (SendTo) Kind() string { return EffectKindSendTo }

// Kind reports the send-parent effect discriminant.
func (SendParent) Kind() string { return EffectKindSendParent }

// Kind reports the respond-to-sender effect discriminant.
func (RespondToSender) Kind() string { return EffectKindRespondToSender }

// Kind reports the forward-event effect discriminant.
func (ForwardEvent) Kind() string { return EffectKindForwardEvent }

// effectLabel renders the diagnostic suffix a Trace records for an emitted
// effect. A KindedEffect (every built-in, plus any host effect that opts in)
// reports its stable Kind, so a journaled trace carries the wire discriminant
// rather than a Go type name; a bare domain effect falls back to its Go type.
func effectLabel(e Effect) string {
	if ke, ok := e.(KindedEffect); ok {
		return ke.Kind()
	}
	return typeName(e)
}

// EffectEnvelope is the serialized form of an effect: a discriminated kind, the
// effect's JSON payload, and an optional extension namespace. It is the output
// half of the data boundary — the shape a host journals, dedupes, renders, or
// emits across a process boundary — mirroring the IR envelope on the input half.
//
// EffectID is reserved: a later ordering contract assigns each emitted effect a
// stable, deterministic identity for journal dedup and replay. The field exists
// in the wire shape now so adding that identity later is non-breaking, but the
// kernel does not populate or stabilize it yet — an inbound EffectID round-trips
// verbatim and otherwise carries no meaning.
type EffectEnvelope struct {
	// Kind is the effect's stable discriminant (see KindedEffect.Kind).
	Kind string `json:"kind"`
	// Payload is the effect's marshaled body. It is opaque to the envelope; an
	// EffectRegistry decodes it into a concrete effect keyed by Kind.
	Payload json.RawMessage `json:"payload,omitempty"`
	// Meta is the reserved per-effect extension namespace — a schema hook and the
	// attachment point for host annotations. The kernel never inspects it; it
	// round-trips verbatim.
	Meta map[string]any `json:"meta,omitempty"`
	// EffectID is the reserved correlation/identity slot. NOT yet stable: the
	// kernel leaves it empty and a later ordering PR will populate it
	// deterministically. An inbound value is preserved on round-trip.
	EffectID string `json:"effectId,omitempty"`

	// extra preserves unknown top-level JSON keys a newer producer emitted so they
	// survive a load -> save cycle (forward-compat). Never inspected by the kernel.
	extra map[string]json.RawMessage
}

// effectEnvelopeKnownKeys is the set of JSON keys EffectEnvelope models; anything
// else is captured into extra and preserved verbatim on round-trip.
var effectEnvelopeKnownKeys = map[string]struct{}{
	"kind": {}, "payload": {}, "meta": {}, "effectId": {},
}

// MarshalJSON encodes an EffectEnvelope, merging its preserved unknown keys back
// in with stable key ordering.
func (e EffectEnvelope) MarshalJSON() ([]byte, error) {
	type alias EffectEnvelope
	return marshalWithExtra(alias(e), e.extra)
}

// UnmarshalJSON decodes an EffectEnvelope and captures any unknown keys into
// extra so they survive re-serialization.
func (e *EffectEnvelope) UnmarshalJSON(data []byte) error {
	type alias EffectEnvelope
	var a alias
	extra, err := captureExtra(data, &a, effectEnvelopeKnownKeys)
	if err != nil {
		return err
	}
	*e = EffectEnvelope(a)
	e.extra = extra
	return nil
}

// UnknownEffect is the preserved form of an effect whose kind the local registry
// does not recognize. It carries the original kind and payload verbatim so an
// unknown effect survives a load -> save cycle byte-for-byte (forward-compat,
// per the closed-enum extension policy). It implements KindedEffect, so it can be
// re-marshaled, but it is never dispatchable — EffectRegistry.Dispatchable
// rejects it with a typed *ErrUnknownEffectKind. The kernel never produces an
// UnknownEffect; only deserialization of a foreign envelope yields one.
type UnknownEffect struct {
	// EffectKind is the unrecognized discriminant, preserved verbatim.
	EffectKind string
	// Payload is the original effect body, preserved verbatim for re-emission.
	Payload json.RawMessage
	// Meta is the preserved extension namespace from the source envelope.
	Meta map[string]any
}

// Kind reports the preserved, unrecognized discriminant.
func (u UnknownEffect) Kind() string { return u.EffectKind }

// MarshalEffect serializes a KindedEffect into an EffectEnvelope. The effect's
// Kind becomes the envelope discriminant and the effect marshals to the payload.
// An UnknownEffect re-emits its preserved kind, payload, and meta verbatim so a
// foreign effect survives a round-trip without the local build understanding it.
func MarshalEffect(eff KindedEffect) (EffectEnvelope, error) {
	if u, ok := eff.(UnknownEffect); ok {
		return EffectEnvelope{Kind: u.EffectKind, Payload: u.Payload, Meta: cloneMeta(u.Meta)}, nil
	}
	payload, err := json.Marshal(eff)
	if err != nil {
		return EffectEnvelope{}, fmt.Errorf("crucible/state: marshal effect %q: %w", eff.Kind(), err)
	}
	return EffectEnvelope{Kind: eff.Kind(), Payload: payload}, nil
}

// EffectFactory builds a fresh, zero-valued concrete effect for a kind. The
// registry unmarshals an envelope's payload into the value the factory returns,
// so a factory returns a pointer to a concrete effect type for json.Unmarshal to
// populate. Built-in factories are pre-registered; a host registers its own
// effect kinds through RegisterEffect.
type EffectFactory func() Effect

// EffectRegistry maps effect kinds to factories for envelope deserialization. It
// is the output-half counterpart to the host registry on the input half: the
// built-in effect kinds are pre-registered, and a host adds its own through the
// RegisterEffect functional option. Deserializing a kind the registry does not
// know does not fail — the envelope is preserved as an UnknownEffect — but such
// an effect is not Dispatchable, realizing the preserve-on-load,
// reject-on-dispatch closed-enum extension policy.
type EffectRegistry struct {
	factories map[string]EffectFactory
}

// RegisterEffectOption configures a NewEffectRegistry call. New deserialization
// knobs arrive as new options, never as a signature change.
type RegisterEffectOption func(*EffectRegistry)

// RegisterEffect registers a factory for an effect kind so the envelope decoder
// can route that kind back to a concrete effect. A later registration for the
// same kind overrides an earlier one (and overrides a built-in), letting a host
// swap a decoder while the kernel's pre-registration stays the default.
func RegisterEffect(kind string, factory EffectFactory) RegisterEffectOption {
	return func(r *EffectRegistry) { r.factories[kind] = factory }
}

// NewEffectRegistry returns an EffectRegistry with every built-in effect kind
// pre-registered, then applies the supplied options (host effect kinds) in
// order. Options registering a built-in kind override the pre-registration.
func NewEffectRegistry(opts ...RegisterEffectOption) *EffectRegistry {
	r := &EffectRegistry{factories: make(map[string]EffectFactory, len(builtinEffectFactories))}
	for kind, factory := range builtinEffectFactories {
		r.factories[kind] = factory
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// builtinEffectFactories pre-registers a factory for every kernel-emitted effect
// kind, so a default EffectRegistry round-trips all built-ins out of the box.
var builtinEffectFactories = map[string]EffectFactory{
	EffectKindSpawnActor:      func() Effect { return &SpawnActor{} },
	EffectKindStopActor:       func() Effect { return &StopActor{} },
	EffectKindStartService:    func() Effect { return &StartService{} },
	EffectKindStopService:     func() Effect { return &StopService{} },
	EffectKindScheduleAfter:   func() Effect { return &ScheduleAfter{} },
	EffectKindCancelScheduled: func() Effect { return &CancelScheduled{} },
	EffectKindSendTo:          func() Effect { return &SendTo{} },
	EffectKindSendParent:      func() Effect { return &SendParent{} },
	EffectKindRespondToSender: func() Effect { return &RespondToSender{} },
	EffectKindForwardEvent:    func() Effect { return &ForwardEvent{} },
}

// Unmarshal decodes an EffectEnvelope into a concrete effect. A recognized kind
// is built by its registered factory and populated from the payload; an
// unrecognized kind is preserved verbatim as an UnknownEffect rather than
// dropped or rejected — the reject happens later at Dispatchable. The returned
// value implements KindedEffect.
func (r *EffectRegistry) Unmarshal(env EffectEnvelope) (Effect, error) {
	factory, ok := r.factories[env.Kind]
	if !ok {
		return UnknownEffect{EffectKind: env.Kind, Payload: env.Payload, Meta: cloneMeta(env.Meta)}, nil
	}
	eff := factory()
	if len(env.Payload) > 0 {
		if err := json.Unmarshal(env.Payload, eff); err != nil {
			return nil, fmt.Errorf("crucible/state: unmarshal effect %q: %w", env.Kind, err)
		}
	}
	return eff, nil
}

// Dispatchable reports whether an effect may be applied by a host. A nil result
// means the effect carries a kind the registry recognizes (or is not kinded at
// all — a bare domain effect the kernel never gated). An UnknownEffect, or any
// KindedEffect whose kind the registry does not know, is rejected with a typed
// *ErrUnknownEffectKind, completing the preserve-on-load, reject-on-dispatch
// policy: a foreign effect is never silently applied.
func (r *EffectRegistry) Dispatchable(eff Effect) error {
	ke, ok := eff.(KindedEffect)
	if !ok {
		// A bare, unkinded domain effect was never part of the closed enum; the
		// kernel does not gate it. The host's own dispatch decides what to do.
		return nil
	}
	if _, known := r.factories[ke.Kind()]; !known {
		return &ErrUnknownEffectKind{Kind: ke.Kind()}
	}
	return nil
}
