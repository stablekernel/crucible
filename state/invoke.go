package state

import "encoding/json"

// This file defines the invoked-services (`invoke`) contract: the declarative
// shape an invoked service takes on a state, the effects the kernel emits so a
// host runtime can run the service, and the entry/exit effect emission that
// drives invoked services. The kernel itself never runs a service, never
// starts a goroutine, and never performs IO — Fire stays pure. Entering a state
// that declares an `invoke` emits a StartService effect per invocation; exiting
// that state before the service completes emits a StopService effect per
// invocation (auto-stop-on-exit). A host's ServiceRunner consumes
// these effects, runs the registered service, and feeds its result back through
// the state's onDone / onError transitions via Fire.
//
// Scope: this is service invocation — a host-run unit of work (a promise-style
// one-shot or a streaming callback) bound by name to a host registry, exactly
// parallel to how guards and actions bind. Child-machine actors (invoking
// another Machine as a sub-actor with parent/child messaging) are NOT in scope
// here; they arrive with the actor model, whose reserved hook is the Instance
// mailbox. The kernel only needs the service-invocation contract; the specific
// logic creators (promise one-shot, streaming callback) are host-side wrappers
// over ServiceFn.

// Invocation is a declarative invoked service on a state.
// On entering the owning state the kernel emits a StartService effect carrying
// Src and Input; the host runs the bound service and re-fires OnDone with the
// result or OnError with the error back through Fire. On exiting the state
// before the service completes, the kernel emits a StopService effect so the
// host stops the in-flight service (auto-stop-on-exit). The whole struct
// serializes, so an invoke block round-trips losslessly through JSON.
type Invocation[S comparable, E comparable, C any] struct {
	// ID identifies this invocation for the lifetime of the owning state's
	// activation. It is stable per (machine, owning state, invoke index), so the
	// StartService emitted on entry and the StopService emitted on exit pair up,
	// and a host keys its running-service table by ID. When omitted in the DSL it
	// defaults to the derived InvokeID.
	ID string `json:"id,omitempty"`
	// Src is the named reference (plus serializable params) to the host-provided
	// service implementation, bound from the service registry at Provide/Quench
	// time exactly like a guard or action ref. An unbound Src fails Quench with
	// the typed *UnboundRefError (Kind "service").
	Src Ref `json:"src"`
	// Input is the serializable input passed to the service when it starts,
	// surfaced on the StartService effect as input. It is data only;
	// the kernel never inspects it.
	Input map[string]any `json:"input,omitempty"`
	// OnDone is the event the host re-fires through Fire when the service
	// completes successfully; the service result rides along as the StartService
	// host contract's done payload. It routes the result through an ordinary
	// transition keyed on this event from the owning state.
	OnDone E `json:"onDone"`
	// OnError is the event the host re-fires through Fire when the service fails;
	// the error rides along as the host contract's error payload. It routes the
	// failure through an ordinary transition keyed on this event from the owning
	// state.
	OnError E `json:"onError"`

	// Kind tags this invocation as a host-run service (the default,
	// ActorKindService) or a child-MACHINE actor (ActorKindMachine). A service
	// invocation emits StartService / StopService and is driven by a ServiceRunner;
	// an actor invocation emits SpawnActor / StopActor and is driven by an
	// ActorSystem that runs the child machine as an actor and routes its done/error
	// back through the parent. The field serializes, so the distinction round-trips
	// losslessly through JSON.
	Kind ActorKind `json:"kind,omitempty"`
	// SystemID is the optional system-scoped name a child-machine actor registers
	// under in the ActorSystem (its systemId), so a sibling can address it
	// by a well-known name. It is meaningful only for an ActorKindMachine
	// invocation and serializes for lossless round-trip.
	SystemID string `json:"systemId,omitempty"`

	// Meta is the reserved extension namespace at invocation granularity:
	// documentation, tags, and codegen hints live here, mirroring the Meta map
	// every other IR node carries. The kernel never inspects it; it round-trips
	// verbatim.
	Meta map[string]any `json:"meta,omitempty"`

	// extra preserves unknown JSON keys a newer producer emitted so they survive a
	// load -> save cycle (forward-compat). Never inspected by the kernel.
	extra map[string]json.RawMessage
}

// invocationKnownKeys is the set of JSON keys Invocation models; anything else is
// captured into extra and preserved verbatim on round-trip.
var invocationKnownKeys = map[string]struct{}{
	"id": {}, "src": {}, "input": {}, "onDone": {}, "onError": {},
	"kind": {}, "systemId": {}, "meta": {},
}

// MarshalJSON encodes an Invocation, merging its preserved unknown keys back in
// with stable key ordering.
func (inv Invocation[S, E, C]) MarshalJSON() ([]byte, error) {
	type alias Invocation[S, E, C]
	return marshalWithExtra(alias(inv), inv.extra)
}

// UnmarshalJSON decodes an Invocation and captures any unknown keys into extra so
// they survive re-serialization.
func (inv *Invocation[S, E, C]) UnmarshalJSON(data []byte) error {
	type alias Invocation[S, E, C]
	var a alias
	extra, err := captureExtra(data, &a, invocationKnownKeys)
	if err != nil {
		return err
	}
	*inv = Invocation[S, E, C](a)
	inv.extra = extra
	return nil
}

// StartService is the effect the kernel emits when an instance enters a state
// that declares an invoked service. The host is expected to run the service
// named by Src with Input and, on completion, re-fire OnDone with the result
// through Fire, or on failure re-fire OnError with the error. ID is stable per
// (instance, owning state, invoke index), so a later StopService with the same
// ID stops exactly this service.
//
// The kernel never runs the service itself: it emits this as data alongside the
// transition's other effects, keeping Fire pure (no goroutine, no IO).
type StartService struct {
	// ID identifies the running service. It is stable across the start/stop pair
	// for one owning state on one instance, so a host keys its service table by ID.
	ID string `json:"id"`
	// Src is the service ref (name + params) the host resolves against its service
	// registry to obtain the implementation to run.
	Src Ref `json:"src"`
	// Input is the serializable input passed to the service at start.
	Input map[string]any `json:"input,omitempty"`
	// OnDone is the event the host re-fires (with the service result) when the
	// service completes successfully, type-erased for the abstract effect surface;
	// a host driver built with NewServiceRunner keeps it typed.
	OnDone any `json:"onDone,omitempty"`
	// OnError is the event the host re-fires (with the error) when the service
	// fails, type-erased for the abstract effect surface.
	OnError any `json:"onError,omitempty"`
	// State names the owning state whose entry started this service, for
	// diagnostics and host bookkeeping.
	State string `json:"state,omitempty"`
}

// StopService is the effect the kernel emits when an instance exits a state that
// had an in-flight invoked service. The host stops the service registered under
// ID; stopping an unknown ID is a no-op. A
// state's invoked services are auto-stopped when the state is exited before they
// complete.
type StopService struct {
	// ID identifies the service to stop. It matches the ID of the StartService
	// that began it (auto-stop-on-exit).
	ID string `json:"id"`
}

// invokeID builds the stable per-instance identifier for the invocation at index
// idx on owning state `from`. The same (machine, from, idx) always yields the
// same ID within a process, so the start emitted on entry and the stop emitted on
// exit pair up without per-instance bookkeeping in the kernel.
func invokeID[S comparable](machine string, from S, idx int) string {
	return machine + ":" + fmtState(from) + ":invoke:" + itoa(idx)
}

// InvokeID returns the stable identifier the kernel assigns to the invoked
// service at index idx on owning state `from` of machine `machine` when the
// invocation declares no explicit ID. A host or test uses it to correlate a
// StartService with a later StopService, or to assert which service a
// StopService targets.
func InvokeID[S comparable](machine string, from S, idx int) string {
	return invokeID(machine, from, idx)
}

// invokeEffectsOnEntry returns the StartService effects for every invoked service
// declared on the entered states, in entry order. It reads no clock, runs no
// service, and performs no IO: it only translates declared invocations into start
// effects for the host to act on.
func (i *Instance[S, E, C]) invokeEffectsOnEntry(entries []S, tr *Trace) []Effect {
	var out []Effect
	m := i.machine
	for _, s := range entries {
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		for ix := range n.state.Invoke {
			inv := &n.state.Invoke[ix]
			if inv.Kind == ActorKindMachine {
				continue
			}
			id := invocationID(m.name, s, ix, inv)
			out = append(out, StartService{
				ID:      id,
				Src:     inv.Src,
				Input:   inv.Input,
				OnDone:  inv.OnDone,
				OnError: inv.OnError,
				State:   fmtState(s),
			})
			tr.note("service.start." + id)
		}
	}
	return out
}

// invokeEffectsOnExit returns the StopService effects for every invoked service
// declared on the exited states, in exit order. Emitting a stop for a service
// that may already have completed is safe: the host treats an unknown ID as a
// no-op; this is auto-stop-on-exit.
func (i *Instance[S, E, C]) invokeEffectsOnExit(exits []S, tr *Trace) []Effect {
	var out []Effect
	m := i.machine
	for _, s := range exits {
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		for ix := range n.state.Invoke {
			inv := &n.state.Invoke[ix]
			if inv.Kind == ActorKindMachine {
				continue
			}
			id := invocationID(m.name, s, ix, inv)
			out = append(out, StopService{ID: id})
			tr.note("service.stop." + id)
		}
	}
	return out
}

// StartEffects returns the StartService effects for the invoked services declared
// on the instance's initial active configuration, so a host can arm the services
// of the state(s) entered at Cast — the entry that Fire never observes because no
// event drove it. Call it once, right after Cast, and route the effects through
// the same ServiceRunner used for Fire's effects. It is a pure read of the
// configuration and emits no IO, consistent with the kernel's effects-as-data
// contract. A flat or single-spine instance reports its single starting state's
// services; a parallel initial configuration reports every active region's.
func (i *Instance[S, E, C]) StartEffects() []Effect {
	var tr Trace
	cfg := i.Configuration()
	out := i.invokeEffectsOnEntry(cfg, &tr)
	out = append(out, i.actorEffectsOnEntry(cfg, &tr)...)
	return out
}

// invocationID resolves the identifier for an invocation: its explicit ID when
// the author supplied one, else the derived stable ID for its position.
func invocationID[S comparable, E comparable, C any](machine string, from S, idx int, inv *Invocation[S, E, C]) string {
	if inv.ID != "" {
		return inv.ID
	}
	return invokeID(machine, from, idx)
}

// itoa renders a small non-negative int without importing strconv, keeping this
// file dependency-light. Indices are always small and non-negative.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
