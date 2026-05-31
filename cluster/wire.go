package cluster

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// WireEndpoint is a node's receive side as a network transport sees it: it accepts
// an operation whose payload arrived as JSON over the wire and applies it to the
// node's local actor system, decoding the payload into the node's own concrete
// types. A *System satisfies it, so a network transport serving a node just holds
// that node's System behind this type-erased interface.
//
// It is the network counterpart to the in-process Endpoint: where InMemoryTransport
// passes Go values between same-process systems unchanged, a network transport
// serializes the event/input on the sending node and hands the bytes to the owning
// node's WireEndpoint, which decodes them into its event type E (or input map) and
// delivers locally. Decoding on the owning node is what lets the transport stay
// type-erased while the kernel keeps its concrete, typed events.
type WireEndpoint interface {
	// DeliverWire decodes eventJSON into the node's event type and delivers it to
	// the local actor named by ref, reporting whether the actor accepted it.
	DeliverWire(ctx context.Context, ref state.ActorRef, eventJSON []byte) (bool, error)
	// SpawnWire decodes inputJSON into the spawn input map and spawns an actor with
	// the given id from src in the local system, returning a ref to it.
	SpawnWire(ctx context.Context, src, id string, inputJSON []byte) (state.ActorRef, error)
}

// DeliverWire decodes a JSON-encoded event into this system's event type E and
// delivers it to the local actor named by ref. It is the receive half a network
// transport calls on the actor's owning node; the sending node produced eventJSON
// with json.Marshal of the original event. An empty or null payload decodes to E's
// zero value.
func (s *System[S, E, C]) DeliverWire(ctx context.Context, ref state.ActorRef, eventJSON []byte) (bool, error) {
	var event E
	if len(eventJSON) > 0 {
		if err := json.Unmarshal(eventJSON, &event); err != nil {
			return false, fmt.Errorf("cluster: decode wire event for %q: %w", ref.ID, err)
		}
	}
	return s.local.Deliver(ctx, ref, event), nil
}

// SpawnWire decodes a JSON-encoded input map and spawns an actor with the given id
// from src in this node's local system, returning a ref stamped with this node. It
// is the receive half of a network transport's Spawn. An empty payload spawns with
// a nil input.
func (s *System[S, E, C]) SpawnWire(ctx context.Context, src, id string, inputJSON []byte) (state.ActorRef, error) {
	var input map[string]any
	if len(inputJSON) > 0 {
		if err := json.Unmarshal(inputJSON, &input); err != nil {
			return state.ActorRef{}, fmt.Errorf("cluster: decode wire input for %q: %w", id, err)
		}
	}
	return s.SpawnLocal(ctx, src, id, input)
}
