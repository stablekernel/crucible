package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/stablekernel/crucible/state"
)

// ErrNodeUnreachable is returned when a delivery targets a node the transport
// cannot reach: for the in-memory transport, a node that was never registered.
var ErrNodeUnreachable = errors.New("cluster: target node is unreachable")

// Endpoint is a node's local endpoint as a transport sees it: it delivers events
// to, and spawns actors in, that node's local ActorSystem. A *System satisfies it,
// so registering a node with an InMemoryTransport is just handing it that node's
// System.
type Endpoint interface {
	Deliver(ctx context.Context, ref state.ActorRef, event any) (bool, error)
	SpawnLocal(ctx context.Context, src, id string, input map[string]any) (state.ActorRef, error)
}

// InMemoryTransport routes deliveries between node-scoped Systems living in the
// same process, with no network involved. It is the reference Transport: it makes
// the multi-node actor model fully exercisable in tests and single-process
// development, and a real network transport implements the same Transport
// interface out of tree. It is safe for concurrent use.
type InMemoryTransport struct {
	mu    sync.RWMutex
	nodes map[string]Endpoint
}

// NewInMemoryTransport returns an empty transport. Register each node's System
// before routing to it.
func NewInMemoryTransport() *InMemoryTransport {
	return &InMemoryTransport{nodes: make(map[string]Endpoint)}
}

// Register wires node's local endpoint into the transport, so an operation
// targeting node routes to e. Registering a node again replaces its endpoint.
func (t *InMemoryTransport) Register(node string, e Endpoint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[node] = e
}

// endpoint resolves a node identifier to its registered endpoint.
func (t *InMemoryTransport) endpoint(node string) (Endpoint, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	e, ok := t.nodes[node]
	return e, ok
}

// Deliver routes event to the node that owns ref and delegates to that node's
// local delivery. A ref naming an unregistered node returns ErrNodeUnreachable; a
// registered node that has no such actor returns (false, nil) — reached, but
// nothing to deliver to.
func (t *InMemoryTransport) Deliver(ctx context.Context, ref state.ActorRef, event any) (bool, error) {
	e, ok := t.endpoint(ref.Node)
	if !ok {
		return false, fmt.Errorf("%w: %q", ErrNodeUnreachable, ref.Node)
	}
	return e.Deliver(ctx, ref, event)
}

// Spawn routes a spawn request to node's local endpoint, returning a ref to the
// new actor. A request for an unregistered node returns ErrNodeUnreachable.
func (t *InMemoryTransport) Spawn(ctx context.Context, node, src, id string, input map[string]any) (state.ActorRef, error) {
	e, ok := t.endpoint(node)
	if !ok {
		return state.ActorRef{}, fmt.Errorf("%w: %q", ErrNodeUnreachable, node)
	}
	return e.SpawnLocal(ctx, src, id, input)
}
