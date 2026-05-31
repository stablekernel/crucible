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

// Deliverer is a node's local delivery endpoint as a transport sees it: given a
// ref the node owns, it delivers an event to the addressed actor. A *System
// satisfies it, so registering a node with an InMemoryTransport is just handing
// it that node's System.
type Deliverer interface {
	Deliver(ctx context.Context, ref state.ActorRef, event any) (bool, error)
}

// InMemoryTransport routes deliveries between node-scoped Systems living in the
// same process, with no network involved. It is the reference Transport: it makes
// the multi-node actor model fully exercisable in tests and single-process
// development, and a real network transport implements the same Transport
// interface out of tree. It is safe for concurrent use.
type InMemoryTransport struct {
	mu    sync.RWMutex
	nodes map[string]Deliverer
}

// NewInMemoryTransport returns an empty transport. Register each node's System
// before routing to it.
func NewInMemoryTransport() *InMemoryTransport {
	return &InMemoryTransport{nodes: make(map[string]Deliverer)}
}

// Register wires node's local delivery endpoint into the transport, so a ref whose
// Node is node routes to d. Registering a node again replaces its endpoint.
func (t *InMemoryTransport) Register(node string, d Deliverer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[node] = d
}

// Deliver routes event to the node that owns ref and delegates to that node's
// local delivery. A ref naming an unregistered node returns ErrNodeUnreachable; a
// registered node that has no such actor returns (false, nil) — reached, but
// nothing to deliver to.
func (t *InMemoryTransport) Deliver(ctx context.Context, ref state.ActorRef, event any) (bool, error) {
	t.mu.RLock()
	d, ok := t.nodes[ref.Node]
	t.mu.RUnlock()
	if !ok {
		return false, fmt.Errorf("%w: %q", ErrNodeUnreachable, ref.Node)
	}
	return d.Deliver(ctx, ref, event)
}
