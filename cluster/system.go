package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// ErrNoTransport is returned when a delivery targets a ref owned by another node
// but the System has no Transport configured to reach it. A System without a
// Transport still serves its local actors; it simply cannot reach remote ones.
var ErrNoTransport = errors.New("cluster: ref names a remote node but no transport is configured")

// Transport carries actor operations to the node that owns the target actor. It is
// the host-supplied seam that keeps the kernel and this package's core free of any
// network dependency: an in-memory transport drives tests, and a real network
// transport implements the same interface.
type Transport interface {
	// Deliver routes event to the actor named by ref on ref's owning node and
	// reports whether the actor accepted it. The node identifier to dial is
	// ref.Node, which the implementation resolves to a concrete address. An error
	// reports a transport-level failure (the node was unreachable, the wire call
	// failed); a nil error with delivered=false means the node was reached but had
	// no such running actor.
	Deliver(ctx context.Context, ref state.ActorRef, event any) (delivered bool, err error)
	// Spawn asks node to start an actor with the given id from the behavior its
	// local system registered under src, passing input, and returns a ref to it
	// (its Node set to node). An error reports a transport-level failure reaching
	// node or a spawn that did not start on the far side.
	Spawn(ctx context.Context, node, src, id string, input map[string]any) (state.ActorRef, error)
}

// System is the distributed actor system for one node: a local
// state.ActorSystem wrapped with a node identity and an optional Transport. A
// holder delivers to an ActorRef without caring where the actor runs — the System
// delegates to the local ActorSystem when it owns the actor and routes over the
// Transport when another node does.
type System[S comparable, E comparable, C any] struct {
	node      string
	local     *state.ActorSystem[S, E, C]
	transport Transport
}

// Option configures a System. New capabilities arrive as additional options, so
// the constructor signature never breaks.
type Option func(*config)

type config struct {
	transport Transport
}

// WithTransport supplies the Transport a System uses to reach actors on other
// nodes. Without it, a System serves only local actors and reports ErrNoTransport
// for a remote ref.
func WithTransport(t Transport) Option {
	return func(c *config) { c.transport = t }
}

// NewSystem wraps local into the distributed system for node. The node identifier
// is the value a remote ref carries in ActorRef.Node to address actors this
// System owns; it must be unique across the cluster. The local ActorSystem keeps
// driving this node's actors exactly as before.
func NewSystem[S comparable, E comparable, C any](node string, local *state.ActorSystem[S, E, C], opts ...Option) *System[S, E, C] {
	var c config
	for _, opt := range opts {
		opt(&c)
	}
	return &System[S, E, C]{node: node, local: local, transport: c.transport}
}

// Node returns this system's node identifier.
func (s *System[S, E, C]) Node() string { return s.node }

// Local returns the wrapped in-process ActorSystem, the escape hatch for the
// kernel-level driver operations (Absorb, Step, Register, snapshots) the System
// does not itself surface.
func (s *System[S, E, C]) Local() *state.ActorSystem[S, E, C] { return s.local }

// owns reports whether ref names an actor this node holds: an empty Node is the
// in-process projection (a ref minted by the local ActorSystem), and a Node equal
// to this node's identifier is an explicit local address.
func (s *System[S, E, C]) owns(ref state.ActorRef) bool {
	return ref.Node == "" || ref.Node == s.node
}

// Deliver routes event to the actor named by ref. When this node owns the actor
// the event is delivered straight through the local ActorSystem; otherwise it is
// routed over the Transport to the owning node. It reports whether the actor
// accepted the event, and an error only for a transport-level failure reaching a
// remote node (ErrNoTransport when no transport is configured).
func (s *System[S, E, C]) Deliver(ctx context.Context, ref state.ActorRef, event any) (bool, error) {
	if s.owns(ref) {
		return s.local.Deliver(ctx, ref, event), nil
	}
	if s.transport == nil {
		return false, ErrNoTransport
	}
	return s.transport.Deliver(ctx, ref, event)
}

// DeliverByID delivers to a local actor by its registry id. It addresses only this
// node's actors; use Deliver with a remote ref to reach another node.
func (s *System[S, E, C]) DeliverByID(ctx context.Context, id string, event any) bool {
	return s.local.DeliverByID(ctx, id, event)
}

// Spawn starts an actor with the given id from the behavior registered under src,
// on node. When node is this node (or empty) the actor is spawned in the local
// ActorSystem; otherwise the spawn is routed over the Transport to node. It
// returns a ref to the new actor with its Node set so the caller can address it
// wherever it runs. A remote spawn with no Transport configured returns
// ErrNoTransport.
func (s *System[S, E, C]) Spawn(ctx context.Context, node, src, id string, input map[string]any) (state.ActorRef, error) {
	if node == "" || node == s.node {
		return s.SpawnLocal(ctx, src, id, input)
	}
	if s.transport == nil {
		return state.ActorRef{}, ErrNoTransport
	}
	return s.transport.Spawn(ctx, node, src, id, input)
}

// SpawnLocal starts an actor with the given id from the behavior the local
// ActorSystem registered under src, passing input, and returns a ref to it stamped
// with this node so it is addressable from other nodes. It is the local half of
// Spawn and the operation a Transport invokes on the owning node. An error reports
// that the spawn did not start (for example, no behavior is registered under src).
func (s *System[S, E, C]) SpawnLocal(ctx context.Context, src, id string, input map[string]any) (state.ActorRef, error) {
	s.local.Absorb(ctx, []state.Effect{state.SpawnActor{ID: id, Src: state.Ref{Name: src}, Input: input}})
	ref, ok := s.local.Ref(id)
	if !ok {
		return state.ActorRef{}, fmt.Errorf("cluster: spawn of %q from %q on node %q did not start", id, src, s.node)
	}
	ref.Node = s.node
	return ref, nil
}

// Respawn replaces the actor registered under id with a fresh instance from src:
// it first tears down any existing actor with that id (a failed actor stays
// registered as done until removed), then spawns anew. It is the primitive a
// supervisor's Restart decision drives, satisfying Respawner. Stopping a missing
// id is a no-op, so Respawn also works as a plain spawn.
func (s *System[S, E, C]) Respawn(ctx context.Context, src, id string, input map[string]any) (state.ActorRef, error) {
	s.local.Stop(state.ActorRef{ID: id})
	return s.SpawnLocal(ctx, src, id, input)
}

// Ref resolves a local actor id to its ref, reporting whether this node runs it.
func (s *System[S, E, C]) Ref(id string) (state.ActorRef, bool) {
	return s.local.Ref(id)
}

// Running reports how many actors this node currently runs.
func (s *System[S, E, C]) Running() int { return s.local.Running() }

// Stop tears down a local actor (and its children); stopping a ref this node does
// not own is a no-op, since teardown of a remote actor is the owning node's job.
func (s *System[S, E, C]) Stop(ref state.ActorRef) {
	if s.owns(ref) {
		s.local.Stop(ref)
	}
}
