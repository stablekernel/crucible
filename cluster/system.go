package cluster

import (
	"context"
	"errors"

	"github.com/stablekernel/crucible/state"
)

// ErrNoTransport is returned when a delivery targets a ref owned by another node
// but the System has no Transport configured to reach it. A System without a
// Transport still serves its local actors; it simply cannot reach remote ones.
var ErrNoTransport = errors.New("cluster: ref names a remote node but no transport is configured")

// Transport moves an actor delivery to the node that owns the target actor. It is
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
