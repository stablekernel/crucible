// Package transport is a gRPC network transport for the Crucible cluster runtime.
// It carries actor deliver and spawn operations between nodes over real gRPC
// (HTTP/2), implementing cluster.Transport on the client side and serving a node's
// cluster.WireEndpoint on the server side.
//
// It lives in its own module so the gRPC dependency stays out of the cluster core,
// which remains stdlib-only: a deployment that uses only the in-memory transport
// never compiles gRPC in. Payloads (events and spawn inputs) cross the wire as the
// JSON the WireEndpoint seam already produces and consumes, via a JSON gRPC codec —
// no protobuf schema or codegen is involved.
package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

const (
	serviceName   = "crucible.transport.v1.Transport"
	methodDeliver = "/" + serviceName + "/Deliver"
	methodSpawn   = "/" + serviceName + "/Spawn"
	jsonCodecName = "crucible-json"
)

// jsonCodec is a gRPC content codec that encodes messages as JSON, so the
// transport carries the same JSON the cluster WireEndpoint produces without a
// protobuf schema.
type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return jsonCodecName }

func init() { encoding.RegisterCodec(jsonCodec{}) }

// DeliverRequest carries a delivery to the actor named by Ref. Event is the
// JSON-encoded event the owning node decodes into its event type.
type DeliverRequest struct {
	Ref   state.ActorRef `json:"ref"`
	Event []byte         `json:"event,omitempty"`
}

// DeliverResponse reports whether the addressed actor accepted the event.
type DeliverResponse struct {
	Delivered bool `json:"delivered"`
}

// SpawnRequest asks the owning node to start an actor with ID from Src, with the
// JSON-encoded Input.
type SpawnRequest struct {
	Src   string `json:"src"`
	ID    string `json:"id"`
	Input []byte `json:"input,omitempty"`
}

// SpawnResponse returns a ref to the spawned actor.
type SpawnResponse struct {
	Ref state.ActorRef `json:"ref"`
}

// Transport is the client side: it routes cluster operations to the gRPC server of
// the node that owns the target actor. It satisfies cluster.Transport. Register
// each reachable node's client connection with AddNode. It is safe for concurrent
// use.
type Transport struct {
	mu    sync.RWMutex
	conns map[string]grpc.ClientConnInterface
}

// New returns a Transport with no nodes; register reachable nodes with AddNode.
func New() *Transport {
	return &Transport{conns: make(map[string]grpc.ClientConnInterface)}
}

// AddNode registers the client connection used to reach node. The caller dials the
// node (grpc.NewClient) and owns the connection's lifecycle. Registering a node
// again replaces its connection.
func (t *Transport) AddNode(node string, conn grpc.ClientConnInterface) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.conns[node] = conn
}

func (t *Transport) conn(node string) (grpc.ClientConnInterface, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	c, ok := t.conns[node]
	return c, ok
}

// Deliver routes event to the actor named by ref on its owning node over gRPC. The
// event is JSON-encoded here and decoded into the owning node's event type there.
func (t *Transport) Deliver(ctx context.Context, ref state.ActorRef, event any) (bool, error) {
	conn, ok := t.conn(ref.Node)
	if !ok {
		return false, fmt.Errorf("%w: %q", cluster.ErrNodeUnreachable, ref.Node)
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("transport: marshal event for %q: %w", ref.ID, err)
	}
	var resp DeliverResponse
	if err := conn.Invoke(ctx, methodDeliver, &DeliverRequest{Ref: ref, Event: eventJSON}, &resp, grpc.ForceCodec(jsonCodec{})); err != nil {
		return false, fmt.Errorf("transport: deliver to node %q: %w", ref.Node, err)
	}
	return resp.Delivered, nil
}

// Spawn asks node to start an actor with id from src, passing input, over gRPC, and
// returns a ref to it.
func (t *Transport) Spawn(ctx context.Context, node, src, id string, input map[string]any) (state.ActorRef, error) {
	conn, ok := t.conn(node)
	if !ok {
		return state.ActorRef{}, fmt.Errorf("%w: %q", cluster.ErrNodeUnreachable, node)
	}
	var inputJSON []byte
	if input != nil {
		var err error
		if inputJSON, err = json.Marshal(input); err != nil {
			return state.ActorRef{}, fmt.Errorf("transport: marshal input for %q: %w", id, err)
		}
	}
	var resp SpawnResponse
	if err := conn.Invoke(ctx, methodSpawn, &SpawnRequest{Src: src, ID: id, Input: inputJSON}, &resp, grpc.ForceCodec(jsonCodec{})); err != nil {
		return state.ActorRef{}, fmt.Errorf("transport: spawn on node %q: %w", node, err)
	}
	return resp.Ref, nil
}
