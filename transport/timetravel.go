package transport

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
	"google.golang.org/grpc"
)

const (
	ttServiceName = "crucible.transport.v1.TimeTravel"
	methodStateAt = "/" + ttServiceName + "/StateAt"
)

// StateAtRequest asks for an instance's reconstructed state as of a recorded step.
type StateAtRequest struct {
	ID   string `json:"id"`
	Step int    `json:"step"`
}

// StateAtResponse carries the reconstructed snapshot as marshaled JSON; the caller
// decodes it with state.UnmarshalSnapshot for its own (S, E, C).
type StateAtResponse struct {
	Snapshot []byte `json:"snapshot"`
}

// TimeTravelEndpoint serves read-only historical state reconstruction for durable
// instances a node hosts. DurableTimeTravel adapts a durable Store + machine into
// one; a node registers it with RegisterTimeTravel so other nodes can query an
// instance's past state over the wire.
type TimeTravelEndpoint interface {
	// StateAt reconstructs the instance's state as of step and returns it as a
	// marshaled kernel snapshot.
	StateAt(ctx context.Context, id string, step int) (snapshot []byte, err error)
}

// DurableTimeTravel adapts a durable Store and machine into a TimeTravelEndpoint by
// running durable.StateAt and marshaling the reconstructed snapshot. It is
// read-only: it runs no service, re-instantiates no actor, and dispatches no
// effect.
type DurableTimeTravel[S comparable, E comparable, C any] struct {
	machine *state.Machine[S, E, C]
	store   durable.Store
}

// NewDurableTimeTravel binds a machine and the durable Store its instances were
// recorded into. The Store should retain full history (a durable.HistoryStore) to
// reach arbitrary steps; otherwise reconstruction is limited to the latest
// checkpoint and tail.
func NewDurableTimeTravel[S comparable, E comparable, C any](m *state.Machine[S, E, C], store durable.Store) *DurableTimeTravel[S, E, C] {
	return &DurableTimeTravel[S, E, C]{machine: m, store: store}
}

// StateAt reconstructs the instance's state as of step and marshals it.
func (d *DurableTimeTravel[S, E, C]) StateAt(ctx context.Context, id string, step int) ([]byte, error) {
	view, err := durable.StateAt(ctx, d.machine, d.store, durable.InstanceID(id), step)
	if err != nil {
		return nil, err
	}
	return state.MarshalSnapshot(view.Snapshot())
}

type timeTravelServer interface {
	StateAt(context.Context, *StateAtRequest) (*StateAtResponse, error)
}

type endpointTimeTravelServer struct {
	ep TimeTravelEndpoint
}

func (s *endpointTimeTravelServer) StateAt(ctx context.Context, req *StateAtRequest) (*StateAtResponse, error) {
	snap, err := s.ep.StateAt(ctx, req.ID, req.Step)
	if err != nil {
		return nil, err
	}
	return &StateAtResponse{Snapshot: snap}, nil
}

var timeTravelServiceDesc = grpc.ServiceDesc{
	ServiceName: ttServiceName,
	HandlerType: (*timeTravelServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "StateAt", Handler: stateAtHandler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "crucible/transport",
}

//nolint:revive // context-as-argument: grpc.methodHandler mandates this signature
func stateAtHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(StateAtRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(timeTravelServer).StateAt(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodStateAt}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(timeTravelServer).StateAt(ctx, req.(*StateAtRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RegisterTimeTravel registers a node's TimeTravelEndpoint as the time-travel
// service on an existing gRPC server, so remote nodes can reconstruct the past
// state of instances this node hosts.
func RegisterTimeTravel(gs grpc.ServiceRegistrar, ep TimeTravelEndpoint) {
	gs.RegisterService(&timeTravelServiceDesc, &endpointTimeTravelServer{ep: ep})
}

// NewTimeTravelServer builds a gRPC server preconfigured with the JSON codec and
// the time-travel service registered. The caller serves it and owns its lifecycle.
func NewTimeTravelServer(ep TimeTravelEndpoint, opts ...grpc.ServerOption) *grpc.Server {
	gs := grpc.NewServer(append([]grpc.ServerOption{grpc.ForceServerCodec(jsonCodec{})}, opts...)...)
	RegisterTimeTravel(gs, ep)
	return gs
}

// StateAt asks node to reconstruct an instance's state as of step and returns the
// marshaled snapshot, which the caller decodes with state.UnmarshalSnapshot for its
// own (S, E, C). It reuses the transport's registered connections.
func (t *Transport) StateAt(ctx context.Context, node, id string, step int) ([]byte, error) {
	conn, ok := t.conn(node)
	if !ok {
		return nil, errNodeUnreachable(node)
	}
	var resp StateAtResponse
	if err := conn.Invoke(ctx, methodStateAt, &StateAtRequest{ID: id, Step: step}, &resp, grpc.ForceCodec(jsonCodec{})); err != nil {
		return nil, fmt.Errorf("transport: state-at on node %q: %w", node, err)
	}
	return resp.Snapshot, nil
}
