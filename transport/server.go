package transport

import (
	"context"

	"github.com/stablekernel/crucible/cluster"
	"google.golang.org/grpc"
)

// transportServer is the server contract the gRPC service dispatches to. The
// endpoint adapter below implements it over a cluster.WireEndpoint.
type transportServer interface {
	Deliver(context.Context, *DeliverRequest) (*DeliverResponse, error)
	Spawn(context.Context, *SpawnRequest) (*SpawnResponse, error)
}

// endpointServer adapts a node's cluster.WireEndpoint to the gRPC service: each RPC
// decodes on this node into its concrete event/input via the WireEndpoint.
type endpointServer struct {
	ep cluster.WireEndpoint
}

func (s *endpointServer) Deliver(ctx context.Context, req *DeliverRequest) (*DeliverResponse, error) {
	delivered, err := s.ep.DeliverWire(ctx, req.Ref, req.Event)
	if err != nil {
		return nil, err
	}
	return &DeliverResponse{Delivered: delivered}, nil
}

func (s *endpointServer) Spawn(ctx context.Context, req *SpawnRequest) (*SpawnResponse, error) {
	ref, err := s.ep.SpawnWire(ctx, req.Src, req.ID, req.Input)
	if err != nil {
		return nil, err
	}
	return &SpawnResponse{Ref: ref}, nil
}

// serviceDesc is the hand-written gRPC service descriptor: two unary methods, no
// protobuf schema. Messages are encoded by the JSON codec.
var serviceDesc = grpc.ServiceDesc{
	ServiceName: serviceName,
	HandlerType: (*transportServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Deliver", Handler: deliverHandler},
		{MethodName: "Spawn", Handler: spawnHandler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "crucible/transport",
}

// deliverHandler matches grpc's methodHandler signature exactly (srv, ctx, dec,
// interceptor); the parameter order is fixed by google.golang.org/grpc.
//
//nolint:revive // context-as-argument: grpc.methodHandler mandates this signature
func deliverHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(DeliverRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(transportServer).Deliver(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodDeliver}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(transportServer).Deliver(ctx, req.(*DeliverRequest))
	}
	return interceptor(ctx, in, info, handler)
}

//nolint:revive // context-as-argument: grpc.methodHandler mandates this signature
func spawnHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SpawnRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(transportServer).Spawn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: methodSpawn}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(transportServer).Spawn(ctx, req.(*SpawnRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RegisterServer registers a node's cluster.WireEndpoint as the transport service
// on an existing gRPC server, so deliveries and spawns arriving over the wire are
// decoded into the node's concrete types and applied to its local actor system.
func RegisterServer(gs grpc.ServiceRegistrar, ep cluster.WireEndpoint) {
	gs.RegisterService(&serviceDesc, &endpointServer{ep: ep})
}

// NewServer builds a gRPC server preconfigured with the JSON codec and the node's
// transport service registered. The caller serves it on a listener and owns its
// lifecycle. Pass extra ServerOptions (interceptors, credentials, keepalives) as
// needed.
func NewServer(ep cluster.WireEndpoint, opts ...grpc.ServerOption) *grpc.Server {
	gs := grpc.NewServer(append([]grpc.ServerOption{grpc.ForceServerCodec(jsonCodec{})}, opts...)...)
	RegisterServer(gs, ep)
	return gs
}
