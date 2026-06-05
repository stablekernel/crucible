# crucible/transport

A **gRPC network transport** for the [Crucible](../README.md)
[`cluster`](../cluster) runtime: it carries actor deliver, spawn, and read-only
time-travel operations between nodes over real gRPC (HTTP/2).

> **Status:** experimental, pre-1.0. The transport is exercised over an in-process
> gRPC connection (bufconn) and the API may still change before v1.

Import path: `github.com/stablekernel/crucible/transport`

## What it is

`cluster` spreads a `state` machine and its child-machine actors across nodes over
a pluggable `Transport`; its `InMemoryTransport` connects node-scoped systems in
one process. This module is the over-the-network implementation: a `Transport`
satisfies `cluster.Transport` on the client side and a server serves a node's
`cluster.WireEndpoint`, so a parent on one host can spawn and drive an actor
running on another.

It lives in its **own module** so the gRPC dependency stays out of the cluster
core, which remains stdlib-only: a deployment that uses only the in-memory
transport never compiles gRPC in.

## How it works

Payloads (events and spawn inputs) cross the wire as the JSON the `WireEndpoint`
seam already produces and consumes, through a JSON gRPC codec. There is **no
protobuf schema or codegen**: the service descriptor is hand-written and both sides
force the JSON codec explicitly (`grpc.ForceCodec` on the client, `ForceServerCodec`
on the server), so the codec is never resolved through the global gRPC encoding
registry and the import has no process-wide side effect.

## Client

```go
tr := transport.New()
tr.AddNode("node-b", conn) // conn is a *grpc.ClientConn the caller dials and owns

nodeA := cluster.NewSystem("node-a", actorSysA, cluster.WithTransport(tr))
ref, err := nodeA.Spawn(ctx, "node-b", "worker", "w-1", nil) // spawn on node-b over gRPC
delivered, err := nodeA.Deliver(ctx, ref, "finish")          // route to its owning node
```

A `Transport` holds one client connection per reachable node, registered with
`AddNode`; the caller dials each node (`grpc.NewClient`) and owns the connection's
lifecycle. An operation addressed to a node that was never registered reports
`cluster.ErrNodeUnreachable`.

## Server

```go
gs := transport.NewServer(nodeBWireEndpoint) // gRPC server with the JSON codec + service registered
go gs.Serve(lis)
```

`NewServer` builds a gRPC server preconfigured with the JSON codec and the node's
transport service registered; the caller serves it on a listener and owns its
lifecycle. Pass extra `grpc.ServerOption`s (interceptors, credentials, keepalives)
as needed, or register the service onto an existing server with `RegisterServer`.

## Time travel

For durable instances, `NewDurableTimeTravel` adapts a `durable.Store` and machine
into a `TimeTravelEndpoint`; `NewTimeTravelServer` / `RegisterTimeTravel` serve it,
and the client `Transport.StateAt` reconstructs an instance's past state as of a
recorded step over the wire. It is **read-only**: it runs no service, re-instantiates
no actor, and dispatches no effect.
