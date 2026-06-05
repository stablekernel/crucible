---
title: What is crucible/transport
description: A gRPC network transport for the cluster runtime, carrying actor deliver and spawn between nodes and serving read-only durable time-travel queries, all over a JSON codec with no protobuf schema.
sidebar:
  order: 1
---

<!-- IMAGE-SLOT: transport-overview-wire (a sky-squid smith carrying a molten casting across a wire bridge between two anvils, with a ghostly past-state casting queried alongside; ember/copper on steel) 16:9 -->

`crucible/transport` is the **gRPC network transport** for the
[`cluster`](/crucible/cluster/overview/) runtime. It carries actor deliver and
spawn operations between nodes over real gRPC (HTTP/2) and serves read-only
historical state reconstruction for durable instances. It implements
`cluster.Transport` on the client side and serves a node's `cluster.WireEndpoint`
on the server side.

It lives in its own module so the gRPC dependency stays out of the cluster core,
which remains stdlib-only: a deployment that uses only the in-memory transport
never compiles gRPC in. Payloads (events and spawn inputs) cross the wire as the
JSON the `WireEndpoint` seam already produces and consumes, through a JSON gRPC
codec. No protobuf schema or codegen is involved; the service descriptors are
hand-written and the messages are encoded as JSON.

## Serving a node

`NewServer` builds a gRPC server preconfigured with the JSON codec and a node's
`cluster.WireEndpoint` registered, so deliveries and spawns arriving over the wire
are decoded into the node's concrete event and input types and applied to its
local actor system. The caller serves it on a listener and owns its lifecycle, and
can pass extra `grpc.ServerOption`s (interceptors, credentials, keepalives):

```go
gs := transport.NewServer(node) // node satisfies cluster.WireEndpoint
go gs.Serve(listener)
defer gs.GracefulStop()
```

`RegisterServer` registers the same service onto an existing `grpc.Server` when
you already run one.

## Reaching other nodes

`New` returns a client `Transport` with no nodes; register each reachable node's
client connection with `AddNode`. The caller dials the node (`grpc.NewClient`) and
owns the connection's lifecycle. The resulting `*Transport` satisfies
`cluster.Transport`, so hand it to a `cluster.System` and remote refs route over
gRPC transparently:

```go
tr := transport.New()
tr.AddNode("node-b", connB) // connB is a grpc.ClientConnInterface

sys := cluster.NewSystem("node-a", actorSys, cluster.WithTransport(tr))
// A ref owned by node-b now delivers and spawns over the wire.
```

`Deliver` JSON-encodes the event and routes it to the owning node, which decodes
it into its concrete event type; `Spawn` asks a node to start an actor and returns
a ref to it. A ref whose node has no registered connection reports
`cluster.ErrNodeUnreachable`.

## Remote time-travel

Transport also exposes the [`durable`](/crucible/durable/overview/) time-travel
reader over the wire, so one node can reconstruct the past state of an instance
another node hosts. `DurableTimeTravel` adapts a durable `Store` and machine into a
`TimeTravelEndpoint` by running `durable.StateAt` and marshaling the reconstructed
snapshot. It is read-only: it runs no service, re-instantiates no actor, and
dispatches no effect.

```go
// On the host node: serve the time-travel endpoint.
tt := transport.NewDurableTimeTravel(machine, store)
gs := transport.NewTimeTravelServer(tt)
go gs.Serve(ttListener)

// On a remote node: ask for an instance's state as of a recorded step.
snapshot, err := tr.StateAt(ctx, "node-b", "order-42", 3)
inst, err := state.UnmarshalSnapshot[Gate, Signal, Turnstile](snapshot)
```

`NewTimeTravelServer` (or `RegisterTimeTravel` onto an existing server) hosts the
endpoint; the client `StateAt` reuses the transport's registered connections and
returns the marshaled snapshot, which the caller decodes with
`state.UnmarshalSnapshot` for its own `(S, E, C)`. For arbitrary steps the host's
`Store` should retain full history (a `durable.HistoryStore`); otherwise
reconstruction is limited to the latest checkpoint and tail.
