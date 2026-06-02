// SPDX-License-Identifier: Apache-2.0

// Package nats is a sink destination that publishes payloads to a NATS subject.
// It wraps [github.com/nats-io/nats.go] behind a narrow [Client] interface so
// the real [*nats.Conn] satisfies it structurally while tests use hand-rolled
// fakes with no live server.
//
// Register a transformer per payload type that returns a [Publish] or
// [PublishMsg] operation, then attach the result of [New] to a sink.Manifold.
//
//	reg := nats.NewRegistry()
//	sink.Register(reg, func(_ context.Context, o OrderPlaced) sink.Op[nats.Client] {
//	    return nats.Publish("orders.placed", []byte(o.ID))
//	})
//
//	nc, _ := gonats.Connect(gonats.DefaultURL)
//	m := sink.NewManifold()
//	m.Attach(nats.New(nc, reg))
//	m.Sink(ctx, OrderPlaced{ID: "A-1"})
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package nats

import (
	"context"

	gonats "github.com/nats-io/nats.go"
	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow NATS surface this destination requires. It is satisfied
// structurally by [*gonats.Conn], so no type assertion or wrapper is needed.
type Client interface {
	// Publish synchronously publishes data to the given subject.
	Publish(subject string, data []byte) error
	// PublishMsg publishes a pre-built [*gonats.Msg].
	PublishMsg(m *gonats.Msg) error
}

// Publish returns an Op that publishes data to subject. The Op's context
// argument is accepted by the Op signature but NATS Publish is a synchronous,
// non-blocking call that carries no context; the call completes or returns an
// error immediately.
func Publish(subject string, data []byte) csink.Op[Client] {
	return csink.OpFunc[Client](func(_ context.Context, c Client) error {
		return c.Publish(subject, data)
	})
}

// PublishMsg returns an Op that publishes the pre-built msg to its subject. Use
// this when you need to set NATS headers or reply subjects.
func PublishMsg(msg *gonats.Msg) csink.Op[Client] {
	return csink.OpFunc[Client](func(_ context.Context, c Client) error {
		return c.PublishMsg(msg)
	})
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to
// client. The outlet is named "nats" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("nats")}, opts...)...)
}
