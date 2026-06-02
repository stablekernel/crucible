// SPDX-License-Identifier: Apache-2.0

// Package gcppubsub is a sink destination that publishes payloads to Google
// Cloud Pub/Sub. It wraps the Pub/Sub publisher behind a narrow [Publisher]
// interface so that unit tests work with hand-rolled fakes and no network or
// emulator is required. Register a transformer per payload type that returns a
// [Publish] Op, then pass the result of [New] to a sink.Manifold.
//
// The Pub/Sub client publishes asynchronously: the underlying publisher returns
// a result handle whose Get blocks for the server-assigned message ID. The
// [Adapt] helper bridges a real *pubsub.Publisher to the [Publisher] interface
// by calling Publish and then blocking on the handle, so each Op resolves to a
// single synchronous publish that surfaces the server-assigned ID or an error.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package gcppubsub

import (
	"context"

	"cloud.google.com/go/pubsub/v2"
	csink "github.com/stablekernel/crucible/sink"
)

// Publisher is the narrow Pub/Sub surface this destination needs. It publishes
// a single message and blocks for the server-assigned message ID. It is
// deliberately decoupled from the SDK so that tests use hand-rolled fakes; the
// real *pubsub.Publisher is bridged to this interface with [Adapt].
type Publisher interface {
	// Publish sends one message with the given data and attributes and returns
	// the server-assigned message ID once the publish completes.
	Publish(ctx context.Context, data []byte, attrs map[string]string) (string, error)
}

// adapter bridges a synchronous publish function to [Publisher]. The function
// is the only seam over the SDK, which keeps the bridge testable without a live
// topic or a server-assigned result handle.
type adapter struct {
	publish func(ctx context.Context, msg *pubsub.Message) (string, error)
}

// Publish builds a message from data and attrs and runs the bridged publish
// function, returning the server-assigned message ID once the publish
// completes.
func (a adapter) Publish(ctx context.Context, data []byte, attrs map[string]string) (string, error) {
	return a.publish(ctx, &pubsub.Message{Data: data, Attributes: attrs})
}

// Adapt wraps a *pubsub.Publisher as a [Publisher]. Publishing through the
// returned value calls the publisher's Publish and then blocks on the result
// handle's Get, so each publish is synchronous from the sink's perspective.
// Reuse a single publisher per topic and call its Stop method when done; Adapt
// does not own its lifecycle.
func Adapt(pub *pubsub.Publisher) Publisher {
	return adapter{publish: func(ctx context.Context, msg *pubsub.Message) (string, error) {
		return pub.Publish(ctx, msg).Get(ctx)
	}}
}

// Publish returns an Op that publishes one message with the given data and
// attributes. The Op blocks until the server assigns a message ID or the
// publish fails; the server-assigned ID is discarded and any error is returned
// for the Emitter to wrap. Pass a nil attrs map when no attributes are needed.
func Publish(data []byte, attrs map[string]string) csink.Op[Publisher] {
	return csink.OpFunc[Publisher](func(ctx context.Context, p Publisher) error {
		_, err := p.Publish(ctx, data, attrs)
		return err
	})
}

// NewRegistry returns an empty registry of Op[Publisher] for callers to
// populate with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Publisher]] {
	return csink.NewRegistry[csink.Op[Publisher]]()
}

// New builds an Outlet that applies each payload's registered Op[Publisher] to
// the publisher. The outlet is named "gcppubsub" unless overridden with
// sink.WithName.
func New(pub Publisher, reg *csink.Registry[csink.Op[Publisher]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Publisher](pub, reg, append([]csink.EmitterOption{csink.WithName("gcppubsub")}, opts...)...)
}
