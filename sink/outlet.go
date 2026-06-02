// SPDX-License-Identifier: Apache-2.0

package sink

import "context"

// Outlet is a single destination a payload can be sunk to. Sink returns
// ErrUnregistered when the outlet has no transformer for the payload's concrete
// type — a normal, silent skip the Manifold does not count as a failure. Any
// other error is a real failure the Manifold logs and meters; a caller that
// holds an outlet directly may also inspect it for per-destination confirmation.
//
// Implementations must be safe for concurrent use: a Manifold fans a single
// payload out to every attached Outlet from the calling goroutine, and the same
// outlet may be shared across manifolds.
type Outlet interface {
	Sink(ctx context.Context, payload any) error
}

// OutletFunc adapts a plain function to an Outlet. It is also the escape hatch
// for nesting one Manifold inside another, since a Manifold is intentionally not
// itself an Outlet (its Sink is fire-and-forget and returns nothing):
//
//	parent.Attach(sink.OutletFunc(func(ctx context.Context, p any) error {
//		child.Sink(ctx, p)
//		return nil
//	}))
type OutletFunc func(ctx context.Context, payload any) error

// Sink calls the underlying function.
func (f OutletFunc) Sink(ctx context.Context, payload any) error { return f(ctx, payload) }

// Optional capabilities an Outlet may also implement. The Manifold detects each
// by type assertion at flush/shutdown time; an outlet that implements none is
// driven through Sink alone.
type (
	// Flusher is an Outlet that buffers and can be forced to emit. Manifold.Flush
	// calls Flush on every attached Flusher.
	Flusher interface {
		Flush(ctx context.Context) error
	}
	// BatchOutlet is an Outlet that can accept many payloads in one call. The
	// Reservoir uses it on flush when the wrapped outlet supports it, falling back
	// to a Sink loop otherwise.
	BatchOutlet interface {
		SinkBatch(ctx context.Context, payloads []any) error
	}
	// Shutdowner is an Outlet that holds resources (a background flush loop, a
	// connection) to release. Manifold.Shutdown calls Shutdown on every attached
	// Shutdowner after flushing, draining in-flight work within ctx's deadline.
	Shutdowner interface {
		Shutdown(ctx context.Context) error
	}
)
