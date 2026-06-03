// SPDX-License-Identifier: Apache-2.0

package source

import "context"

// SubscribeConfig is the backend-neutral subscription request an [Inlet]
// resolves into a live [Subscription]. Backend-specific tuning (Kafka balancer,
// JetStream ack-wait, pull batch sizes) is supplied through the concrete inlet's
// own functional options at construction time, never here — this struct carries
// only what every backend understands.
type SubscribeConfig struct {
	// Topics are the topics (Kafka) or subjects (JetStream) to consume. At least
	// one is required.
	Topics []string
	// Group is the consumer group (Kafka) or durable consumer name (JetStream)
	// for competing-consumer load balancing. Empty means a standalone/ephemeral
	// subscription that receives every message on its own.
	Group string
}

// Inlet is a per-backend ingress adapter: it opens subscriptions onto an
// external stream. It mirrors sink.Outlet — thin, vendor-specific, and not
// itself the consume engine. The [Hopper] drives whatever an Inlet returns.
//
// Concrete inlets (source/kafka, source/jetstream, source/memsource) live in
// their own modules so their vendor SDKs never enter this core's dependency
// graph. An inlet may also implement optional capability interfaces ([Seekable],
// [ConsumerGroups], …) that the Hopper detects by type assertion.
type Inlet interface {
	// Subscribe opens a Subscription for cfg. The returned Subscription is
	// driven by the Hopper; the caller closes it (or the Inlet) to drain.
	Subscribe(ctx context.Context, cfg SubscribeConfig) (Subscription, error)
	// Close releases the inlet's resources (connections, clients). It does not
	// settle in-flight messages; close live Subscriptions first.
	Close() error
}

// Subscription is a live stream of inbound messages from an [Inlet]. It is a
// thin pull-and-settle surface the [Hopper] drives: the engine owns the consume
// loop, concurrency, decoding, and the middleware chain, while the Subscription
// only fetches the next message and applies the engine's settle decision to the
// backend. This split keeps every adapter small and uniform — the hard parts
// (ordering, backpressure, retry) live once, in the Hopper.
//
// A Subscription is single-consumer: Next is called from one goroutine (the
// Hopper's fetch loop). Settle may be called concurrently from worker
// goroutines and must be safe for that.
type Subscription interface {
	// Next returns the next message. It blocks until one is available, returns
	// ctx.Err() if ctx is canceled, or returns ErrDrained once the subscription
	// has been closed and all delivered messages settled.
	Next(ctx context.Context) (Message, error)
	// Settle applies a handler [Result] to a message previously returned by Next:
	// ack/commit, schedule redelivery, route to dead-letter, or extend the
	// deadline, per Result.Action. It is the single point where a delivery
	// decision reaches the backend.
	Settle(ctx context.Context, m Message, r Result) error
	// Close begins a graceful drain: Next stops yielding new messages, and once
	// in-flight messages are settled, Next returns ErrDrained. Close is
	// idempotent.
	Close() error
}
