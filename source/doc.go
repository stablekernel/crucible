// SPDX-License-Identifier: Apache-2.0

// Package source is Crucible's ingress seam: the symmetric counterpart to sink.
// Where sink fans an emitted effect out to many destinations, source funnels
// inbound messages from external streams into the suite, with delivery
// semantics (ack/nak/term, ordering, backpressure) that egress does not need.
//
// # Shape
//
// The themed surface is exactly two names, mirroring sink one-for-one:
//
//   - [Inlet] is a per-backend adapter (mirrors sink.Outlet). It opens a
//     [Subscription] that yields messages and settles them. Concrete inlets
//     (Kafka, JetStream, the in-memory test inlet) live in their own modules so
//     vendor SDKs never enter this core's dependency graph.
//   - [Hopper] is the consume engine (mirrors sink.Manifold). It drives a
//     Subscription with bounded, per-key-ordered concurrency, decodes payloads,
//     runs the middleware chain, invokes the [Handler], and settles each message
//     according to the [Result] the handler returns.
//
// Everything else is literal: [Message], [Handler], [Result], [Subscription],
// [Cursor], and the codec, retry, dead-letter, and replay concepts.
//
// # Contract
//
// Delivery is at-least-once within a live subscription: a message is acked only
// after its handler reports success, never before processing, and a [Nak]
// redelivers — JetStream naks natively (with optional delay), Kafka pauses and
// re-seeks the record's partition so it is fetched again. Across process
// restarts and rebalances, backends whose redelivery rides persisted offsets
// (Kafka) resume from the last committed position, which a concurrently
// committed higher offset can advance past a nacked record; those backends
// therefore redeliver exactly within a session and best-effort across restarts,
// and each adapter documents the precise semantics in its own module. A handler
// returns a [Result] — [Ack], [Nak], [Term], [InProgress], or [Manual] — and
// the Hopper applies it to the backend. Backends differ (Kafka commits offsets
// per partition; JetStream acks per message), so capabilities a backend may or
// may not have — replay, consumer groups, transactions — are optional
// interfaces discovered by type assertion ([Seekable], [ConsumerGroups],
// [Transactional], …) rather than a lowest-common-denominator API that lies
// about what a backend can do.
//
// # No forced dependencies
//
// The core is stdlib-only. Logging is *slog.Logger (no-op by default), tracing
// and metrics go through the vendor-neutral telemetry interface (no-op by
// default), and no vendor type appears in any public signature. A zero-option
// Hopper is fully functional and silent.
package source
