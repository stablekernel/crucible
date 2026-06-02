// SPDX-License-Identifier: Apache-2.0

// Package sink is a fire-and-forget fan-out emitter: a service calls one
// [Manifold.Sink] and the payload fans out to every attached [Outlet] —
// SQL, DynamoDB, StatsD, a webhook, a log — without the call site knowing
// which destinations are wired.
//
// # Manifold and Outlets
//
// A [Manifold] holds a set of [Outlet]s and fans each payload out to all of
// them. [Manifold.Sink] is the only emit path and returns nothing: outlet
// failures are routed to the configured logger and the sink.failed metric, not
// back to the caller. A caller that needs confirmation for one critical
// destination holds that [Outlet] directly and calls its Sink, which returns an
// honest per-destination error. A Manifold is therefore not itself an Outlet;
// nest one in another with [OutletFunc].
//
// # Op and Registry
//
// A destination is usually an [Emitter], which binds a typed client C to a
// [Registry] of [Op] values keyed by payload type. [Register] maps a concrete
// payload type to a transformer that builds the [Op] persisting it; an
// unregistered payload yields [ErrUnregistered], which the Manifold treats as a
// silent skip. There is no package-global registry — every Registry is
// constructed and injected.
//
// # Reservoir and Poller
//
// [Reservoir] wraps an Outlet to buffer payloads and release them in batches, by
// size or on an interval, using an injected clock. [Poller] periodically samples
// state through a [CollectFunc] and sinks the results. Both take their clock as
// an option so tests are deterministic and sleep-free.
//
// # Observability
//
// The Manifold's seams are first-class and have no-op defaults: a discarding
// [log/slog.Logger], the no-op tracer and meter from
// [github.com/stablekernel/crucible/telemetry]. With real seams wired, each Sink
// starts a "sink.Sink" span (whose context is propagated to every Outlet, so a
// downstream span nests beneath it) and records the sink.sunk, sink.failed,
// sink.skipped, and sink.dropped counters plus the sink.batch_size and
// sink.flush_latency_ms histograms.
//
// # Stability
//
// Experimental (pre-v1). The API is feature-complete and intended to become
// v1.0.0 after cross-module review; until then it may change.
package sink
