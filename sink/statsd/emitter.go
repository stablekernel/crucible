// SPDX-License-Identifier: Apache-2.0

package statsd

import (
	"context"
	"time"

	csink "github.com/stablekernel/crucible/sink"
)

// The Emitter path is the secondary surface for callers who want a direct
// payload-to-operation mapping with no in-process aggregation: every Sink emits
// one StatsD call. Use it when a destination already aggregates downstream (for
// example a sidecar agent) and double aggregation is undesirable. For folding
// counters and gauges in process, prefer the Aggregator from NewAggregator.

// Count returns an Op that emits a StatsD count.
func Count(name string, value int64, tags []string, rate float64) csink.Op[Client] {
	return csink.OpFunc[Client](func(_ context.Context, c Client) error {
		return c.Count(name, value, tags, rate)
	})
}

// Gauge returns an Op that emits a StatsD gauge.
func Gauge(name string, value float64, tags []string, rate float64) csink.Op[Client] {
	return csink.OpFunc[Client](func(_ context.Context, c Client) error {
		return c.Gauge(name, value, tags, rate)
	})
}

// Histogram returns an Op that emits a StatsD histogram sample.
func Histogram(name string, value float64, tags []string, rate float64) csink.Op[Client] {
	return csink.OpFunc[Client](func(_ context.Context, c Client) error {
		return c.Histogram(name, value, tags, rate)
	})
}

// Distribution returns an Op that emits a StatsD distribution sample.
func Distribution(name string, value float64, tags []string, rate float64) csink.Op[Client] {
	return csink.OpFunc[Client](func(_ context.Context, c Client) error {
		return c.Distribution(name, value, tags, rate)
	})
}

// Timing returns an Op that emits a StatsD timing sample.
func Timing(name string, value time.Duration, tags []string, rate float64) csink.Op[Client] {
	return csink.OpFunc[Client](func(_ context.Context, c Client) error {
		return c.Timing(name, value, tags, rate)
	})
}

// Set returns an Op that emits a StatsD set sample.
func Set(name string, value string, tags []string, rate float64) csink.Op[Client] {
	return csink.OpFunc[Client](func(_ context.Context, c Client) error {
		return c.Set(name, value, tags, rate)
	})
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register, for use with New.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to the
// client, emitting one StatsD call per Sink with no aggregation. The outlet is
// named "statsd" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("statsd")}, opts...)...)
}
