// SPDX-License-Identifier: Apache-2.0

// Package bridge composes a crucible/state machine with a crucible/sink Manifold
// without either core importing the other. It adapts state's two non-required
// observation seams to a sink fan-out:
//
//   - [Middleware] wraps a machine's Fire so every successful transition fans
//     out through a Manifold. Because Fire carries a context.Context, the
//     middleware starts a "state.transition" span and propagates its context
//     into Manifold.Sink, so the "sink.Sink" span (and each outlet's downstream
//     span) nests under the transition span through the shared crucible/telemetry
//     tracer. This is the context-propagating, trace-correlating path.
//   - [Inspector] adapts a Manifold to state's Inspector observer. It is the
//     ergonomic one-liner for "fan every transition out", but state.Inspector
//     carries no context.Context, so it cannot propagate trace context; use
//     Middleware when span nesting matters.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package bridge

import (
	"context"
	"fmt"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/telemetry"
)

// Transition is the payload the bridge fans out for each observed state
// transition. Register a transformer for it on each destination's registry to
// persist or publish transitions.
type Transition struct {
	// Machine is the name of the machine that transitioned.
	Machine string
	// Event is the human-readable label of the event that drove the transition.
	Event string
	// From is the primary active leaf before the transition.
	From string
	// To is the primary active leaf after the transition.
	To string
}

// Option configures the bridge adapters.
type Option func(*config)

type config struct {
	tracer   telemetry.Tracer
	spanName string
}

func newConfig(opts ...Option) config {
	cfg := config{tracer: telemetry.NopTracer(), spanName: "state.transition"}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithTracer sets the tracer the Middleware starts the transition span on. The
// default is telemetry.NopTracer(). Wire the same tracer the Manifold uses so
// the emit span nests under the transition span. A nil tracer is ignored.
func WithTracer(t telemetry.Tracer) Option {
	return func(c *config) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithSpanName overrides the transition span name (default "state.transition").
func WithSpanName(name string) Option {
	return func(c *config) {
		if name != "" {
			c.spanName = name
		}
	}
}

// Middleware returns a state.Middleware that fans every successful transition
// out through m. It starts a transition span on the configured tracer and
// propagates that span's context into m.Sink, so the emit span nests under the
// transition span. Install it with the machine builder's Use method.
func Middleware[S comparable, E comparable, C any](m *csink.Manifold, opts ...Option) state.Middleware[S, E, C] {
	cfg := newConfig(opts...)
	return func(next state.FireFunc[S, E, C]) state.FireFunc[S, E, C] {
		return func(ctx context.Context, event E) state.FireResult[S] {
			ctx, span := cfg.tracer.Start(ctx, cfg.spanName)
			defer span.End()

			res := next(ctx, event)
			if res.Err != nil {
				span.RecordError(res.Err)
				span.SetStatus(telemetry.StatusError, "transition failed")
				return res
			}
			span.SetStatus(telemetry.StatusOK, "")
			m.Sink(ctx, Transition{
				Machine: res.Trace.Machine,
				Event:   res.Trace.Event,
				From:    res.Trace.FromState,
				To:      fmt.Sprint(res.NewState),
			})
			return res
		}
	}
}

// Inspector adapts m to a state.Inspector, fanning each transition event out
// through m. It uses context.Background because state.Inspector carries no
// context, so emit spans do not nest under a transition span; use Middleware for
// trace correlation. Register it with the WithInspector cast option.
func Inspector(m *csink.Manifold) state.InspectorFunc {
	return func(ev state.InspectionEvent) {
		if ev.Kind != state.InspectTransition {
			return
		}
		m.Sink(context.Background(), Transition{
			Machine: ev.Machine,
			Event:   ev.Event,
			From:    ev.From,
			To:      ev.To,
		})
	}
}
