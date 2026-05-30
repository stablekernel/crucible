package telemetry

// This file provides the functional-option wiring a consuming module embeds so
// every Crucible IO module exposes telemetry injection the same way. A module
// holds a Provider in its config, seeds it with Nop() (the silent default), and
// exposes WithTracer/WithMeter options built from the helpers here. The shape is
// consistent with the suite's functional-options convention: a zero-option call
// is silent, and telemetry arrives as additive options.

// Provider bundles a Tracer and a Meter. A consuming module keeps one in its
// config; the zero value is not ready for use — seed it with Nop and apply
// options. Provider always holds non-nil interfaces after Nop, so call sites
// never need nil checks.
type Provider struct {
	Tracer Tracer
	Meter  Meter
}

// Nop returns a Provider wired to the no-op Tracer and Meter. Use it as a
// module config's default so unconfigured telemetry is silent and allocation-
// free.
func Nop() Provider {
	return Provider{Tracer: NopTracer(), Meter: NopMeter()}
}

// Option mutates a Provider. Modules can re-export these directly, or wrap them
// in their own option type when their config holds more than telemetry.
type Option func(*Provider)

// WithTracer sets the Provider's Tracer. A nil tracer is ignored, preserving the
// no-op default rather than introducing a nil that call sites would have to
// guard.
func WithTracer(t Tracer) Option {
	return func(p *Provider) {
		if t != nil {
			p.Tracer = t
		}
	}
}

// WithMeter sets the Provider's Meter. A nil meter is ignored, preserving the
// no-op default.
func WithMeter(m Meter) Option {
	return func(p *Provider) {
		if m != nil {
			p.Meter = m
		}
	}
}

// Apply returns a copy of p with opts applied. It never mutates the receiver, so
// a module can keep an immutable default Provider and derive per-construction
// copies from it.
func (p Provider) Apply(opts ...Option) Provider {
	out := p
	if out.Tracer == nil {
		out.Tracer = NopTracer()
	}
	if out.Meter == nil {
		out.Meter = NopMeter()
	}
	for _, opt := range opts {
		opt(&out)
	}
	return out
}
