// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"log/slog"

	"github.com/stablekernel/crucible/telemetry"
)

// config holds a Manifold's resolved seams. Every field has a no-op default so a
// zero-option Manifold is fully functional and silent.
type config struct {
	logger  *slog.Logger
	tracer  telemetry.Tracer
	meter   telemetry.Meter
	outlets []Outlet
}

// defaultConfig returns the no-op defaults: a discarding logger, the no-op
// tracer, and the no-op meter. None allocate a backend or perform IO.
func defaultConfig() config {
	return config{
		logger: slog.New(slog.DiscardHandler),
		tracer: telemetry.NopTracer(),
		meter:  telemetry.NopMeter(),
	}
}

// Option configures a Manifold. Options are additive and have no-op defaults; a
// nil value passed to a With* option is ignored, leaving the default in place.
type Option func(*config)

// WithLogger sets the structured logger the Manifold writes outlet failures to.
// The default discards all records. A nil logger is ignored.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithTracer sets the tracer the Manifold starts emit spans on. The default is
// telemetry.NopTracer(). A nil tracer is ignored.
func WithTracer(t telemetry.Tracer) Option {
	return func(c *config) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithMeter sets the meter the Manifold records its counters on. The default is
// telemetry.NopMeter(). A nil meter is ignored.
func WithMeter(m telemetry.Meter) Option {
	return func(c *config) {
		if m != nil {
			c.meter = m
		}
	}
}

// WithOutlets attaches outlets at construction time, equivalent to a subsequent
// Attach call. It is additive across repeated uses.
func WithOutlets(outlets ...Outlet) Option {
	return func(c *config) { c.outlets = append(c.outlets, outlets...) }
}
