// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/stablekernel/crucible/telemetry"
)

// Manifold fans a payload out to every attached Outlet, fire-and-forget. Sink is
// the only emit path and returns nothing: outlet failures are routed to the
// logger and the sink.failed counter, not back to the caller. A caller that
// needs per-destination confirmation holds that outlet directly and calls its
// Sink (see OutletFunc). The zero value is unusable; construct with NewManifold.
//
// A Manifold is safe for concurrent Sink, Attach, Flush, and Shutdown.
type Manifold struct {
	logger *slog.Logger
	tracer telemetry.Tracer

	sunk    telemetry.Counter
	failed  telemetry.Counter
	skipped telemetry.Counter

	mu      sync.RWMutex
	outlets []Outlet
}

// NewManifold constructs a Manifold with the given options. With no options it
// is silent and untraced: a discarding logger, the no-op tracer, and the no-op
// meter.
func NewManifold(opts ...Option) *Manifold {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return &Manifold{
		logger:  cfg.logger,
		tracer:  cfg.tracer,
		sunk:    cfg.meter.Counter("sink.sunk", telemetry.WithDescription("payloads sunk to an outlet without error")),
		failed:  cfg.meter.Counter("sink.failed", telemetry.WithDescription("outlet failures observed during fan-out")),
		skipped: cfg.meter.Counter("sink.skipped", telemetry.WithDescription("outlets that skipped a payload as unregistered")),
		outlets: append([]Outlet(nil), cfg.outlets...),
	}
}

// Attach adds outlets to the Manifold and returns it for chaining. It is safe
// for concurrent use. Attach replaces the outlet slice with a freshly allocated
// one (copy-on-write) rather than appending in place, so a concurrent Sink that
// is iterating an earlier snapshot never observes a mutated backing array.
func (m *Manifold) Attach(outlets ...Outlet) *Manifold {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := make([]Outlet, 0, len(m.outlets)+len(outlets))
	next = append(next, m.outlets...)
	next = append(next, outlets...)
	m.outlets = next
	return m
}

// snapshot returns the current outlet slice for one operation. Because Attach
// is copy-on-write the returned slice is never mutated in place, so callers may
// read it without copying; the RLock only guards reading the slice header.
func (m *Manifold) snapshot() []Outlet {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.outlets
}

// Sink fans payload out to every attached outlet, fire-and-forget. It starts a
// "sink.Sink" span and propagates the returned context to each Outlet.Sink, so a
// downstream span (an outlet's own, or another module's) nests under the emit
// span. A nil-skip (ErrUnregistered) is counted as skipped; any other error is
// logged at ERROR, recorded on the span, and counted as failed. Success is
// counted as sunk.
func (m *Manifold) Sink(ctx context.Context, payload any) {
	pt := typeName(payload)
	ctx, span := m.tracer.Start(ctx, "sink.Sink", telemetry.String("payload.type", pt))
	defer span.End()

	var failures int
	for _, o := range m.snapshot() {
		switch err := o.Sink(ctx, payload); {
		case err == nil:
			m.sunk.Add(ctx, 1)
		case errors.Is(err, ErrUnregistered):
			m.skipped.Add(ctx, 1)
		default:
			failures++
			m.failed.Add(ctx, 1)
			span.RecordError(err)
			m.logger.ErrorContext(ctx, "sink: outlet failed",
				slog.String("payload_type", pt),
				slog.Any("error", err))
		}
	}
	if failures > 0 {
		span.SetStatus(telemetry.StatusError, "one or more outlets failed")
	}
}

// Flush forces every attached Flusher to emit its buffered payloads. Errors from
// all flushers are joined and returned.
func (m *Manifold) Flush(ctx context.Context) error {
	var errs []error
	for _, o := range m.snapshot() {
		if f, ok := o.(Flusher); ok {
			if err := f.Flush(ctx); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// Shutdown flushes, then shuts down every attached Shutdowner, draining
// in-flight work within ctx's deadline. Errors from the flush and from each
// shutdowner are joined and returned.
func (m *Manifold) Shutdown(ctx context.Context) error {
	errs := []error{m.Flush(ctx)}
	for _, o := range m.snapshot() {
		if s, ok := o.(Shutdowner); ok {
			if err := s.Shutdown(ctx); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// Close implements io.Closer by calling Shutdown with a background context.
func (m *Manifold) Close() error { return m.Shutdown(context.Background()) }

var _ io.Closer = (*Manifold)(nil)
