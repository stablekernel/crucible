// Package slog implements the Crucible telemetry interfaces on top of the
// standard library's log/slog. It proves the telemetry seam end to end using
// zero external dependencies: spans and metric instruments are emitted as
// structured log records.
//
// Import path: github.com/stablekernel/crucible/telemetry/slog
//
// It is intended for development, tests, and environments where structured logs
// are the only observability sink. For production tracing/metrics backends, use
// (or write) an otel or datadog adapter against the same interfaces.
//
// # Emission shape
//
//   - Tracer.Start logs at debug with msg "span.start" and a "span" group
//     carrying name, attributes, and a generated span id. The returned context
//     carries the span id so nested spans log a "span.parent" id, reproducing
//     span parentage in the logs.
//   - Span.End logs "span.end" with name, id, status, and elapsed duration.
//   - Span.RecordError logs at error with "span.error".
//   - Counter/Histogram/Gauge log at debug with msg "metric" and a "metric"
//     group carrying name, kind, value, unit, and attributes.
//
// All durations are sourced from an injectable clock so emission is
// deterministic in tests.
package slog

import (
	"context"
	sl "log/slog"
	"sync/atomic"
	"time"

	"github.com/stablekernel/crucible/telemetry"
)

// config holds resolved Option state.
type config struct {
	logger *sl.Logger
	now    func() time.Time
	nextID func() uint64
}

// Option configures the adapter.
type Option func(*config)

// WithLogger sets the slog.Logger the adapter emits to. The default discards all
// records (slog.New(slog.DiscardHandler)), keeping the adapter silent until a
// real logger is supplied.
func WithLogger(l *sl.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithClock sets the time source used for span durations. The default is
// time.Now. Injecting a clock makes emitted durations deterministic in tests.
func WithClock(now func() time.Time) Option {
	return func(c *config) {
		if now != nil {
			c.now = now
		}
	}
}

// WithIDFn sets the monotonic span-id generator. The default is an internal
// atomic counter starting at 1. Injecting it makes emitted span ids
// deterministic in tests.
func WithIDFn(next func() uint64) Option {
	return func(c *config) {
		if next != nil {
			c.nextID = next
		}
	}
}

func resolve(opts ...Option) config {
	ctr := new(atomic.Uint64)
	c := config{
		logger: sl.New(sl.DiscardHandler),
		now:    time.Now,
		nextID: func() uint64 { return ctr.Add(1) },
	}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// spanIDKey is the context key under which the current span id is stored, so a
// child span can log its parent id.
type spanIDKey struct{}

// Tracer is a telemetry.Tracer backed by slog.
type Tracer struct {
	cfg config
}

// NewTracer returns a slog-backed Tracer.
func NewTracer(opts ...Option) *Tracer { return &Tracer{cfg: resolve(opts...)} }

// Start logs "span.start" and returns a context carrying the new span's id.
func (t *Tracer) Start(ctx context.Context, name string, attrs ...telemetry.Attr) (context.Context, telemetry.Span) {
	id := t.cfg.nextID()
	parent, hasParent := ctx.Value(spanIDKey{}).(uint64)

	logAttrs := []any{sl.String("name", name), sl.Uint64("id", id)}
	if hasParent {
		logAttrs = append(logAttrs, sl.Uint64("parent", parent))
	}
	logAttrs = append(logAttrs, attrArgs(attrs)...)
	t.cfg.logger.LogAttrs(ctx, sl.LevelDebug, "span.start", sl.Group("span", logAttrs...))

	s := &span{
		cfg:   t.cfg,
		name:  name,
		id:    id,
		start: t.cfg.now(),
	}
	return context.WithValue(ctx, spanIDKey{}, id), s
}

// span is a telemetry.Span backed by slog.
type span struct {
	cfg       config
	name      string
	id        uint64
	start     time.Time
	status    telemetry.StatusCode
	statusMsg string
	ended     bool
}

func (s *span) SetAttributes(attrs ...telemetry.Attr) {
	if s.ended {
		return
	}
	args := append([]any{sl.String("name", s.name), sl.Uint64("id", s.id)}, attrArgs(attrs)...)
	s.cfg.logger.LogAttrs(context.Background(), sl.LevelDebug, "span.attributes", sl.Group("span", args...))
}

func (s *span) RecordError(err error) {
	if s.ended || err == nil {
		return
	}
	s.cfg.logger.LogAttrs(context.Background(), sl.LevelError, "span.error",
		sl.Group("span",
			sl.String("name", s.name),
			sl.Uint64("id", s.id),
			sl.String("error", err.Error()),
		),
	)
}

func (s *span) SetStatus(code telemetry.StatusCode, msg string) {
	if s.ended {
		return
	}
	s.status = code
	s.statusMsg = msg
}

func (s *span) End() {
	if s.ended {
		return
	}
	s.ended = true
	args := []any{
		sl.String("name", s.name),
		sl.Uint64("id", s.id),
		sl.String("status", statusString(s.status)),
		sl.Duration("elapsed", s.cfg.now().Sub(s.start)),
	}
	if s.statusMsg != "" {
		args = append(args, sl.String("status_msg", s.statusMsg))
	}
	s.cfg.logger.LogAttrs(context.Background(), sl.LevelDebug, "span.end", sl.Group("span", args...))
}

// Meter is a telemetry.Meter backed by slog.
type Meter struct {
	logger *sl.Logger
}

// NewMeter returns a slog-backed Meter.
func NewMeter(opts ...Option) *Meter { return &Meter{logger: resolve(opts...).logger} }

// Counter returns a counter instrument that logs a "metric" record per Add.
func (m *Meter) Counter(name string, opts ...telemetry.InstrumentOption) telemetry.Counter {
	return &instrument{logger: m.logger, name: name, kind: "counter", cfg: telemetry.ResolveInstrument(opts...)}
}

// Histogram returns a histogram instrument that logs a "metric" record per Record.
func (m *Meter) Histogram(name string, opts ...telemetry.InstrumentOption) telemetry.Histogram {
	return &instrument{logger: m.logger, name: name, kind: "histogram", cfg: telemetry.ResolveInstrument(opts...)}
}

// Gauge returns a gauge instrument that logs a "metric" record per Record.
func (m *Meter) Gauge(name string, opts ...telemetry.InstrumentOption) telemetry.Gauge {
	return &instrument{logger: m.logger, name: name, kind: "gauge", cfg: telemetry.ResolveInstrument(opts...)}
}

// instrument backs Counter, Histogram, and Gauge: each logs a "metric" record.
type instrument struct {
	logger *sl.Logger
	name   string
	kind   string
	cfg    telemetry.InstrumentConfig
}

func (i *instrument) Add(ctx context.Context, n int64, attrs ...telemetry.Attr) {
	i.emit(ctx, sl.Int64("value", n), attrs)
}

func (i *instrument) Record(ctx context.Context, v float64, attrs ...telemetry.Attr) {
	i.emit(ctx, sl.Float64("value", v), attrs)
}

func (i *instrument) emit(ctx context.Context, value sl.Attr, attrs []telemetry.Attr) {
	args := []any{sl.String("name", i.name), sl.String("kind", i.kind), value}
	if i.cfg.Unit != "" {
		args = append(args, sl.String("unit", i.cfg.Unit))
	}
	if i.cfg.Description != "" {
		args = append(args, sl.String("description", i.cfg.Description))
	}
	args = append(args, attrArgs(attrs)...)
	i.logger.LogAttrs(ctx, sl.LevelDebug, "metric", sl.Group("metric", args...))
}

// attrArgs nests the telemetry attributes under a single "attrs" group so their
// keys never collide with the span/metric fields. Because telemetry.Attr is an
// alias for slog.Attr, the attributes pass straight through with no conversion —
// scalar values are never re-boxed.
func attrArgs(attrs []telemetry.Attr) []any {
	if len(attrs) == 0 {
		return nil
	}
	inner := make([]any, len(attrs))
	for i, a := range attrs {
		inner[i] = a
	}
	return []any{sl.Group("attrs", inner...)}
}

func statusString(c telemetry.StatusCode) string {
	switch c {
	case telemetry.StatusOK:
		return "ok"
	case telemetry.StatusError:
		return "error"
	default:
		return "unset"
	}
}

// Compile-time assertions.
var (
	_ telemetry.Tracer    = (*Tracer)(nil)
	_ telemetry.Span      = (*span)(nil)
	_ telemetry.Meter     = (*Meter)(nil)
	_ telemetry.Counter   = (*instrument)(nil)
	_ telemetry.Histogram = (*instrument)(nil)
	_ telemetry.Gauge     = (*instrument)(nil)
)
