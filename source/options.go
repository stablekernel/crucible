// SPDX-License-Identifier: Apache-2.0

package source

import (
	"log/slog"
	"time"

	"github.com/stablekernel/crucible/telemetry"
)

// config holds a Hopper's resolved seams. Every field has a no-op default so a
// zero-option Hopper is fully functional and silent.
type config struct {
	name        string
	logger      *slog.Logger
	tracer      telemetry.Tracer
	meter       telemetry.Meter
	registry    *Registry
	middleware  []Middleware
	concurrency int
	maxInFlight int

	// batch holds the batch-mode tuning; batch.enabled is false unless WithBatch
	// is supplied, in which case RunBatch/ReceiveBatch accumulate per lane.
	batch batchConfig
}

// batchConfig holds the resolved batch-mode seams. now is injected (mirroring
// sink.Reservoir) so max-wait flushing is deterministic in tests.
type batchConfig struct {
	enabled bool
	size    int
	maxWait time.Duration
	now     func() time.Time
}

// defaultConfig returns the no-op defaults: a discarding logger, the no-op
// tracer and meter, a single lane, and an unbounded in-flight window. None
// allocate a backend or perform IO. The codec registry defaults to nil; a Hopper
// with no registry passes the raw [Message] to the handler (which reads
// Value/As itself) rather than decoding.
func defaultConfig() config {
	return config{
		name:        "hopper",
		logger:      slog.New(slog.DiscardHandler),
		tracer:      telemetry.NopTracer(),
		meter:       telemetry.NopMeter(),
		concurrency: 1,
		maxInFlight: 0,
		batch: batchConfig{
			now: time.Now,
		},
	}
}

// Option configures a Hopper. Options are additive and have no-op defaults; a
// nil value passed to a With* option is ignored, leaving the default in place.
type Option func(*config)

// WithName sets the name the Hopper reports in logs and telemetry attributes.
// The default is "hopper". An empty name is ignored.
func WithName(name string) Option {
	return func(c *config) {
		if name != "" {
			c.name = name
		}
	}
}

// WithLogger sets the structured logger the Hopper writes processing failures
// to. The default discards all records. A nil logger is ignored.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithTracer sets the tracer the Hopper starts per-message spans on. The default
// is telemetry.NopTracer(). A nil tracer is ignored.
func WithTracer(t telemetry.Tracer) Option {
	return func(c *config) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithMeter sets the meter the Hopper records its counters and lag gauge on. The
// default is telemetry.NopMeter(). A nil meter is ignored.
func WithMeter(m telemetry.Meter) Option {
	return func(c *config) {
		if m != nil {
			c.meter = m
		}
	}
}

// WithCodec sets a single default [Codec] the Hopper decodes every message with,
// a shorthand for a registry holding only a default. It builds a fresh
// [Registry] with codec as its default; combine with [WithRegistry] only when
// you need content-type routing. A nil codec is ignored.
func WithCodec(codec Codec) Option {
	return func(c *config) {
		if codec != nil {
			c.registry = NewRegistry().SetDefault(codec)
		}
	}
}

// WithRegistry sets the [Registry] the Hopper resolves a per-message [Codec]
// from by content type. The default is no registry, in which case the raw
// [Message] reaches the handler undecoded. A nil registry is ignored.
func WithRegistry(r *Registry) Option {
	return func(c *config) {
		if r != nil {
			c.registry = r
		}
	}
}

// WithMiddleware appends middleware to wrap the handler, additive across repeated
// uses. The first middleware supplied is the outermost (see [Chain]). Nil entries
// are skipped.
func WithMiddleware(mw ...Middleware) Option {
	return func(c *config) {
		for _, m := range mw {
			if m != nil {
				c.middleware = append(c.middleware, m)
			}
		}
	}
}

// WithConcurrency caps the number of ordered lanes that run in parallel: at most
// n distinct partition keys are processed concurrently, while messages sharing a
// key always run on one lane in order. The default is 1 (strict global order). A
// value < 1 is ignored, leaving the default.
func WithConcurrency(n int) Option {
	return func(c *config) {
		if n >= 1 {
			c.concurrency = n
		}
	}
}

// WithMaxInFlight bounds the number of messages delivered but not yet settled.
// When the window is full the fetch loop blocks, applying backpressure to the
// subscription. The default (0) is unbounded. A value < 0 is ignored.
func WithMaxInFlight(n int) Option {
	return func(c *config) {
		if n >= 0 {
			c.maxInFlight = n
		}
	}
}

// WithBatch enables batch mode for a Hopper driven with [Hopper.RunBatch] or
// [Hopper.ReceiveBatch]. Within each ordered lane the engine accumulates up to
// size messages, or until maxWait elapses since the lane's first buffered
// message, then invokes the [BatchHandler] once with the accumulated slice and
// settles each message by its corresponding [Result]. Per-key ordering and the
// [WithMaxInFlight] bound are preserved: a lane never overlaps two batches, and a
// batch is settled before the lane accepts more.
//
// When the subscription implements [Batched], whole fetched batches are handed to
// lanes as a unit (still re-grouped by key so a lane only ever sees its own keys
// in order); otherwise the engine accumulates per-message under the size/maxWait
// policy. maxWait uses the clock from [WithBatchClock] (default time.Now), so a
// max-wait flush is deterministic in tests.
//
// A size < 1 is treated as 1 (every message is its own batch). A maxWait <= 0
// disables the timer, so a lane flushes only when it reaches size or the run
// drains. WithBatch only takes effect for the RunBatch/ReceiveBatch entry points;
// it is ignored by the per-message [Hopper.Run] and [Hopper.Receive].
//
// Tuning note: a lane buffers up to size delivered-but-unsettled messages, each
// holding a [WithMaxInFlight] slot until the batch is dispatched. Set maxInFlight
// to at least size (times the number of concurrently filling lanes you expect),
// or supply a positive maxWait, so a lane can always reach a flush. With both
// maxWait <= 0 and maxInFlight < size a single busy lane can stall waiting for
// slots it will never get; the size/maxWait pair is the intended steady-state
// trigger.
func WithBatch(size int, maxWait time.Duration) Option {
	return func(c *config) {
		if size < 1 {
			size = 1
		}
		c.batch.enabled = true
		c.batch.size = size
		c.batch.maxWait = maxWait
	}
}

// WithBatchClock injects the clock the batch engine reads to time a lane's
// max-wait flush, for deterministic tests (mirroring sink.WithReservoirClock).
// The default is time.Now. A nil clock is ignored. It has no effect unless
// [WithBatch] is also supplied.
func WithBatchClock(now func() time.Time) Option {
	return func(c *config) {
		if now != nil {
			c.batch.now = now
		}
	}
}
