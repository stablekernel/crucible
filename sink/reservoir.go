// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"sync"
	"time"

	"github.com/stablekernel/crucible/telemetry"
)

// ReservoirOption configures a Reservoir. Options are additive with no-op
// defaults.
type ReservoirOption func(*reservoirConfig)

type reservoirConfig struct {
	size     int
	interval time.Duration
	now      func() time.Time
	meter    telemetry.Meter
	maxBuf   int // 0 = unbounded
}

func defaultReservoirConfig() reservoirConfig {
	return reservoirConfig{
		size:     100,
		interval: 5 * time.Second,
		now:      time.Now,
		meter:    telemetry.NopMeter(),
	}
}

// WithBatchSize sets the buffered-payload count that triggers a flush. A size
// <= 0 disables size-triggered flushing (interval only).
func WithBatchSize(n int) ReservoirOption { return func(c *reservoirConfig) { c.size = n } }

// WithBatchInterval sets the period of the background time-triggered flush. An
// interval <= 0 disables the background loop (size/manual flush only).
func WithBatchInterval(d time.Duration) ReservoirOption {
	return func(c *reservoirConfig) { c.interval = d }
}

// WithReservoirClock injects the clock the Reservoir reads to time flush
// latency, for deterministic tests. The default is time.Now. A nil clock is
// ignored.
func WithReservoirClock(now func() time.Time) ReservoirOption {
	return func(c *reservoirConfig) {
		if now != nil {
			c.now = now
		}
	}
}

// WithReservoirMeter sets the meter the Reservoir records batch-size, flush
// latency, and drop counts on. The default is telemetry.NopMeter(). A nil meter
// is ignored.
func WithReservoirMeter(m telemetry.Meter) ReservoirOption {
	return func(c *reservoirConfig) {
		if m != nil {
			c.meter = m
		}
	}
}

// WithMaxBuffered caps the number of payloads held between flushes. Payloads
// arriving over the cap are dropped and counted on sink.dropped. The default (0)
// is unbounded.
func WithMaxBuffered(n int) ReservoirOption { return func(c *reservoirConfig) { c.maxBuf = n } }

// reservoir buffers payloads and releases them to inner in batches, by size or
// on an interval. It is an Outlet, a Flusher, and a Shutdowner.
type reservoir struct {
	inner    Outlet
	size     int
	interval time.Duration
	now      func() time.Time
	maxBuf   int

	batchSize    telemetry.Histogram
	flushLatency telemetry.Histogram
	dropped      telemetry.Counter

	mu  sync.Mutex
	buf []any

	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once // guards Shutdown
}

// Reservoir wraps inner so payloads are buffered and flushed in batches, by
// reaching the batch size or on the interval tick. The returned Outlet is also a
// Flusher and a Shutdowner; call Shutdown (or attach it to a Manifold) to stop
// the background loop and drain. Defaults: size 100, interval 5s, now = time.Now,
// meter = telemetry.NopMeter(), unbounded buffer. When inner is a BatchOutlet,
// flushes use SinkBatch; otherwise each buffered payload is sunk in turn.
func Reservoir(inner Outlet, opts ...ReservoirOption) Outlet {
	cfg := defaultReservoirConfig()
	for _, o := range opts {
		o(&cfg)
	}
	r := &reservoir{
		inner:        inner,
		size:         cfg.size,
		interval:     cfg.interval,
		now:          cfg.now,
		maxBuf:       cfg.maxBuf,
		batchSize:    cfg.meter.Histogram("sink.batch_size", telemetry.WithUnit("{record}")),
		flushLatency: cfg.meter.Histogram("sink.flush_latency_ms", telemetry.WithUnit("ms")),
		dropped:      cfg.meter.Counter("sink.dropped", telemetry.WithUnit("{record}")),
	}
	if r.interval > 0 {
		loopCtx, cancel := context.WithCancel(context.Background())
		r.cancel = cancel
		r.wg.Add(1)
		go r.loop(loopCtx)
	}
	return r
}

// Sink buffers payload, flushing synchronously if the buffer reaches the batch
// size. An over-cap payload is dropped and counted, never blocking the caller.
func (r *reservoir) Sink(ctx context.Context, payload any) error {
	r.mu.Lock()
	if r.maxBuf > 0 && len(r.buf) >= r.maxBuf {
		r.mu.Unlock()
		r.dropped.Add(ctx, 1)
		return nil
	}
	r.buf = append(r.buf, payload)
	full := r.size > 0 && len(r.buf) >= r.size
	batch := r.takeLocked(full)
	r.mu.Unlock()

	if batch == nil {
		return nil
	}
	return r.dispatch(ctx, batch)
}

// Flush releases all buffered payloads to inner synchronously.
func (r *reservoir) Flush(ctx context.Context) error {
	r.mu.Lock()
	batch := r.takeLocked(true)
	r.mu.Unlock()
	if batch == nil {
		return nil
	}
	return r.dispatch(ctx, batch)
}

// Shutdown stops the background loop, waits for it to exit, and flushes any
// remaining buffered payloads. It is idempotent.
func (r *reservoir) Shutdown(ctx context.Context) error {
	r.once.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		r.wg.Wait()
	})
	return r.Flush(ctx)
}

// takeLocked returns the buffered payloads to dispatch and clears the buffer, or
// nil when there is nothing to flush. The caller must hold r.mu.
func (r *reservoir) takeLocked(release bool) []any {
	if !release || len(r.buf) == 0 {
		return nil
	}
	batch := r.buf
	r.buf = nil
	return batch
}

// dispatch sends a batch to inner, via SinkBatch when supported, recording the
// batch size and flush latency on the injected clock.
func (r *reservoir) dispatch(ctx context.Context, batch []any) error {
	start := r.now()
	r.batchSize.Record(ctx, float64(len(batch)))

	var err error
	if b, ok := r.inner.(BatchOutlet); ok {
		err = b.SinkBatch(ctx, batch)
	} else {
		for _, p := range batch {
			if e := r.inner.Sink(ctx, p); e != nil {
				err = e
			}
		}
	}
	r.flushLatency.Record(ctx, float64(r.now().Sub(start).Milliseconds()))
	return err
}

// loop drives interval-triggered flushes until ctx is canceled.
func (r *reservoir) loop(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// tick performs one interval-driven flush. It is the seam the background loop
// calls on each tick and the deterministic, sleep-free entry point tests use to
// drive an interval flush.
func (r *reservoir) tick(ctx context.Context) {
	r.mu.Lock()
	batch := r.takeLocked(true)
	r.mu.Unlock()
	if batch != nil {
		_ = r.dispatch(ctx, batch)
	}
}

var (
	_ Outlet     = (*reservoir)(nil)
	_ Flusher    = (*reservoir)(nil)
	_ Shutdowner = (*reservoir)(nil)
)
