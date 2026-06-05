// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"sync"
	"time"
)

// CollectFunc samples some state and yields zero or more payloads by calling
// sink for each. It runs on the Poller's interval; it must not block
// indefinitely.
type CollectFunc func(ctx context.Context, sink func(payload any))

// PollerOption configures a Poller.
type PollerOption func(*pollerConfig)

type pollerConfig struct {
	interval time.Duration
}

func defaultPollerConfig() pollerConfig {
	return pollerConfig{interval: 60 * time.Second}
}

// WithPollInterval sets the sampling period. The default is 60s.
func WithPollInterval(d time.Duration) PollerOption {
	return func(c *pollerConfig) { c.interval = d }
}

// Poller periodically runs a CollectFunc and sinks each yielded payload to a
// target Outlet (via SinkBatch when the target supports it). Construct with
// NewPoller; drive it with Start and stop it with Stop.
type Poller struct {
	target   Outlet
	collect  CollectFunc
	interval time.Duration

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
}

// NewPoller binds a target and a CollectFunc into a Poller. The default
// interval is 60s.
func NewPoller(target Outlet, collect CollectFunc, opts ...PollerOption) *Poller {
	cfg := defaultPollerConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return &Poller{target: target, collect: collect, interval: cfg.interval}
}

// Start launches the sampling loop and returns the Poller for chaining. It is
// idempotent: a second call while running is a no-op.
func (p *Poller) Start(ctx context.Context) *Poller {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return p
	}
	loopCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.done = make(chan struct{})
	p.started = true
	go p.loop(loopCtx)
	return p
}

// Stop cancels the sampling loop and waits for it to exit. It is safe to call
// when not started and is idempotent.
func (p *Poller) Stop() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	cancel, done := p.cancel, p.done
	p.started = false
	p.mu.Unlock()

	cancel()
	<-done
}

func (p *Poller) loop(ctx context.Context) {
	defer close(p.done)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.collectOnce(ctx)
		}
	}
}

// collectOnce runs the CollectFunc once and delivers the yielded payloads to the
// target, batching via SinkBatch when supported. It is the deterministic,
// sleep-free seam tests drive directly.
func (p *Poller) collectOnce(ctx context.Context) {
	var batch []any
	p.collect(ctx, func(payload any) { batch = append(batch, payload) })
	if len(batch) == 0 {
		return
	}
	if b, ok := p.target.(BatchOutlet); ok {
		_ = b.SinkBatch(ctx, batch)
		return
	}
	for _, payload := range batch {
		_ = p.target.Sink(ctx, payload)
	}
}
