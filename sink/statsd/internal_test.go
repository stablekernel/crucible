// SPDX-License-Identifier: Apache-2.0

package statsd

import (
	"context"
	"sync"
	"testing"
	"time"
)

// manualTicker is a hand-driven ticker for white-box tests. Sending on tick
// fires the loop; the test controls timing entirely, with no sleeps.
type manualTicker struct {
	c        chan time.Time
	stopped  chan struct{}
	stopOnce sync.Once
}

func newManualTicker() *manualTicker {
	return &manualTicker{c: make(chan time.Time, 1), stopped: make(chan struct{})}
}

func (m *manualTicker) C() <-chan time.Time { return m.c }

func (m *manualTicker) Stop() { m.stopOnce.Do(func() { close(m.stopped) }) }

// tick fires one tick stamped at now.
func (m *manualTicker) tick(now time.Time) { m.c <- now }

// countingClient counts flushed counts and signals each flush on flushed.
type countingClient struct {
	mu      sync.Mutex
	count   int64
	flushed chan struct{}
}

func (c *countingClient) Count(_ string, v int64, _ []string, _ float64) error {
	c.mu.Lock()
	c.count += v
	c.mu.Unlock()
	c.flushed <- struct{}{}
	return nil
}

func (c *countingClient) total() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func (c *countingClient) Gauge(string, float64, []string, float64) error        { return nil }
func (c *countingClient) Histogram(string, float64, []string, float64) error    { return nil }
func (c *countingClient) Distribution(string, float64, []string, float64) error { return nil }
func (c *countingClient) Timing(string, time.Duration, []string, float64) error { return nil }
func (c *countingClient) Set(string, string, []string, float64) error           { return nil }

// TestIntervalFlushDrivenByTick exercises the background loop through the
// injected tick/flush seam: advancing the clock and firing a tick triggers a
// flush, with no sleeps and no reliance on wall-clock timing.
func TestIntervalFlushDrivenByTick(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{at: time.Unix(0, 0)}
	cc := &countingClient{flushed: make(chan struct{}, 4)}
	mt := newManualTicker()

	a := &Aggregator{
		client:   cc,
		name:     "statsd",
		interval: time.Second,
		now:      clock.now,
		cur:      newWindow(),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		ticker:   mt, // injected before the loop starts
	}

	ctx := context.Background()
	if err := a.Sink(ctx, Metric{Type: TypeCount, Name: "n", Int: 5, Rate: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	// Advance the clock past one interval and fire a tick; the loop flushes.
	clock.advance(time.Second)
	mt.tick(clock.now())
	<-cc.flushed // deterministic handoff, no sleep

	if got := cc.total(); got != 5 {
		t.Fatalf("after interval tick, total = %d, want 5", got)
	}

	// A second window flushes on the next tick.
	if err := a.Sink(ctx, Metric{Type: TypeCount, Name: "n", Int: 7, Rate: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	clock.advance(time.Second)
	mt.tick(clock.now())
	<-cc.flushed

	if got := cc.total(); got != 12 {
		t.Fatalf("after second tick, total = %d, want 12", got)
	}

	if err := a.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

// fakeClock is a monotonic test clock advanced explicitly by the test.
type fakeClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// TestMetricRateDefault verifies a non-positive rate normalizes to 1.
func TestMetricRateDefault(t *testing.T) {
	t.Parallel()
	for _, r := range []float64{0, -1} {
		if got := (Metric{Rate: r}).rate(); got != 1 {
			t.Fatalf("rate(%v) = %v, want 1", r, got)
		}
	}
	if got := (Metric{Rate: 0.25}).rate(); got != 0.25 {
		t.Fatalf("rate(0.25) = %v, want 0.25", got)
	}
}
