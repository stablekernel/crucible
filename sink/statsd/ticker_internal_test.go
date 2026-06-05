// SPDX-License-Identifier: Apache-2.0

package statsd

import (
	"context"
	"testing"
	"time"

	csink "github.com/stablekernel/crucible/sink"
)

// TestRealTickerFiresStampedTicks verifies the production tick source wraps
// time.Ticker, stamps each tick with the injected clock, and releases its
// goroutine on Stop without leaking.
func TestRealTickerFiresStampedTicks(t *testing.T) {
	t.Parallel()

	stamp := time.Unix(1000, 0)
	tk := newRealTicker(time.Millisecond, func() time.Time { return stamp })
	t.Cleanup(tk.Stop)

	select {
	case got := <-tk.C():
		if !got.Equal(stamp) {
			t.Fatalf("tick stamp = %v, want %v (the injected clock)", got, stamp)
		}
	case <-time.After(time.Second):
		t.Fatal("realTicker did not fire within 1s")
	}
}

// TestRealTickerStopIsClean verifies Stop returns once the run goroutine has
// exited, so a second Stop via t.Cleanup would be redundant rather than racy.
func TestRealTickerStopIsClean(t *testing.T) {
	t.Parallel()

	tk := newRealTicker(time.Hour, time.Now)
	done := make(chan struct{})
	go func() {
		tk.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return within 1s")
	}
}

// ctxKey is a private context key used to assert the caller context survives
// the registry transform step.
type ctxKey struct{}

// TestMetricForThreadsContext verifies metricFor passes the caller's context
// into the registry transformer rather than substituting context.Background.
func TestMetricForThreadsContext(t *testing.T) {
	t.Parallel()

	type payload struct{ V int }
	reg := NewMetricRegistry()
	var seen context.Context
	csink.Register(reg, func(ctx context.Context, p payload) Metric {
		seen = ctx
		return Metric{Type: TypeCount, Name: "n", Int: int64(p.V), Rate: 1}
	})

	a := &Aggregator{
		client:   &countingClient{flushed: make(chan struct{}, 1)},
		registry: reg,
		name:     "statsd",
		cur:      newWindow(),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}

	ctx := context.WithValue(context.Background(), ctxKey{}, "sentinel")
	m, ok := a.metricFor(ctx, payload{V: 3})
	if !ok {
		t.Fatal("metricFor returned ok=false for a registered payload")
	}
	if m.Int != 3 {
		t.Fatalf("metric Int = %d, want 3", m.Int)
	}
	if seen == nil || seen.Value(ctxKey{}) != "sentinel" {
		t.Fatalf("transformer context = %v, want the caller context with the sentinel value", seen)
	}
}
