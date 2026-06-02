// SPDX-License-Identifier: Apache-2.0

package statsd_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/sinktest"
	statsdsink "github.com/stablekernel/crucible/sink/statsd"
)

// call records one StatsD client invocation for assertions.
type call struct {
	method string
	name   string
	i      int64
	f      float64
	d      time.Duration
	s      string
	tags   []string
	rate   float64
}

// fakeClient is a hand-rolled statsd.Client. It records every call and can
// inject an error. It is safe for concurrent use.
type fakeClient struct {
	mu    sync.Mutex
	calls []call
	err   error
}

func (c *fakeClient) record(cl call) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, cl)
	return c.err
}

func (c *fakeClient) snapshot() []call {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]call(nil), c.calls...)
}

func (c *fakeClient) Count(n string, v int64, tags []string, rate float64) error {
	return c.record(call{method: "count", name: n, i: v, tags: tags, rate: rate})
}

func (c *fakeClient) Gauge(n string, v float64, tags []string, rate float64) error {
	return c.record(call{method: "gauge", name: n, f: v, tags: tags, rate: rate})
}

func (c *fakeClient) Histogram(n string, v float64, tags []string, rate float64) error {
	return c.record(call{method: "histogram", name: n, f: v, tags: tags, rate: rate})
}

func (c *fakeClient) Distribution(n string, v float64, tags []string, rate float64) error {
	return c.record(call{method: "distribution", name: n, f: v, tags: tags, rate: rate})
}

func (c *fakeClient) Timing(n string, v time.Duration, tags []string, rate float64) error {
	return c.record(call{method: "timing", name: n, d: v, tags: tags, rate: rate})
}

func (c *fakeClient) Set(n string, v string, tags []string, rate float64) error {
	return c.record(call{method: "set", name: n, s: v, tags: tags, rate: rate})
}

// orderPlaced is a sample domain payload used by the registry tests.
type orderPlaced struct{ Total int64 }

func TestOpConstructors(t *testing.T) {
	t.Parallel()

	tags := []string{"env:test"}
	tests := []struct {
		name string
		op   csink.Op[statsdsink.Client]
		want call
	}{
		{
			name: "count",
			op:   statsdsink.Count("c", 3, tags, 1),
			want: call{method: "count", name: "c", i: 3, tags: tags, rate: 1},
		},
		{
			name: "gauge",
			op:   statsdsink.Gauge("g", 2.5, tags, 1),
			want: call{method: "gauge", name: "g", f: 2.5, tags: tags, rate: 1},
		},
		{
			name: "histogram",
			op:   statsdsink.Histogram("h", 4.5, tags, 0.5),
			want: call{method: "histogram", name: "h", f: 4.5, tags: tags, rate: 0.5},
		},
		{
			name: "distribution",
			op:   statsdsink.Distribution("d", 6.5, tags, 1),
			want: call{method: "distribution", name: "d", f: 6.5, tags: tags, rate: 1},
		},
		{
			name: "timing",
			op:   statsdsink.Timing("t", 20*time.Millisecond, tags, 1),
			want: call{method: "timing", name: "t", d: 20 * time.Millisecond, tags: tags, rate: 1},
		},
		{
			name: "set",
			op:   statsdsink.Set("s", "user-1", tags, 1),
			want: call{method: "set", name: "s", s: "user-1", tags: tags, rate: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fc := &fakeClient{}
			if err := tt.op.Apply(context.Background(), fc); err != nil {
				t.Fatalf("Apply() error = %v", err)
			}
			got := fc.snapshot()
			if len(got) != 1 {
				t.Fatalf("calls = %d, want 1", len(got))
			}
			if got[0].method != tt.want.method || got[0].name != tt.want.name ||
				got[0].i != tt.want.i || got[0].f != tt.want.f || got[0].d != tt.want.d ||
				got[0].s != tt.want.s || got[0].rate != tt.want.rate {
				t.Fatalf("call = %+v, want %+v", got[0], tt.want)
			}
		})
	}
}

func emitterOutlet(c statsdsink.Client) csink.Outlet {
	reg := statsdsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[statsdsink.Client] {
		return statsdsink.Count("orders.placed", o.Total, nil, 1)
	})
	return statsdsink.New(c, reg)
}

func TestEmitterEmitsRegisteredOp(t *testing.T) {
	t.Parallel()

	fc := &fakeClient{}
	if err := emitterOutlet(fc).Sink(context.Background(), orderPlaced{Total: 7}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	got := fc.snapshot()
	if len(got) != 1 || got[0].method != "count" || got[0].i != 7 {
		t.Fatalf("calls = %+v, want one count of 7", got)
	}
}

func TestEmitterUnregisteredSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	err := emitterOutlet(&fakeClient{}).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestEmitterApplyErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("agent unreachable")
	err := emitterOutlet(&fakeClient{err: boom}).Sink(context.Background(), orderPlaced{Total: 1})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "statsd" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:statsd, Phase:apply}", se)
	}
}

func TestAggregatorFoldsCounts(t *testing.T) {
	t.Parallel()

	fc := &fakeClient{}
	agg := statsdsink.NewAggregator(fc, statsdsink.WithInterval(0))
	ctx := context.Background()

	// Two counts with identical name and tags (tag order differs) must fold.
	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeCount, Name: "hits", Int: 2, Tags: []string{"a:1", "b:2"}, Rate: 1})
	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeCount, Name: "hits", Int: 3, Tags: []string{"b:2", "a:1"}, Rate: 1})

	if err := agg.(csink.Flusher).Flush(ctx); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	got := fc.snapshot()
	if len(got) != 1 {
		t.Fatalf("calls = %d (%+v), want 1 folded count", len(got), got)
	}
	if got[0].method != "count" || got[0].name != "hits" || got[0].i != 5 {
		t.Fatalf("folded call = %+v, want count hits=5", got[0])
	}
}

func TestAggregatorGaugeLastWriteWins(t *testing.T) {
	t.Parallel()

	fc := &fakeClient{}
	agg := statsdsink.NewAggregator(fc, statsdsink.WithInterval(0))

	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeGauge, Name: "temp", Value: 1, Rate: 1})
	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeGauge, Name: "temp", Value: 9, Rate: 1})

	if err := agg.(csink.Flusher).Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	got := fc.snapshot()
	if len(got) != 1 || got[0].method != "gauge" || got[0].f != 9 {
		t.Fatalf("calls = %+v, want one gauge of 9", got)
	}
}

func TestAggregatorRawSamplesEmittedEach(t *testing.T) {
	t.Parallel()

	fc := &fakeClient{}
	agg := statsdsink.NewAggregator(fc, statsdsink.WithInterval(0))

	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeHistogram, Name: "lat", Value: 10, Rate: 1})
	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeHistogram, Name: "lat", Value: 20, Rate: 1})
	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeDistribution, Name: "dist", Value: 5, Rate: 1})
	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeTiming, Name: "dur", Dur: time.Second, Rate: 1})
	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeSet, Name: "uniq", Str: "u1", Rate: 1})

	if err := agg.(csink.Flusher).Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	got := fc.snapshot()
	if len(got) != 5 {
		t.Fatalf("calls = %d (%+v), want 5 raw samples", len(got), got)
	}
}

func TestAggregatorFlushEmptyIsNoop(t *testing.T) {
	t.Parallel()

	fc := &fakeClient{}
	agg := statsdsink.NewAggregator(fc, statsdsink.WithInterval(0))
	if err := agg.(csink.Flusher).Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if got := fc.snapshot(); len(got) != 0 {
		t.Fatalf("calls = %+v, want none", got)
	}
}

func TestAggregatorShutdownFinalFlush(t *testing.T) {
	t.Parallel()

	fc := &fakeClient{}
	agg := statsdsink.NewAggregator(fc, statsdsink.WithInterval(0))
	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeCount, Name: "n", Int: 4, Rate: 1})

	if err := agg.(csink.Shutdowner).Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	got := fc.snapshot()
	if len(got) != 1 || got[0].i != 4 {
		t.Fatalf("calls after shutdown = %+v, want one count of 4", got)
	}
	// Idempotent.
	if err := agg.(csink.Shutdowner).Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func TestAggregatorFlushErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("send failed")
	fc := &fakeClient{err: boom}
	agg := statsdsink.NewAggregator(fc, statsdsink.WithInterval(0))
	mustSink(t, agg, statsdsink.Metric{Type: statsdsink.TypeCount, Name: "n", Int: 1, Rate: 1})

	err := agg.(csink.Flusher).Flush(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Flush() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseFlush || se.Outlet != "statsd" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:statsd, Phase:flush}", se)
	}
}

func TestAggregatorRegistryTransform(t *testing.T) {
	t.Parallel()

	reg := statsdsink.NewMetricRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) statsdsink.Metric {
		return statsdsink.Metric{Type: statsdsink.TypeCount, Name: "orders", Int: o.Total, Rate: 1}
	})

	fc := &fakeClient{}
	agg := statsdsink.NewAggregator(fc, statsdsink.WithInterval(0), statsdsink.WithRegistry(reg))
	mustSink(t, agg, orderPlaced{Total: 2})
	mustSink(t, agg, orderPlaced{Total: 5})

	if err := agg.(csink.Flusher).Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	got := fc.snapshot()
	if len(got) != 1 || got[0].i != 7 {
		t.Fatalf("calls = %+v, want one folded count of 7", got)
	}
}

func TestAggregatorUnregisteredSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	agg := statsdsink.NewAggregator(&fakeClient{}, statsdsink.WithInterval(0))
	if err := agg.Sink(context.Background(), other{}); !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestAggregatorConcurrentSinkAndFlush(t *testing.T) {
	t.Parallel()

	fc := &fakeClient{}
	agg := statsdsink.NewAggregator(fc, statsdsink.WithInterval(0))
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = agg.Sink(ctx, statsdsink.Metric{Type: statsdsink.TypeCount, Name: "race", Int: 1, Rate: 1})
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = agg.(csink.Flusher).Flush(ctx)
		}()
	}
	wg.Wait()

	if err := agg.(csink.Flusher).Flush(ctx); err != nil {
		t.Fatalf("final Flush() error = %v", err)
	}

	var total int64
	for _, c := range fc.snapshot() {
		if c.method == "count" && c.name == "race" {
			total += c.i
		}
	}
	if total != 64 {
		t.Fatalf("summed race counts = %d, want 64", total)
	}
}

func TestConformanceEmitter(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return emitterOutlet(&fakeClient{}) })
}

func TestConformanceAggregator(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet {
		return statsdsink.NewAggregator(&fakeClient{}, statsdsink.WithInterval(0))
	})
}

func mustSink(t *testing.T, o csink.Outlet, payload any) {
	t.Helper()
	if err := o.Sink(context.Background(), payload); err != nil {
		t.Fatalf("Sink(%T) error = %v", payload, err)
	}
}
