// SPDX-License-Identifier: Apache-2.0

// Package statsd is a sink destination that aggregates payloads into StatsD
// metrics and flushes them to a StatsD client on an interval and on demand. It
// depends on crucible/sink and a single StatsD SDK whose client satisfies the
// narrow [Client] interface declared here.
//
// Two surfaces are offered. The primary one is the [Aggregator]: it folds
// counters (summed) and gauges (last write wins) by metric identity, buffers
// histograms, distributions, timings, and sets as raw samples, and emits them
// to the [Client] on a flush interval and on Flush or Shutdown. It implements
// sink.Outlet, sink.Flusher, and sink.Shutdowner so a sink.Manifold drives its
// lifecycle. The secondary surface is the Emitter path ([NewRegistry] and
// [New]) for callers who want a payload-to-operation mapping with no in-process
// aggregation.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package statsd

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow StatsD surface this destination needs. It is satisfied
// structurally by the StatsD SDK client (and its no-op and mock variants), so
// tests wire a hand-rolled fake and consumers wire the real client without this
// package depending on a concrete SDK type in any exported signature.
type Client interface {
	Count(name string, value int64, tags []string, rate float64) error
	Gauge(name string, value float64, tags []string, rate float64) error
	Histogram(name string, value float64, tags []string, rate float64) error
	Distribution(name string, value float64, tags []string, rate float64) error
	Timing(name string, value time.Duration, tags []string, rate float64) error
	Set(name string, value string, tags []string, rate float64) error
}

// MetricType identifies which StatsD metric a Metric carries.
type MetricType uint8

const (
	// TypeCount is a monotonic counter; samples with the same identity are summed.
	TypeCount MetricType = iota
	// TypeGauge is a point-in-time value; the last sample for an identity wins.
	TypeGauge
	// TypeHistogram is a statistical sample emitted verbatim, one per sample.
	TypeHistogram
	// TypeDistribution is a global distribution sample emitted verbatim.
	TypeDistribution
	// TypeTiming is a duration sample emitted verbatim.
	TypeTiming
	// TypeSet is a unique-value sample emitted verbatim.
	TypeSet
)

// Metric is the payload an Aggregator folds. Name and Type are required; the
// value field that matters depends on Type. Count reads Int, Set reads Str,
// Timing reads Dur, and the float types (Gauge, Histogram, Distribution) read
// Value. Rate is the StatsD sample rate (1 means every sample); a non-positive
// Rate is treated as 1.
type Metric struct {
	// Name is the metric name, for example "orders.placed".
	Name string
	// Str is the sample for Set metrics.
	Str string
	// Tags are the StatsD tags applied to the metric.
	Tags []string
	// Value is the sample for Gauge, Histogram, and Distribution metrics.
	Value float64
	// Rate is the StatsD sample rate; non-positive is treated as 1.
	Rate float64
	// Int is the sample for Count metrics.
	Int int64
	// Dur is the sample for Timing metrics.
	Dur time.Duration
	// Type selects which StatsD metric and value field apply.
	Type MetricType
}

func (m Metric) rate() float64 {
	if m.Rate <= 0 {
		return 1
	}
	return m.Rate
}

// identity keys an aggregated counter or gauge by name and sorted tag set so
// that two samples with the same logical metric fold together regardless of tag
// order.
type identity struct {
	name string
	tags string
}

func keyFor(name string, tags []string) identity {
	sorted := append([]string(nil), tags...)
	sort.Strings(sorted)
	return identity{name: name, tags: strings.Join(sorted, ",")}
}

// folded carries an aggregated counter or gauge ready to emit. For counters
// value is the running sum; for gauges it is the last write.
type folded struct {
	name  string
	tags  []string
	rate  float64
	value float64
}

// window holds one aggregation interval: counters and gauges folded by
// identity, and raw samples preserved in arrival order for the verbatim metric
// types.
type window struct {
	counts map[identity]*folded
	gauges map[identity]*folded
	raw    []Metric
}

func newWindow() *window {
	return &window{
		counts: make(map[identity]*folded),
		gauges: make(map[identity]*folded),
	}
}

func (w *window) empty() bool {
	return len(w.counts) == 0 && len(w.gauges) == 0 && len(w.raw) == 0
}

// add folds a metric into the window.
func (w *window) add(m Metric) {
	switch m.Type {
	case TypeCount:
		k := keyFor(m.Name, m.Tags)
		if f, ok := w.counts[k]; ok {
			f.value += float64(m.Int)
			return
		}
		w.counts[k] = &folded{name: m.Name, tags: m.Tags, rate: m.rate(), value: float64(m.Int)}
	case TypeGauge:
		k := keyFor(m.Name, m.Tags)
		w.gauges[k] = &folded{name: m.Name, tags: m.Tags, rate: m.rate(), value: m.Value}
	case TypeHistogram, TypeDistribution, TypeTiming, TypeSet:
		w.raw = append(w.raw, m)
	}
}

// Aggregator folds StatsD payloads in process and emits them to a Client on an
// interval and on Flush or Shutdown. It is safe for concurrent use: Sink folds
// under a mutex, and flush swaps the live window for a fresh one under the same
// mutex before emitting outside the lock, so a slow Client never blocks
// producers. It implements sink.Outlet, sink.Flusher, and sink.Shutdowner.
type Aggregator struct {
	client   Client
	registry *csink.Registry[Metric]
	name     string

	interval time.Duration
	now      func() time.Time

	mu  sync.Mutex
	cur *window

	// Background-loop lifecycle. started guards a single loop start; stopped
	// makes Shutdown idempotent. Both are mutex-protected.
	started bool
	stopped bool
	ticker  ticker
	stop    chan struct{}
	done    chan struct{}
}

// NewMetricRegistry returns an empty registry mapping payload types to Metric
// values, for use with WithRegistry. Populate it with sink.Register.
func NewMetricRegistry() *csink.Registry[Metric] {
	return csink.NewRegistry[Metric]()
}

// AggregatorOption configures an Aggregator. Options are additive and have
// no-op defaults; a zero or nil value is ignored, leaving the default in place.
type AggregatorOption func(*Aggregator)

// WithName sets the outlet name used when wrapping emit failures. The default
// is "statsd". An empty name is ignored.
func WithName(name string) AggregatorOption {
	return func(a *Aggregator) {
		if name != "" {
			a.name = name
		}
	}
}

// WithRegistry installs a transformer registry so the Aggregator can fold
// arbitrary payload types, not only Metric values. A payload with no registered
// transformer is skipped (sink.ErrUnregistered). A nil registry is ignored.
func WithRegistry(reg *csink.Registry[Metric]) AggregatorOption {
	return func(a *Aggregator) {
		if reg != nil {
			a.registry = reg
		}
	}
}

// WithInterval sets the background flush interval. A non-positive interval
// disables the background loop, leaving Flush and Shutdown as the only emit
// triggers. The default is 10 seconds.
func WithInterval(d time.Duration) AggregatorOption {
	return func(a *Aggregator) { a.interval = d }
}

// WithClock injects the time source the background loop schedules against,
// enabling deterministic tests that advance time without sleeping. The default
// is time.Now. A nil clock is ignored.
func WithClock(now func() time.Time) AggregatorOption {
	return func(a *Aggregator) {
		if now != nil {
			a.now = now
		}
	}
}

// NewAggregator builds an Aggregator bound to client and returns it as a
// sink.Outlet. The background flush loop starts on the first Sink, so an idle
// Aggregator holds no goroutine. Attach the result to a sink.Manifold, whose
// Flush and Shutdown drive the matching methods.
func NewAggregator(client Client, opts ...AggregatorOption) csink.Outlet {
	a := &Aggregator{
		client:   client,
		name:     "statsd",
		interval: 10 * time.Second,
		now:      time.Now,
		cur:      newWindow(),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// Sink folds payload into the current window. A Metric payload is folded
// directly; any other payload is resolved through the registry installed with
// WithRegistry, and an unregistered type returns sink.ErrUnregistered. The
// first Sink starts the background flush loop when an interval is configured.
func (a *Aggregator) Sink(_ context.Context, payload any) error {
	m, ok := a.metricFor(payload)
	if !ok {
		return csink.ErrUnregistered
	}
	a.mu.Lock()
	a.cur.add(m)
	a.maybeStartLoop()
	a.mu.Unlock()
	return nil
}

// metricFor resolves payload to a Metric, either directly or via the registry.
func (a *Aggregator) metricFor(payload any) (Metric, bool) {
	if m, ok := payload.(Metric); ok {
		return m, true
	}
	if a.registry == nil {
		return Metric{}, false
	}
	transform, ok := a.registry.Lookup(payload)
	if !ok {
		return Metric{}, false
	}
	return transform(context.Background(), payload), true
}

// Flush swaps the live window for a fresh one and emits the captured window to
// the Client. It is safe to call concurrently with Sink and is a no-op on an
// empty window. The first emit error is returned (wrapped as *sink.Error with
// PhaseFlush); the remaining metrics in the window are still attempted.
func (a *Aggregator) Flush(_ context.Context) error {
	return a.flush()
}

// Shutdown stops the background loop, performs a final flush, and returns any
// emit error from that flush. It is idempotent: a second call is a no-op
// returning nil.
func (a *Aggregator) Shutdown(_ context.Context) error {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return nil
	}
	a.stopped = true
	running := a.started
	a.mu.Unlock()

	if running {
		close(a.stop)
		<-a.done
	}
	return a.flush()
}

// maybeStartLoop starts the background flush loop once when an interval is set.
// The caller holds a.mu.
func (a *Aggregator) maybeStartLoop() {
	if a.started || a.stopped || a.interval <= 0 {
		return
	}
	a.started = true
	if a.ticker == nil {
		a.ticker = newRealTicker(a.interval, a.now)
	}
	go a.loop()
}

// loop drives interval flushes until stop is closed. Emit errors are
// intentionally swallowed here: the background loop has no caller to return them
// to, mirroring how a Manifold treats outlet failures as logged-not-fatal.
func (a *Aggregator) loop() {
	defer close(a.done)
	defer a.ticker.Stop()
	c := a.ticker.C()
	for {
		select {
		case <-a.stop:
			return
		case <-c:
			_ = a.flush()
		}
	}
}

// swap atomically replaces the live window with a fresh one and returns the old
// one. The lock is held only for the swap, never across a Client call.
func (a *Aggregator) swap() *window {
	a.mu.Lock()
	defer a.mu.Unlock()
	old := a.cur
	a.cur = newWindow()
	return old
}

// flush captures and emits the current window.
func (a *Aggregator) flush() error {
	w := a.swap()
	if w.empty() {
		return nil
	}
	return a.emit(w)
}

// emit writes a captured window to the Client. Folded counters and gauges go
// out as one sample each; raw samples go out verbatim in arrival order. The
// first error is captured and returned wrapped, while the remaining metrics are
// still attempted so one bad sample does not strand the window.
func (a *Aggregator) emit(w *window) error {
	var first error
	record := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}

	for _, f := range w.counts {
		record(a.client.Count(f.name, int64(f.value), f.tags, f.rate))
	}
	for _, f := range w.gauges {
		record(a.client.Gauge(f.name, f.value, f.tags, f.rate))
	}
	for _, m := range w.raw {
		record(a.emitRaw(m))
	}

	if first != nil {
		return &csink.Error{Outlet: a.name, Phase: csink.PhaseFlush, PayloadType: "statsd.Metric", Err: first}
	}
	return nil
}

func (a *Aggregator) emitRaw(m Metric) error {
	switch m.Type {
	case TypeHistogram:
		return a.client.Histogram(m.Name, m.Value, m.Tags, m.rate())
	case TypeDistribution:
		return a.client.Distribution(m.Name, m.Value, m.Tags, m.rate())
	case TypeTiming:
		return a.client.Timing(m.Name, m.Dur, m.Tags, m.rate())
	case TypeSet:
		return a.client.Set(m.Name, m.Str, m.Tags, m.rate())
	case TypeCount, TypeGauge:
		return nil
	default:
		return nil
	}
}

var (
	_ csink.Outlet     = (*Aggregator)(nil)
	_ csink.Flusher    = (*Aggregator)(nil)
	_ csink.Shutdowner = (*Aggregator)(nil)
)
