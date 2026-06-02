// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"sync"

	"github.com/stablekernel/crucible/telemetry"
)

// fakeMeter is an in-test telemetry.Meter that records counter totals and
// histogram observation counts by instrument name. Instruments are cached by
// name so repeated constructor calls report into the same accumulator.
type fakeMeter struct {
	mu         sync.Mutex
	counters   map[string]*fakeCounter
	histograms map[string]*fakeHistogram
	gauges     map[string]*fakeGauge
}

func newFakeMeter() *fakeMeter {
	return &fakeMeter{
		counters:   map[string]*fakeCounter{},
		histograms: map[string]*fakeHistogram{},
		gauges:     map[string]*fakeGauge{},
	}
}

func (m *fakeMeter) Counter(name string, _ ...telemetry.InstrumentOption) telemetry.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.counters[name]; ok {
		return c
	}
	c := &fakeCounter{}
	m.counters[name] = c
	return c
}

func (m *fakeMeter) Histogram(name string, _ ...telemetry.InstrumentOption) telemetry.Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.histograms[name]; ok {
		return h
	}
	h := &fakeHistogram{}
	m.histograms[name] = h
	return h
}

func (m *fakeMeter) Gauge(name string, _ ...telemetry.InstrumentOption) telemetry.Gauge {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g, ok := m.gauges[name]; ok {
		return g
	}
	g := &fakeGauge{}
	m.gauges[name] = g
	return g
}

func (m *fakeMeter) counterValue(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.counters[name]; ok {
		return c.total()
	}
	return 0
}

func (m *fakeMeter) histogramObserved(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.histograms[name]; ok {
		return h.count() > 0
	}
	return false
}

type fakeCounter struct {
	mu  sync.Mutex
	sum int64
}

func (c *fakeCounter) Add(_ context.Context, n int64, _ ...telemetry.Attr) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sum += n
}

func (c *fakeCounter) total() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sum
}

type fakeHistogram struct {
	mu sync.Mutex
	n  int
}

func (h *fakeHistogram) Record(_ context.Context, _ float64, _ ...telemetry.Attr) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.n++
}

func (h *fakeHistogram) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

type fakeGauge struct{}

func (fakeGauge) Record(context.Context, float64, ...telemetry.Attr) {}

// evt is an arbitrary payload type for internal (white-box) tests, which cannot
// see the payloadA/payloadB fixtures defined in the external test package.
type evt struct{ N int }

// fakeTracer records spans started, errors recorded, and statuses set.
type fakeTracer struct {
	mu    sync.Mutex
	spans []*fakeSpan
}

func (tr *fakeTracer) Start(ctx context.Context, name string, _ ...telemetry.Attr) (context.Context, telemetry.Span) {
	s := &fakeSpan{name: name}
	tr.mu.Lock()
	tr.spans = append(tr.spans, s)
	tr.mu.Unlock()
	return ctx, s
}

func (tr *fakeTracer) only() *fakeSpan {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if len(tr.spans) == 0 {
		return nil
	}
	return tr.spans[0]
}

type fakeSpan struct {
	mu       sync.Mutex
	name     string
	errors   int
	ended    bool
	status   telemetry.StatusCode
	statated bool
}

func (s *fakeSpan) SetAttributes(...telemetry.Attr) {}

func (s *fakeSpan) RecordError(error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors++
}

func (s *fakeSpan) SetStatus(code telemetry.StatusCode, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = code
	s.statated = true
}

func (s *fakeSpan) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = true
}

func (s *fakeSpan) errorCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errors
}
