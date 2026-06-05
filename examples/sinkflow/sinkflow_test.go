// SPDX-License-Identifier: Apache-2.0

package sinkflow_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/bridge"

	"github.com/stablekernel/crucible/examples/sinkflow"
	"github.com/stablekernel/crucible/telemetry"
)

// TestFlakyOutletSinkRejectsUnregisteredPayload covers FlakyOutlet.Sink's
// non-Transition branch: the bridge always hands it a bridge.Transition, but the
// outlet defends against a misregistered transformer by returning
// sink.ErrUnregistered for any other payload type. A matching-event Transition
// then exercises the induced-failure branch, whose error renders a non-empty
// message.
func TestFlakyOutletSinkRejectsUnregisteredPayload(t *testing.T) {
	t.Parallel()
	out := &sinkflow.FlakyOutlet{FailOnEvent: sinkflow.Dispatch}

	if err := out.Sink(context.Background(), "not a transition"); !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(non-transition) = %v, want ErrUnregistered", err)
	}

	err := out.Sink(context.Background(), bridge.Transition{Machine: "order", Event: sinkflow.Dispatch})
	if err == nil {
		t.Fatal("Sink of the failing event = nil, want an induced failure")
	}
	if err.Error() == "" {
		t.Fatal("induced failure rendered an empty error message")
	}

	if got := len(out.Delivered); got != 0 {
		t.Fatalf("a rejected and a failed Sink should record nothing; Delivered=%d", got)
	}
}

// --- recording telemetry + logging seams for the assertions -----------------

type ctxKey struct{}

type recSpan struct{}

func (recSpan) SetAttributes(...telemetry.Attr)        {}
func (recSpan) RecordError(error)                      {}
func (recSpan) SetStatus(telemetry.StatusCode, string) {}
func (recSpan) End()                                   {}

type spanRec struct {
	id, name, parent string
}

type recTracer struct {
	mu    sync.Mutex
	spans []spanRec
	n     int
}

func (tr *recTracer) Start(ctx context.Context, name string, _ ...telemetry.Attr) (context.Context, telemetry.Span) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.n++
	id := string(rune('a' + tr.n - 1))
	parent, _ := ctx.Value(ctxKey{}).(string)
	tr.spans = append(tr.spans, spanRec{id: id, name: name, parent: parent})
	return context.WithValue(ctx, ctxKey{}, id), recSpan{}
}

type fakeCounter struct {
	mu  sync.Mutex
	sum int64
}

func (c *fakeCounter) Add(_ context.Context, n int64, _ ...telemetry.Attr) {
	c.mu.Lock()
	c.sum += n
	c.mu.Unlock()
}

type fakeMeter struct {
	mu       sync.Mutex
	counters map[string]*fakeCounter
}

func newFakeMeter() *fakeMeter { return &fakeMeter{counters: map[string]*fakeCounter{}} }

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

type noopHist struct{}

func (noopHist) Record(context.Context, float64, ...telemetry.Attr) {}

type noopGauge struct{}

func (noopGauge) Record(context.Context, float64, ...telemetry.Attr) {}

func (m *fakeMeter) Histogram(string, ...telemetry.InstrumentOption) telemetry.Histogram {
	return noopHist{}
}

func (m *fakeMeter) Gauge(string, ...telemetry.InstrumentOption) telemetry.Gauge { return noopGauge{} }

func (m *fakeMeter) value(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.counters[name]; ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.sum
	}
	return 0
}

type captureHandler struct {
	mu   sync.Mutex
	errs int
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelError {
		h.mu.Lock()
		h.errs++
		h.mu.Unlock()
	}
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) errorCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.errs
}

// --- the flagship end-to-end assertions --------------------------------------

func TestFlowFansEveryTransitionToEveryDestination(t *testing.T) {
	t.Parallel()

	tr := &recTracer{}
	meter := newFakeMeter()
	handler := &captureHandler{}
	flow := sinkflow.New(sinkflow.NewOptions{
		Logger: slog.New(handler),
		Tracer: tr,
		Meter:  meter,
	})

	if final := flow.Run(context.Background()); final != sinkflow.Delivered {
		t.Fatalf("final stage = %q, want %q", final, sinkflow.Delivered)
	}

	// Every healthy destination receives all three transitions, even though one
	// outlet failed on the dispatch transition.
	if got := len(flow.Analytics.All()); got != 3 {
		t.Errorf("analytics received %d transitions, want 3", got)
	}
	if got := len(flow.Audit.All()); got != 3 {
		t.Errorf("audit received %d transitions, want 3", got)
	}
	// The warehouse failed on Dispatch, so it has the other two.
	if got := len(flow.Warehouse.Delivered); got != 2 {
		t.Errorf("warehouse delivered %d transitions, want 2 (dispatch failed)", got)
	}

	// The induced failure is observed on both the logger and the sink.failed counter.
	if got := meter.value("sink.failed"); got != 1 {
		t.Errorf("sink.failed = %d, want 1", got)
	}
	if got := handler.errorCount(); got != 1 {
		t.Errorf("error log records = %d, want 1", got)
	}
}

func TestFlowNestsEmitSpansUnderTransitionSpans(t *testing.T) {
	t.Parallel()

	tr := &recTracer{}
	flow := sinkflow.New(sinkflow.NewOptions{Tracer: tr})
	flow.Run(context.Background())

	tr.mu.Lock()
	defer tr.mu.Unlock()
	parents := map[string]string{} // span id -> name, to resolve a parent's name
	for _, s := range tr.spans {
		parents[s.id] = s.name
	}
	var transitions, nestedEmits int
	for _, s := range tr.spans {
		switch s.name {
		case "state.transition":
			transitions++
		case "sink.Sink":
			if parents[s.parent] == "state.transition" {
				nestedEmits++
			}
		}
	}
	if transitions != 3 {
		t.Errorf("state.transition spans = %d, want 3", transitions)
	}
	if nestedEmits != 3 {
		t.Errorf("sink.Sink spans nested under a transition = %d, want 3", nestedEmits)
	}
}
