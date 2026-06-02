// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/telemetry"
)

// captureHandler is a slog.Handler that records every Record it handles, for
// asserting that the Manifold logs outlet failures.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) errorRecords() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Record
	for _, r := range h.records {
		if r.Level == slog.LevelError {
			out = append(out, r)
		}
	}
	return out
}

func TestManifoldLoggerObservesOutletFailure(t *testing.T) {
	t.Parallel()

	h := &captureHandler{}
	m := NewManifold(WithLogger(slog.New(h)))
	m.Attach(OutletFunc(func(context.Context, any) error { return errors.New("write failed") }))
	m.Sink(context.Background(), evt{N: 1})

	errs := h.errorRecords()
	if len(errs) != 1 {
		t.Fatalf("error records = %d, want 1", len(errs))
	}
	var sawType, sawErr bool
	errs[0].Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "payload_type":
			sawType = true
		case "error":
			sawErr = true
		}
		return true
	})
	if !sawType || !sawErr {
		t.Errorf("error record missing fields: payload_type=%v error=%v", sawType, sawErr)
	}
}

func TestManifoldUnregisteredProducesNoErrorLog(t *testing.T) {
	t.Parallel()

	h := &captureHandler{}
	m := NewManifold(WithLogger(slog.New(h)))
	m.Attach(OutletFunc(func(context.Context, any) error { return ErrUnregistered }))
	m.Sink(context.Background(), evt{N: 1})

	if got := len(h.errorRecords()); got != 0 {
		t.Fatalf("ErrUnregistered produced %d error records, want 0", got)
	}
}

func TestManifoldTelemetryRecordsSpanAndCounters(t *testing.T) {
	t.Parallel()

	tr := &fakeTracer{}
	fm := newFakeMeter()
	m := NewManifold(WithTracer(tr), WithMeter(fm))
	m.Attach(
		NewBucket(), // success
		OutletFunc(func(context.Context, any) error { return errors.New("boom") }), // failure
		OutletFunc(func(context.Context, any) error { return ErrUnregistered }),    // skip
	)
	m.Sink(context.Background(), evt{N: 1})

	span := tr.only()
	if span == nil {
		t.Fatal("no span started")
	}
	if span.name != "sink.Sink" {
		t.Errorf("span name = %q, want sink.Sink", span.name)
	}
	if !span.ended {
		t.Error("span not ended")
	}
	if span.errorCount() != 1 {
		t.Errorf("span RecordError count = %d, want 1", span.errorCount())
	}
	if span.status != telemetry.StatusError {
		t.Errorf("span status = %v, want StatusError", span.status)
	}
	if got := fm.counterValue("sink.sunk"); got != 1 {
		t.Errorf("sink.sunk = %d, want 1", got)
	}
	if got := fm.counterValue("sink.failed"); got != 1 {
		t.Errorf("sink.failed = %d, want 1", got)
	}
	if got := fm.counterValue("sink.skipped"); got != 1 {
		t.Errorf("sink.skipped = %d, want 1", got)
	}
}
