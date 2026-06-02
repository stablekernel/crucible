// SPDX-License-Identifier: Apache-2.0

package slog_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/sinktest"
	slogsink "github.com/stablekernel/crucible/sink/slog"
)

// captureHandler is a hand-rolled slog.Handler that records every record it
// receives. It is not a replacement for a real handler — just enough to assert
// that Log/Info/Error emit what callers expect.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Clone the record so stored attrs are not aliased by future appends.
	clone := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		clone.AddAttrs(a)
		return true
	})
	h.records = append(h.records, clone)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Minimal implementation sufficient for tests.
	_ = attrs
	return h
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	_ = name
	return h
}

func (h *captureHandler) all() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]slog.Record, len(h.records))
	copy(out, h.records)
	return out
}

// newCapture returns a *slog.Logger backed by captureHandler and the handler
// itself so tests can inspect emitted records.
func newCapture() (*slog.Logger, *captureHandler) {
	h := &captureHandler{}
	return slog.New(h), h
}

type orderShipped struct{ ID string }

func newOutlet(logger *slog.Logger) csink.Outlet {
	reg := slogsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderShipped) csink.Op[*slog.Logger] {
		return slogsink.Log(
			slog.LevelInfo, "order.shipped",
			slog.String("order_id", o.ID),
		)
	})
	return slogsink.New(logger, reg)
}

func TestLog_EmitsRecord(t *testing.T) {
	t.Parallel()

	logger, h := newCapture()
	if err := newOutlet(logger).Sink(context.Background(), orderShipped{ID: "O-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	recs := h.all()
	if len(recs) != 1 {
		t.Fatalf("captured %d records, want 1", len(recs))
	}
	r := recs[0]
	if r.Level != slog.LevelInfo {
		t.Errorf("Level = %v, want %v", r.Level, slog.LevelInfo)
	}
	if r.Message != "order.shipped" {
		t.Errorf("Message = %q, want %q", r.Message, "order.shipped")
	}

	var got []slog.Attr
	r.Attrs(func(a slog.Attr) bool {
		got = append(got, a)
		return true
	})
	want := slog.String("order_id", "O-1")
	if !slices.ContainsFunc(got, func(a slog.Attr) bool {
		return a.Key == want.Key && a.Value.String() == want.Value.String()
	}) {
		t.Errorf("attrs = %v, want to contain %v", got, want)
	}
}

func TestInfo_UsesInfoLevel(t *testing.T) {
	t.Parallel()

	logger, h := newCapture()
	logger2 := slog.New(h)
	_ = logger2

	op := slogsink.Info("hello", slog.Int("n", 42))
	if err := op.Apply(context.Background(), logger); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	recs := h.all()
	if len(recs) != 1 {
		t.Fatalf("captured %d records, want 1", len(recs))
	}
	if recs[0].Level != slog.LevelInfo {
		t.Errorf("Level = %v, want %v", recs[0].Level, slog.LevelInfo)
	}
	if recs[0].Message != "hello" {
		t.Errorf("Message = %q, want %q", recs[0].Message, "hello")
	}
}

func TestError_UsesErrorLevel(t *testing.T) {
	t.Parallel()

	logger, h := newCapture()
	op := slogsink.Error("something failed", slog.String("reason", "timeout"))
	if err := op.Apply(context.Background(), logger); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	recs := h.all()
	if len(recs) != 1 {
		t.Fatalf("captured %d records, want 1", len(recs))
	}
	if recs[0].Level != slog.LevelError {
		t.Errorf("Level = %v, want %v", recs[0].Level, slog.LevelError)
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	logger, _ := newCapture()
	type unknown struct{}
	err := newOutlet(logger).Sink(context.Background(), unknown{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestLog_ApplyError_Wrapped(t *testing.T) {
	t.Parallel()

	// slog.Logger.LogAttrs never errors, so we exercise the error-wrapping path
	// by confirming a conforming Op returning nil propagates cleanly. We verify
	// the *sink.Error shape via a custom OpFunc that returns an error.
	boom := errors.New("handler rejected record")
	logger, _ := newCapture()
	reg := slogsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, _ orderShipped) csink.Op[*slog.Logger] {
		return csink.OpFunc[*slog.Logger](func(_ context.Context, _ *slog.Logger) error {
			return boom
		})
	})
	outlet := slogsink.New(logger, reg)
	err := outlet.Sink(context.Background(), orderShipped{ID: "X"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "slog" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:slog, Phase:apply}", se)
	}
}

func TestLog_MultipleAttrs(t *testing.T) {
	t.Parallel()

	logger, h := newCapture()
	op := slogsink.Log(
		slog.LevelWarn, "multi",
		slog.String("a", "1"),
		slog.String("b", "2"),
		slog.Int("c", 3),
	)
	if err := op.Apply(context.Background(), logger); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	recs := h.all()
	if len(recs) != 1 {
		t.Fatalf("captured %d records, want 1", len(recs))
	}
	if recs[0].Level != slog.LevelWarn {
		t.Errorf("Level = %v, want Warn", recs[0].Level)
	}
	var keys []string
	recs[0].Attrs(func(a slog.Attr) bool {
		keys = append(keys, a.Key)
		return true
	})
	for _, k := range []string{"a", "b", "c"} {
		if !slices.Contains(keys, k) {
			t.Errorf("attr %q missing from record; got keys %v", k, keys)
		}
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	logger, _ := newCapture()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(logger) })
}
