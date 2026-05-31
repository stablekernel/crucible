package state_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// captureHandler is a minimal slog.Handler that records every Record it is
// asked to handle, so a test can assert what an instance logged through the
// WithLogger seam without parsing formatted output.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// attrs collapses a record's attributes to a map for assertion.
func attrs(r slog.Record) map[string]string {
	out := map[string]string{}
	r.Attrs(func(a slog.Attr) bool {
		out[a.Key] = a.Value.String()
		return true
	})
	return out
}

// TestWithLogger_NoopByDefault asserts an instance cast without WithLogger never
// logs: the seam is off by default, so an un-logged Fire performs no logging IO.
func TestWithLogger_NoopByDefault(t *testing.T) {
	h := &captureHandler{}
	// Install the handler as the process default to prove the kernel does not
	// reach for slog.Default() when no logger is wired.
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })

	m := buildDocMachine()
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	inst := m.Cast(doc) // no WithLogger

	inst.Fire(context.Background(), Submit)

	h.mu.Lock()
	got := len(h.records)
	h.mu.Unlock()
	if got != 0 {
		t.Fatalf("un-logged instance wrote %d records, want 0", got)
	}
}

// TestWithLogger_WiredLogsEachFire asserts a provided logger is used: each Fire
// writes one fixed-shape record carrying the machine, event, from/to leaves, and
// outcome.
func TestWithLogger_WiredLogsEachFire(t *testing.T) {
	h := &captureHandler{}
	logger := slog.New(h)

	m := buildDocMachine()
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	inst := m.Cast(doc, state.WithLogger[DocState](logger))

	inst.Fire(context.Background(), Submit)

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) != 1 {
		t.Fatalf("records = %d, want 1", len(h.records))
	}
	rec := h.records[0]
	if rec.Level != slog.LevelDebug {
		t.Fatalf("level = %v, want Debug", rec.Level)
	}
	a := attrs(rec)
	if a["event"] != "Submit" {
		t.Fatalf("event attr = %q, want Submit", a["event"])
	}
	if a["from"] != "Draft" || a["to"] != "Submitted" {
		t.Fatalf("from/to = %q/%q, want Draft/Submitted", a["from"], a["to"])
	}
	if a["outcome"] != "success" {
		t.Fatalf("outcome = %q, want success", a["outcome"])
	}
	if a["machine"] == "" {
		t.Fatal("machine attr was empty")
	}
}

// TestWithLogger_FailingOutcomeCarriesErr asserts a Fire that does not match a
// transition logs the failure outcome and the error message under "err", so the
// host's ordinary logs reflect a rejected event.
func TestWithLogger_FailingOutcomeCarriesErr(t *testing.T) {
	h := &captureHandler{}
	m := buildDocMachine()
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	inst := m.Cast(doc, state.WithLogger[DocState](slog.New(h)))

	// Approve is not a valid event from Draft, so the Fire fails with an
	// invalid-transition outcome.
	inst.Fire(context.Background(), Approve)

	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.records) != 1 {
		t.Fatalf("records = %d, want 1", len(h.records))
	}
	a := attrs(h.records[0])
	if a["outcome"] != "invalidTransition" {
		t.Fatalf("outcome = %q, want invalidTransition", a["outcome"])
	}
	if a["err"] == "" {
		t.Fatal("failing Fire logged no err attr")
	}
}

// TestWithLogger_IndependentOfInspector asserts the logger and the Inspector are
// independent seams: wiring both feeds each its own stream from the same Fire.
func TestWithLogger_IndependentOfInspector(t *testing.T) {
	h := &captureHandler{}
	insp := &recordingInspector{}
	m := buildDocMachine()
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	inst := m.Cast(doc,
		state.WithLogger[DocState](slog.New(h)),
		state.WithInspector[DocState](insp),
	)

	inst.Fire(context.Background(), Submit)

	h.mu.Lock()
	logs := len(h.records)
	h.mu.Unlock()
	if logs != 1 {
		t.Fatalf("logger records = %d, want 1", logs)
	}
	if len(insp.ofKind(state.InspectTransition)) != 1 {
		t.Fatalf("inspector transition events = %d, want 1",
			len(insp.ofKind(state.InspectTransition)))
	}
}
