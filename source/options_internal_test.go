// SPDX-License-Identifier: Apache-2.0

package source

import (
	"log/slog"
	"testing"

	"github.com/stablekernel/crucible/telemetry"
)

func TestDefaultConfigHasNoOpSeams(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	if cfg.logger == nil {
		t.Error("default logger is nil")
	}
	if cfg.tracer == nil {
		t.Error("default tracer is nil")
	}
	if cfg.meter == nil {
		t.Error("default meter is nil")
	}
	if cfg.concurrency != 1 {
		t.Errorf("default concurrency = %d, want 1", cfg.concurrency)
	}
	if cfg.maxInFlight != 0 {
		t.Errorf("default maxInFlight = %d, want 0 (unbounded)", cfg.maxInFlight)
	}
	if cfg.registry != nil {
		t.Error("default registry should be nil (raw message passthrough)")
	}
}

func TestWithOptionsApply(t *testing.T) {
	t.Parallel()
	custom := slog.New(slog.DiscardHandler)
	reg := NewRegistry()
	mw := func(h Handler) Handler { return h }

	cfg := defaultConfig()
	for _, o := range []Option{
		WithName("ingest"),
		WithLogger(custom),
		WithTracer(telemetry.NopTracer()),
		WithMeter(telemetry.NopMeter()),
		WithRegistry(reg),
		WithMiddleware(mw, mw),
		WithConcurrency(8),
		WithMaxInFlight(64),
	} {
		o(&cfg)
	}
	if cfg.name != "ingest" {
		t.Errorf("name = %q, want ingest", cfg.name)
	}
	if cfg.logger != custom {
		t.Error("WithLogger did not set the logger")
	}
	if cfg.registry != reg {
		t.Error("WithRegistry did not set the registry")
	}
	if len(cfg.middleware) != 2 {
		t.Errorf("middleware count = %d, want 2", len(cfg.middleware))
	}
	if cfg.concurrency != 8 {
		t.Errorf("concurrency = %d, want 8", cfg.concurrency)
	}
	if cfg.maxInFlight != 64 {
		t.Errorf("maxInFlight = %d, want 64", cfg.maxInFlight)
	}
}

func TestWithCodecBuildsRegistry(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	WithCodec(NewJSONCodec[order]())(&cfg)
	if cfg.registry == nil {
		t.Fatal("WithCodec should build a default registry")
	}
	if _, err := cfg.registry.Decode(testRawMsg{value: []byte(`{"id":"x"}`)}); err != nil {
		t.Fatalf("registry from WithCodec failed to decode: %v", err)
	}
}

func TestWithInvalidOptionsIgnored(t *testing.T) {
	t.Parallel()
	cfg := defaultConfig()
	orig := cfg.logger

	WithName("")(&cfg)
	WithLogger(nil)(&cfg)
	WithTracer(nil)(&cfg)
	WithMeter(nil)(&cfg)
	WithCodec(nil)(&cfg)
	WithRegistry(nil)(&cfg)
	WithMiddleware(nil)(&cfg)
	WithConcurrency(0)(&cfg)
	WithConcurrency(-5)(&cfg)
	WithMaxInFlight(-1)(&cfg)

	if cfg.name != "hopper" {
		t.Errorf("name = %q, want default hopper", cfg.name)
	}
	if cfg.logger != orig {
		t.Error("WithLogger(nil) replaced the default logger")
	}
	if cfg.registry != nil {
		t.Error("nil codec/registry should leave registry nil")
	}
	if len(cfg.middleware) != 0 {
		t.Error("nil middleware should be skipped")
	}
	if cfg.concurrency != 1 {
		t.Errorf("concurrency = %d, want default 1", cfg.concurrency)
	}
	if cfg.maxInFlight != 0 {
		t.Errorf("maxInFlight = %d, want default 0", cfg.maxInFlight)
	}
}

// order and testRawMsg are local to the internal test package.
type order struct {
	ID  string `json:"id"`
	Qty int    `json:"qty"`
}

type testRawMsg struct {
	value []byte
}

func (m testRawMsg) Key() []byte          { return nil }
func (m testRawMsg) Value() []byte        { return m.value }
func (m testRawMsg) Headers() Headers     { return nil }
func (m testRawMsg) Subject() string      { return "" }
func (m testRawMsg) PartitionKey() string { return "" }
func (m testRawMsg) Cursor() Cursor       { return nil }
func (m testRawMsg) As(any) bool          { return false }
