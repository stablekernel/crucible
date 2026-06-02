// SPDX-License-Identifier: Apache-2.0

package sink

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
}

func TestWithOptionsApply(t *testing.T) {
	t.Parallel()

	custom := slog.New(slog.DiscardHandler)
	cfg := defaultConfig()
	for _, o := range []Option{
		WithLogger(custom),
		WithTracer(telemetry.NopTracer()),
		WithMeter(telemetry.NopMeter()),
		WithOutlets(NewBucket(), NewBucket()),
	} {
		o(&cfg)
	}
	if cfg.logger != custom {
		t.Error("WithLogger did not set the logger")
	}
	if len(cfg.outlets) != 2 {
		t.Errorf("WithOutlets set %d outlets, want 2", len(cfg.outlets))
	}
}

func TestWithNilOptionsIgnored(t *testing.T) {
	t.Parallel()

	cfg := defaultConfig()
	orig := cfg.logger
	WithLogger(nil)(&cfg)
	WithTracer(nil)(&cfg)
	WithMeter(nil)(&cfg)
	if cfg.logger != orig {
		t.Error("WithLogger(nil) replaced the default logger")
	}
}
