// SPDX-License-Identifier: Apache-2.0

//go:build integration

package slog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	slogsink "github.com/stablekernel/crucible/sink/slog"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string
}

// TestIntegrationSinkLandsRecordInRealHandler drives the real Outlet path
// through a real slog.Logger backed by a JSON handler writing to a buffer, then
// decodes the emitted record to prove the structured log landed.
func TestIntegrationSinkLandsRecordInRealHandler(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	reg := slogsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[*slog.Logger] {
		return slogsink.Info("order placed", slog.String("order_id", o.ID))
	})

	outlet := slogsink.New(logger, reg)
	if err := outlet.Sink(context.Background(), orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("decode log record error = %v (raw %q)", err, buf.String())
	}
	if rec["msg"] != "order placed" {
		t.Errorf("msg = %v, want %q", rec["msg"], "order placed")
	}
	if rec["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", rec["level"])
	}
	if rec["order_id"] != "A-1" {
		t.Errorf("order_id = %v, want A-1", rec["order_id"])
	}
}
