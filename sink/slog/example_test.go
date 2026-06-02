// SPDX-License-Identifier: Apache-2.0

package slog_test

import (
	"context"
	"log/slog"
	"os"
	"time"

	csink "github.com/stablekernel/crucible/sink"
	slogsink "github.com/stablekernel/crucible/sink/slog"
)

type paymentReceived struct{ Amount int }

// ExampleNew demonstrates wiring a *slog.Logger as a sink destination. The
// text handler writes to stdout with a fixed time so the output is
// deterministic.
func ExampleNew() {
	// Use a text handler with a fixed time replacer so the output is stable.
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{Key: slog.TimeKey, Value: slog.TimeValue(time.Time{})}
			}
			return a
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))

	reg := slogsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p paymentReceived) csink.Op[*slog.Logger] {
		return slogsink.Info("payment.received", slog.Int("amount_cents", p.Amount))
	})

	outlet := slogsink.New(logger, reg)
	_ = outlet.Sink(context.Background(), paymentReceived{Amount: 1099})
	// Output:
	// time=0001-01-01T00:00:00.000Z level=INFO msg=payment.received amount_cents=1099
}
