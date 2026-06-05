// SPDX-License-Identifier: Apache-2.0

package statsd_test

import (
	"context"
	"errors"
	"testing"
	"time"

	csink "github.com/stablekernel/crucible/sink"
	statsdsink "github.com/stablekernel/crucible/sink/statsd"
)

// TestWithName_AppearsInFlushError verifies WithName overrides the outlet name
// carried on the *sink.Error returned from a failing flush.
func TestWithName_AppearsInFlushError(t *testing.T) {
	t.Parallel()

	boom := errors.New("emit failed")
	fc := &fakeClient{err: boom}
	agg := statsdsink.NewAggregator(fc,
		statsdsink.WithName("custom-statsd"),
		statsdsink.WithInterval(0), // no background loop; Flush is the only emit
	)

	if err := agg.Sink(context.Background(), statsdsink.Metric{Type: statsdsink.TypeCount, Name: "n", Int: 1, Rate: 1}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	f, ok := agg.(csink.Flusher)
	if !ok {
		t.Fatal("Aggregator does not implement sink.Flusher")
	}
	err := f.Flush(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Flush() = %v, want to wrap %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Outlet != "custom-statsd" {
		t.Fatalf("error = %+v, want Outlet=custom-statsd", se)
	}
}

// TestWithName_EmptyIgnored verifies an empty name leaves the default in place.
func TestWithName_EmptyIgnored(t *testing.T) {
	t.Parallel()

	boom := errors.New("emit failed")
	fc := &fakeClient{err: boom}
	agg := statsdsink.NewAggregator(fc, statsdsink.WithName(""), statsdsink.WithInterval(0))
	_ = agg.Sink(context.Background(), statsdsink.Metric{Type: statsdsink.TypeCount, Name: "n", Int: 1, Rate: 1})

	err := agg.(csink.Flusher).Flush(context.Background())
	var se *csink.Error
	if !errors.As(err, &se) || se.Outlet != "statsd" {
		t.Fatalf("error = %+v, want default Outlet=statsd", se)
	}
}

// TestWithClock_NonNilAcceptedNilIgnored verifies WithClock installs a non-nil
// clock and ignores a nil one. The aggregator stays usable in both cases.
func TestWithClock_NonNilAcceptedNilIgnored(t *testing.T) {
	t.Parallel()

	fixed := time.Unix(42, 0)
	clock := func() time.Time { return fixed }

	for _, tc := range []struct {
		name string
		now  func() time.Time
	}{
		{"non-nil clock", clock},
		{"nil clock falls back to default", nil},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := &fakeClient{}
			agg := statsdsink.NewAggregator(fc, statsdsink.WithClock(tc.now), statsdsink.WithInterval(0))
			if err := agg.Sink(context.Background(), statsdsink.Metric{Type: statsdsink.TypeGauge, Name: "g", Value: 1, Rate: 1}); err != nil {
				t.Fatalf("Sink() error = %v", err)
			}
			if err := agg.(csink.Flusher).Flush(context.Background()); err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			if got := len(fc.snapshot()); got != 1 {
				t.Fatalf("emitted %d metrics, want 1", got)
			}
		})
	}
}

// TestDial_ReturnsUsableClient verifies Dial constructs a Client over a UDP
// address without requiring a live StatsD server. The datadog SDK resolves the
// address lazily, so a valid host:port yields a ready client.
func TestDial_ReturnsUsableClient(t *testing.T) {
	t.Parallel()

	c, err := statsdsink.Dial("127.0.0.1:8125")
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	if c == nil {
		t.Fatal("Dial() returned a nil Client")
	}
	if err := c.Count("dial.test", 1, nil, 1); err != nil {
		t.Fatalf("Count() on dialed client error = %v", err)
	}
}
