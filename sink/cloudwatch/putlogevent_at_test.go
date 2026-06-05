// SPDX-License-Identifier: Apache-2.0

package cloudwatch_test

import (
	"context"
	"testing"
	"time"

	cw "github.com/stablekernel/crucible/sink/cloudwatch"
)

// TestPutLogEventAt_StampsSuppliedTime verifies the clock-injectable variant
// stamps the event with the caller-supplied time rather than the wall clock, so
// the emitted timestamp is deterministic.
func TestPutLogEventAt_StampsSuppliedTime(t *testing.T) {
	t.Parallel()

	at := time.Unix(1700000000, 0) // a fixed, known instant
	c := &fakeClient{}
	op := cw.PutLogEventAt("/app/events", "app-stream", "deterministic", at)
	if err := op.Apply(context.Background(), c); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(c.calls) != 1 {
		t.Fatalf("PutLogEvents call count = %d, want 1", len(c.calls))
	}
	events := c.calls[0].LogEvents
	if len(events) != 1 {
		t.Fatalf("events count = %d, want 1", len(events))
	}
	if events[0].Timestamp == nil {
		t.Fatal("event timestamp is nil")
	}
	if got, want := *events[0].Timestamp, at.UnixMilli(); got != want {
		t.Fatalf("timestamp = %d, want %d (the supplied time in ms)", got, want)
	}
	if events[0].Message == nil || *events[0].Message != "deterministic" {
		t.Errorf("message = %v, want %q", events[0].Message, "deterministic")
	}
}

// TestPutLogEvent_DelegatesToPutLogEventAt verifies the convenience wrapper
// still emits exactly one event to the named group and stream.
func TestPutLogEvent_DelegatesToPutLogEventAt(t *testing.T) {
	t.Parallel()

	c := &fakeClient{}
	op := cw.PutLogEvent("/app/events", "app-stream", "now")
	if err := op.Apply(context.Background(), c); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if len(c.calls) != 1 || len(c.calls[0].LogEvents) != 1 {
		t.Fatalf("calls = %+v, want one call with one event", c.calls)
	}
	if c.calls[0].LogEvents[0].Timestamp == nil {
		t.Error("PutLogEvent should stamp a timestamp")
	}
}
