package slogadapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/telemetry"
	"github.com/stablekernel/crucible/telemetry/slogadapter"
)

// newCapture builds a logger that writes JSON records to a buffer at debug
// level (so span/metric debug records are captured), plus deterministic clock
// and id options for stable assertions.
func newCapture() (*bytes.Buffer, []slogadapter.Option) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(h)

	var ids atomic.Uint64
	base := time.Unix(0, 0).UTC()
	var ticks atomic.Int64
	clock := func() time.Time {
		// Each call advances 1ms so span elapsed is deterministic and non-zero.
		return base.Add(time.Duration(ticks.Add(1)) * time.Millisecond)
	}
	return &buf, []slogadapter.Option{
		slogadapter.WithLogger(logger),
		slogadapter.WithClock(clock),
		slogadapter.WithIDFn(func() uint64 { return ids.Add(1) }),
	}
}

// records parses the buffer into one map per JSON log line.
func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("unmarshal %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func group(m map[string]any, name string) map[string]any {
	g, _ := m[name].(map[string]any)
	return g
}

// TestTracer_EmitsSpanLifecycle asserts start/attributes/error/end records carry
// the expected fields, including status and a non-zero elapsed duration.
func TestTracer_EmitsSpanLifecycle(t *testing.T) {
	buf, opts := newCapture()
	tr := slogadapter.NewTracer(opts...)

	_, span := tr.Start(context.Background(), "sink.Sink", telemetry.String("payload.type", "Order"))
	span.SetAttributes(telemetry.String("outlet", "dynamo"))
	span.RecordError(errors.New("boom"))
	span.SetStatus(telemetry.StatusError, "outlet failed")
	span.End()

	recs := records(t, buf)
	byMsg := map[string]map[string]any{}
	for _, r := range recs {
		byMsg[r["msg"].(string)] = r
	}

	start := group(byMsg["span.start"], "span")
	if start == nil || start["name"] != "sink.Sink" {
		t.Fatalf("span.start missing/wrong: %+v", byMsg["span.start"])
	}
	if attrs := group(start, "attrs"); attrs == nil || attrs["payload.type"] != "Order" {
		t.Errorf("span.start attrs wrong: %+v", start)
	}

	if byMsg["span.error"] == nil {
		t.Error("missing span.error record")
	} else if byMsg["span.error"]["level"] != "ERROR" {
		t.Errorf("span.error not at ERROR level: %v", byMsg["span.error"]["level"])
	}

	end := group(byMsg["span.end"], "span")
	if end == nil {
		t.Fatal("missing span.end record")
	}
	if end["status"] != "error" {
		t.Errorf("status = %v, want error", end["status"])
	}
	if end["status_msg"] != "outlet failed" {
		t.Errorf("status_msg = %v, want 'outlet failed'", end["status_msg"])
	}
	if end["elapsed"] == nil {
		t.Error("span.end missing elapsed")
	}
}

// TestTracer_ContextParentage asserts a child span started from the returned
// context logs the parent's id, reproducing span parentage in the logs.
func TestTracer_ContextParentage(t *testing.T) {
	buf, opts := newCapture()
	tr := slogadapter.NewTracer(opts...)

	ctx, parent := tr.Start(context.Background(), "state.transition")
	_, child := tr.Start(ctx, "sink.Sink")
	child.End()
	parent.End()

	var childStart map[string]any
	for _, r := range records(t, buf) {
		if r["msg"] == "span.start" {
			if g := group(r, "span"); g["name"] == "sink.Sink" {
				childStart = g
			}
		}
	}
	if childStart == nil {
		t.Fatal("no child span.start")
	}
	if childStart["parent"] == nil {
		t.Fatalf("child span missing parent id: %+v", childStart)
	}
	// parent span id is 1, child id is 2.
	if childStart["parent"].(float64) != 1 {
		t.Errorf("parent id = %v, want 1", childStart["parent"])
	}
	if childStart["id"].(float64) != 2 {
		t.Errorf("child id = %v, want 2", childStart["id"])
	}
}

// TestMeter_EmitsInstruments asserts counter/histogram/gauge records carry name,
// kind, value, unit, and attributes.
func TestMeter_EmitsInstruments(t *testing.T) {
	buf, opts := newCapture()
	mt := slogadapter.NewMeter(opts...)
	ctx := context.Background()

	mt.Counter("sink.sunk", telemetry.WithDescription("records sunk")).
		Add(ctx, 3, telemetry.String("outlet", "dynamo"))
	mt.Histogram("sink.flush_latency_ms", telemetry.WithUnit("ms")).
		Record(ctx, 12.5)
	mt.Gauge("state.in_state").Record(ctx, 7)

	var counter, histo, gauge map[string]any
	for _, r := range records(t, buf) {
		g := group(r, "metric")
		switch g["kind"] {
		case "counter":
			counter = g
		case "histogram":
			histo = g
		case "gauge":
			gauge = g
		}
	}

	if counter == nil || counter["name"] != "sink.sunk" || counter["value"].(float64) != 3 {
		t.Errorf("counter wrong: %+v", counter)
	}
	if counter["description"] != "records sunk" {
		t.Errorf("counter description = %v", counter["description"])
	}
	if a := group(counter, "attrs"); a == nil || a["outlet"] != "dynamo" {
		t.Errorf("counter attrs wrong: %+v", counter)
	}
	if histo == nil || histo["unit"] != "ms" || histo["value"].(float64) != 12.5 {
		t.Errorf("histogram wrong: %+v", histo)
	}
	if gauge == nil || gauge["value"].(float64) != 7 {
		t.Errorf("gauge wrong: %+v", gauge)
	}
}

// TestDefaults_Silent confirms a zero-option adapter discards everything (no
// panic, no output) — the no-op-by-default posture.
func TestDefaults_Silent(t *testing.T) {
	tr := slogadapter.NewTracer()
	_, span := tr.Start(context.Background(), "op")
	span.SetAttributes(telemetry.Int64("k", 1))
	span.RecordError(errors.New("x"))
	span.SetStatus(telemetry.StatusOK, "")
	span.End()

	mt := slogadapter.NewMeter()
	mt.Counter("c").Add(context.Background(), 1)
	mt.Histogram("h").Record(context.Background(), 1)
	mt.Gauge("g").Record(context.Background(), 1)
}

// TestSpan_PostEndIsNoop confirms calls after End emit nothing further.
func TestSpan_PostEndIsNoop(t *testing.T) {
	buf, opts := newCapture()
	tr := slogadapter.NewTracer(opts...)
	_, span := tr.Start(context.Background(), "op")
	span.End()
	before := len(records(t, buf))

	span.End()
	span.SetAttributes(telemetry.Int64("k", 1))
	span.RecordError(errors.New("x"))
	span.SetStatus(telemetry.StatusError, "late")

	if after := len(records(t, buf)); after != before {
		t.Errorf("post-End calls emitted %d extra records", after-before)
	}
}
