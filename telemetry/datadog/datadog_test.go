package datadog_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	"github.com/stablekernel/crucible/telemetry"
	ddadapter "github.com/stablekernel/crucible/telemetry/datadog"
)

// metricCall captures one DogStatsD emission for assertion.
type metricCall struct {
	kind   string
	name   string
	ivalue int64
	fvalue float64
	tags   []string
	rate   float64
}

// fakeStatsd embeds NoOpClient (so it satisfies the full ClientInterface) and
// records the Count/Histogram/Gauge calls the adapter makes.
type fakeStatsd struct {
	statsd.NoOpClient
	calls []metricCall
}

func (f *fakeStatsd) Count(name string, value int64, tags []string, rate float64) error {
	f.calls = append(f.calls, metricCall{kind: "count", name: name, ivalue: value, tags: tags, rate: rate})
	return nil
}

func (f *fakeStatsd) Histogram(name string, value float64, tags []string, rate float64) error {
	f.calls = append(f.calls, metricCall{kind: "histogram", name: name, fvalue: value, tags: tags, rate: rate})
	return nil
}

func (f *fakeStatsd) Gauge(name string, value float64, tags []string, rate float64) error {
	f.calls = append(f.calls, metricCall{kind: "gauge", name: name, fvalue: value, tags: tags, rate: rate})
	return nil
}

// TestTracer_SpanLifecycle asserts a span is created with the right name and
// attribute tags and finishes successfully (no error tag) under mocktracer.
func TestTracer_SpanLifecycle(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	tr := ddadapter.NewTracer()
	_, span := tr.Start(context.Background(), "sink.Sink",
		telemetry.String("payload.type", "Order"),
		telemetry.Int64("count", 3),
		telemetry.Bool("flush", true),
		telemetry.Float64("ratio", 0.5),
	)
	span.SetAttributes(telemetry.String("outlet", "dynamo"))
	span.SetStatus(telemetry.StatusOK, "done")
	span.End()

	spans := mt.FinishedSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.OperationName() != "sink.Sink" {
		t.Errorf("name = %q, want sink.Sink", s.OperationName())
	}
	if s.Tag("payload.type") != "Order" {
		t.Errorf("payload.type = %v", s.Tag("payload.type"))
	}
	// mocktracer normalizes numeric tags to float64 and bool tags to their
	// string form, so assert against those normalized representations.
	if s.Tag("count") != float64(3) {
		t.Errorf("count = %v", s.Tag("count"))
	}
	if s.Tag("flush") != "true" {
		t.Errorf("flush = %v", s.Tag("flush"))
	}
	if s.Tag("ratio") != 0.5 {
		t.Errorf("ratio = %v", s.Tag("ratio"))
	}
	if s.Tag("outlet") != "dynamo" {
		t.Errorf("outlet = %v", s.Tag("outlet"))
	}
	if s.Tag("error.message") != nil {
		t.Errorf("expected no error.message tag on a successful span, got %v", s.Tag("error.message"))
	}
}

// TestTracer_RecordError asserts RecordError marks the finished span errored.
func TestTracer_RecordError(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	tr := ddadapter.NewTracer()
	_, span := tr.Start(context.Background(), "op")
	span.RecordError(errors.New("boom"))
	span.SetStatus(telemetry.StatusError, "failed")
	span.End()

	s := mt.FinishedSpans()[0]
	if s.Tag("error.message") != "boom" {
		t.Errorf("error.message = %v, want boom", s.Tag("error.message"))
	}
}

// TestTracer_StatusErrorWithoutRecordedError asserts SetStatus(Error) alone marks
// the span errored even when no error was recorded.
func TestTracer_StatusErrorWithoutRecordedError(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	tr := ddadapter.NewTracer()
	_, span := tr.Start(context.Background(), "op")
	span.SetStatus(telemetry.StatusError, "outlet failed")
	span.End()

	s := mt.FinishedSpans()[0]
	if s.Tag("error.message") != "outlet failed" {
		t.Errorf("error.message = %v, want 'outlet failed'", s.Tag("error.message"))
	}
}

// TestTracer_StatusOKClearsPriorError asserts that SetStatus(StatusOK) after a
// RecordError clears the recorded error so the span finishes without the
// Datadog error flag.
func TestTracer_StatusOKClearsPriorError(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	tr := ddadapter.NewTracer()
	_, span := tr.Start(context.Background(), "op")
	span.RecordError(errors.New("transient"))
	// Caller decides the operation ultimately succeeded.
	span.SetStatus(telemetry.StatusOK, "recovered")
	span.End()

	s := mt.FinishedSpans()[0]
	if s.Tag("error.message") != nil {
		t.Errorf("expected no error tag after StatusOK, got %v", s.Tag("error.message"))
	}
}

// TestTracer_WithSpanStarter asserts the injected spanStarter is called and its
// span is used instead of the default global tracer.
func TestTracer_WithSpanStarter(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	called := false
	starter := func(ctx context.Context, name string, opts ...tracer.StartSpanOption) (*tracer.Span, context.Context) {
		called = true
		return tracer.StartSpanFromContext(ctx, name, opts...)
	}
	tr := ddadapter.NewTracer(ddadapter.WithSpanStarter(starter))
	_, span := tr.Start(context.Background(), "op")
	span.End()

	if !called {
		t.Error("WithSpanStarter: custom starter was not called")
	}
	if len(mt.FinishedSpans()) != 1 {
		t.Fatalf("got %d finished spans, want 1", len(mt.FinishedSpans()))
	}
}

// TestAttrValue_Kinds exercises attrValue for duration, time, uint64, and any
// kinds so tag conversion is covered end to end via the span path. mocktracer
// normalizes many value types (duration → string, time → string, uint64 →
// float64), so assertions check that each tag is present and non-nil.
func TestAttrValue_Kinds(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	tr := ddadapter.NewTracer()
	_, span := tr.Start(context.Background(), "op",
		telemetry.Duration("elapsed", 1500*time.Millisecond),
		telemetry.Time("at", time.Unix(0, 0).UTC()),
		telemetry.Any("obj", struct{ X int }{1}),
		telemetry.Uint64("u", 42),
	)
	span.End()

	s := mt.FinishedSpans()[0]
	for _, key := range []string{"elapsed", "at", "obj", "u"} {
		if s.Tag(key) == nil {
			t.Errorf("tag %q missing from span", key)
		}
	}
}

// TestTracer_Parentage asserts a child span started from the returned context
// parents under the first span.
func TestTracer_Parentage(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	tr := ddadapter.NewTracer()
	ctx, parent := tr.Start(context.Background(), "state.transition")
	_, child := tr.Start(ctx, "sink.Sink")
	child.End()
	parent.End()

	var p, c *mocktracer.Span
	for _, s := range mt.FinishedSpans() {
		switch s.OperationName() {
		case "state.transition":
			p = s
		case "sink.Sink":
			c = s
		}
	}
	if p == nil || c == nil {
		t.Fatal("missing spans")
	}
	if c.ParentID() != p.SpanID() {
		t.Errorf("child parent = %d, want %d", c.ParentID(), p.SpanID())
	}
}

// TestSpan_PostEndIsNoop confirms calls after End have no effect.
func TestSpan_PostEndIsNoop(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	tr := ddadapter.NewTracer()
	_, span := tr.Start(context.Background(), "op")
	span.End()
	span.End() // second End is a no-op
	span.SetAttributes(telemetry.Int64("k", 1))
	span.RecordError(errors.New("late"))
	span.SetStatus(telemetry.StatusError, "late")

	if n := len(mt.FinishedSpans()); n != 1 {
		t.Fatalf("got %d finished spans, want 1", n)
	}
	if mt.FinishedSpans()[0].Tag("k") != nil {
		t.Error("post-End SetAttributes should not have applied")
	}
}

// TestMeter_Instruments asserts counter/histogram/gauge emit the right DogStatsD
// calls with converted tags and the configured sample rate.
func TestMeter_Instruments(t *testing.T) {
	fake := &fakeStatsd{}
	mt := ddadapter.NewMeter(fake)
	ctx := context.Background()

	mt.Counter("sink.sunk", telemetry.WithDescription("records sunk"), telemetry.WithUnit("{record}")).
		Add(ctx, 3, telemetry.String("outlet", "dynamo"), telemetry.Int64("shard", 7))
	mt.Histogram("sink.flush_latency_ms", telemetry.WithUnit("ms")).
		Record(ctx, 12.5, telemetry.Bool("primary", true))
	mt.Gauge("state.in_state").Record(ctx, 7, telemetry.Float64("load", 0.25))

	if len(fake.calls) != 3 {
		t.Fatalf("got %d calls, want 3", len(fake.calls))
	}

	c := fake.calls[0]
	if c.kind != "count" || c.name != "sink.sunk" || c.ivalue != 3 || c.rate != 1.0 {
		t.Errorf("counter call wrong: %+v", c)
	}
	if !hasTag(c.tags, "outlet:dynamo") || !hasTag(c.tags, "shard:7") {
		t.Errorf("counter tags = %v", c.tags)
	}

	h := fake.calls[1]
	if h.kind != "histogram" || h.name != "sink.flush_latency_ms" || h.fvalue != 12.5 {
		t.Errorf("histogram call wrong: %+v", h)
	}
	if !hasTag(h.tags, "primary:true") {
		t.Errorf("histogram tags = %v", h.tags)
	}

	g := fake.calls[2]
	if g.kind != "gauge" || g.name != "state.in_state" || g.fvalue != 7 {
		t.Errorf("gauge call wrong: %+v", g)
	}
	if !hasTag(g.tags, "load:0.25") {
		t.Errorf("gauge tags = %v", g.tags)
	}
}

// TestMeter_SampleRate asserts WithSampleRate is applied to emissions.
func TestMeter_SampleRate(t *testing.T) {
	fake := &fakeStatsd{}
	mt := ddadapter.NewMeter(fake, ddadapter.WithSampleRate(0.5))
	mt.Counter("c").Add(context.Background(), 1)

	if fake.calls[0].rate != 0.5 {
		t.Errorf("rate = %v, want 0.5", fake.calls[0].rate)
	}
}

// TestMeter_TagKinds exercises the duration/time tag conversions.
func TestMeter_TagKinds(t *testing.T) {
	fake := &fakeStatsd{}
	mt := ddadapter.NewMeter(fake)
	mt.Counter("c").Add(context.Background(), 1,
		telemetry.Duration("elapsed", 1500*time.Millisecond),
		telemetry.Time("at", time.Unix(0, 0).UTC()),
		telemetry.Any("obj", "literal"),
	)

	tags := fake.calls[0].tags
	if !hasTag(tags, "elapsed:1.5s") {
		t.Errorf("missing duration tag in %v", tags)
	}
	if !hasTagPrefix(tags, "at:") {
		t.Errorf("missing time tag in %v", tags)
	}
	if !hasTagPrefix(tags, "obj:") {
		t.Errorf("missing any tag in %v", tags)
	}
}

func hasTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

func hasTagPrefix(tags []string, prefix string) bool {
	for _, tag := range tags {
		if len(tag) >= len(prefix) && tag[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}
