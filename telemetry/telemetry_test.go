package telemetry_test

import (
	"context"
	"errors"
	"go/build"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/telemetry"
)

// TestImportGraph_StdlibOnly enforces telemetry's stdlib-only boundary: the
// core package must import nothing outside the Go standard library. This is the
// whole point of the module — the vendor-neutral interface forces no dependency
// on consumers.
func TestImportGraph_StdlibOnly(t *testing.T) {
	pkg, err := build.ImportDir(".", 0)
	if err != nil {
		t.Fatalf("ImportDir: %v", err)
	}
	for _, imp := range pkg.Imports {
		first := imp
		if idx := strings.IndexByte(imp, '/'); idx >= 0 {
			first = imp[:idx]
		}
		if strings.Contains(first, ".") {
			t.Errorf("non-stdlib import in telemetry core: %q", imp)
		}
	}
}

// TestNopTracer_RecordsNothingNeverPanics exercises every Span method on a
// no-op tracer, including after End, to prove it never panics and is nil-safe.
func TestNopTracer_RecordsNothingNeverPanics(t *testing.T) {
	tr := telemetry.NopTracer()
	ctx, span := tr.Start(context.Background(), "op", telemetry.Int64("k", 1))
	if ctx == nil {
		t.Fatal("Start returned nil context")
	}
	if span == nil {
		t.Fatal("Start returned nil span")
	}
	// All of these must be no-ops, including the nil error and post-End calls.
	span.SetAttributes(telemetry.String("a", "b"))
	span.RecordError(nil)
	span.RecordError(errors.New("boom"))
	span.SetStatus(telemetry.StatusError, "failed")
	span.End()
	span.End() // double End must be safe.
	span.SetAttributes(telemetry.Bool("after", true))
	span.RecordError(errors.New("after end"))
	span.SetStatus(telemetry.StatusOK, "")
}

// TestNopTracer_ContextPassthrough asserts the no-op tracer returns the same
// context it was given, so propagation is a true pass-through.
func TestNopTracer_ContextPassthrough(t *testing.T) {
	type key struct{}
	in := context.WithValue(context.Background(), key{}, "v")
	out, span := telemetry.NopTracer().Start(in, "op")
	defer span.End()
	if out.Value(key{}) != "v" {
		t.Fatal("no-op tracer did not pass the context through")
	}
}

// TestNopMeter_InstrumentsNeverPanic exercises every instrument from a no-op
// meter.
func TestNopMeter_InstrumentsNeverPanic(t *testing.T) {
	mt := telemetry.NopMeter()
	ctx := context.Background()

	c := mt.Counter("c", telemetry.WithUnit("{record}"), telemetry.WithDescription("count"))
	c.Add(ctx, 0)
	c.Add(ctx, 5, telemetry.String("outcome", "ok"))

	h := mt.Histogram("h", telemetry.WithUnit("ms"))
	h.Record(ctx, 12.5, telemetry.Int64("size", 100))

	g := mt.Gauge("g")
	g.Record(ctx, 42)
}

// TestResolveInstrument applies the instrument options and confirms the
// resolved metadata is exposed to adapters.
func TestResolveInstrument(t *testing.T) {
	got := telemetry.ResolveInstrument(
		telemetry.WithUnit("By"),
		telemetry.WithDescription("payload bytes"),
	)
	if got.Unit != "By" {
		t.Errorf("Unit = %q, want %q", got.Unit, "By")
	}
	if got.Description != "payload bytes" {
		t.Errorf("Description = %q, want %q", got.Description, "payload bytes")
	}

	empty := telemetry.ResolveInstrument()
	if empty.Unit != "" || empty.Description != "" {
		t.Errorf("empty ResolveInstrument = %+v, want zero", empty)
	}
}

// TestProvider_NopDefault confirms Nop wires non-nil no-op implementations.
func TestProvider_NopDefault(t *testing.T) {
	p := telemetry.Nop()
	if p.Tracer == nil || p.Meter == nil {
		t.Fatal("Nop returned a Provider with nil members")
	}
	// They must behave (no panic).
	_, span := p.Tracer.Start(context.Background(), "op")
	span.End()
	p.Meter.Counter("c").Add(context.Background(), 1)
}

// stubTracer / stubMeter are minimal interface implementations used to prove
// the option wiring swaps the defaults.
type stubTracer struct{ telemetry.Tracer }

type stubMeter struct{ telemetry.Meter }

// TestProvider_Apply confirms options override the defaults and that a nil
// argument preserves the no-op default rather than nilling the field.
func TestProvider_Apply(t *testing.T) {
	base := telemetry.Nop()
	tr := stubTracer{Tracer: telemetry.NopTracer()}
	mt := stubMeter{Meter: telemetry.NopMeter()}

	got := base.Apply(telemetry.WithTracer(tr), telemetry.WithMeter(mt))
	if _, ok := got.Tracer.(stubTracer); !ok {
		t.Errorf("Tracer not overridden: %T", got.Tracer)
	}
	if _, ok := got.Meter.(stubMeter); !ok {
		t.Errorf("Meter not overridden: %T", got.Meter)
	}

	// Apply must not mutate the receiver.
	if _, ok := base.Tracer.(stubTracer); ok {
		t.Error("Apply mutated the receiver's Tracer")
	}

	// nil options keep the existing (no-op) value.
	keep := telemetry.Nop().Apply(telemetry.WithTracer(nil), telemetry.WithMeter(nil))
	if keep.Tracer == nil || keep.Meter == nil {
		t.Error("nil option nilled a Provider field")
	}
}

// TestProvider_ApplyZeroValueSelfHeals confirms Apply repairs a zero-value
// Provider to no-op implementations before applying options.
func TestProvider_ApplyZeroValueSelfHeals(t *testing.T) {
	var zero telemetry.Provider
	got := zero.Apply()
	if got.Tracer == nil || got.Meter == nil {
		t.Fatal("Apply on a zero Provider left nil members")
	}
	_, span := got.Tracer.Start(context.Background(), "op")
	span.End()
}
