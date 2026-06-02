package e2e_test

import (
	"context"
	"sync"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/bridge"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/telemetry"
)

// ---- state ⊗ sink ⊗ bridge ⊗ telemetry: transitions fan out to many sinks ----
//
// The state kernel imports no IO and the sink core imports no state; the optional
// bridge composes them. This test wires a machine's transitions out through a
// sink Manifold and confirms both that every outlet receives every transition
// and that the emit span nests under the transition span via one shared
// telemetry tracer — the cross-module seam the per-module tests cannot reach.

type fulfillment struct{ stage string }

// ctxSpanKey threads the recording tracer's current span id through the context
// so a child Start can record its parent.
type ctxSpanKey struct{}

type recSpan struct{}

func (recSpan) SetAttributes(...telemetry.Attr)        {}
func (recSpan) RecordError(error)                      {}
func (recSpan) SetStatus(telemetry.StatusCode, string) {}
func (recSpan) End()                                   {}

// recTracer records each span's name and its parent's name, so the test can
// assert the sink emit span nests beneath the bridge's transition span.
type recTracer struct {
	mu     sync.Mutex
	names  []string
	parent map[int]int // span index -> parent span index (-1 for root)
}

func newRecTracer() *recTracer { return &recTracer{parent: map[int]int{}} }

func (tr *recTracer) Start(ctx context.Context, name string, _ ...telemetry.Attr) (context.Context, telemetry.Span) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	id := len(tr.names)
	tr.names = append(tr.names, name)
	parent, ok := ctx.Value(ctxSpanKey{}).(int)
	if !ok {
		parent = -1
	}
	tr.parent[id] = parent
	return context.WithValue(ctx, ctxSpanKey{}, id), recSpan{}
}

// counts returns how many "state.transition" spans were started and how many
// "sink.Sink" spans nested directly beneath one.
func (tr *recTracer) counts() (transitions, nestedEmits int) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for id, name := range tr.names {
		switch name {
		case "state.transition":
			transitions++
		case "sink.Sink":
			if p := tr.parent[id]; p >= 0 && tr.names[p] == "state.transition" {
				nestedEmits++
			}
		}
	}
	return transitions, nestedEmits
}

func TestE2E_StateTransitionsFanOutThroughSink(t *testing.T) {
	tr := newRecTracer()
	analytics := csink.NewBucket()
	audit := csink.NewBucket()

	// One Manifold, one shared tracer, two emulated destinations.
	m := csink.NewManifold(csink.WithTracer(tr), csink.WithOutlets(analytics, audit))

	machine := state.Forge[string, string, *fulfillment]("fulfillment").
		Use(bridge.Middleware[string, string, *fulfillment](m, bridge.WithTracer(tr))).
		State("placed").State("packed").State("shipped").
		Initial("placed").
		CurrentStateFn(func(f *fulfillment) string { return f.stage }).
		Transition("placed").On("pack").GoTo("packed").
		Transition("packed").On("ship").GoTo("shipped").
		Quench(state.Strict())

	inst := machine.Cast(&fulfillment{stage: "placed"})
	for _, ev := range []string{"pack", "ship"} {
		if res := inst.Fire(context.Background(), ev); res.Err != nil {
			t.Fatalf("fire %q: %v", ev, res.Err)
		}
	}

	// Every outlet received every transition: the kernel's transitions reached
	// all destinations through one Manifold, neither core importing the other.
	for name, b := range map[string]*csink.Bucket{"analytics": analytics, "audit": audit} {
		got := csink.RecordsOf[bridge.Transition](b)
		if len(got) != 2 {
			t.Fatalf("%s received %d transitions, want 2", name, len(got))
		}
		if got[0].To != "packed" || got[1].To != "shipped" {
			t.Fatalf("%s transitions = %+v, want packed then shipped", name, got)
		}
	}

	// Each emit span nested under a transition span: one trace correlates the
	// state decision and its fan-out through the shared telemetry provider.
	transitions, nested := tr.counts()
	if transitions != 2 {
		t.Fatalf("state.transition spans = %d, want 2", transitions)
	}
	if nested != 2 {
		t.Fatalf("sink.Sink spans nested under a transition = %d, want 2", nested)
	}
}
