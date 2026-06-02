// SPDX-License-Identifier: Apache-2.0

package bridge_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/bridge"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/telemetry"
)

// --- a minimal light-switch machine -----------------------------------------

type bulb struct{ on bool }

func (b *bulb) cur() string {
	if b.on {
		return "on"
	}
	return "off"
}

func switchMachine(mw ...state.Middleware[string, string, *bulb]) *state.Machine[string, string, *bulb] {
	return state.Forge[string, string, *bulb]("switch").
		Use(mw...).
		State("off").
		State("on").
		Initial("off").
		CurrentStateFn(func(b *bulb) string { return b.cur() }).
		Transition("off").On("toggle").GoTo("on").
		Transition("on").On("toggle").GoTo("off").
		Quench(state.Strict())
}

// --- a context-threading tracer that records span parentage ------------------

type ctxKey struct{}

type recSpan struct{}

func (recSpan) SetAttributes(...telemetry.Attr)        {}
func (recSpan) RecordError(error)                      {}
func (recSpan) SetStatus(telemetry.StatusCode, string) {}
func (recSpan) End()                                   {}

type span struct {
	name   string
	parent string
}

type recTracer struct {
	mu    sync.Mutex
	spans []span
	n     int
}

func (tr *recTracer) Start(ctx context.Context, name string, _ ...telemetry.Attr) (context.Context, telemetry.Span) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.n++
	id := fmt.Sprint(tr.n)
	parent, _ := ctx.Value(ctxKey{}).(string)
	tr.spans = append(tr.spans, span{name: name, parent: parent})
	return context.WithValue(ctx, ctxKey{}, id), recSpan{}
}

func (tr *recTracer) find(name string) (span, bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for _, s := range tr.spans {
		if s.name == name {
			return s, true
		}
	}
	return span{}, false
}

func (tr *recTracer) idOf(name string) string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	for i, s := range tr.spans {
		if s.name == name {
			return fmt.Sprint(i + 1)
		}
	}
	return ""
}

func TestMiddlewareFansTransitionOut(t *testing.T) {
	t.Parallel()

	bucket := csink.NewBucket()
	m := csink.NewManifold(csink.WithOutlets(bucket))
	machine := switchMachine(bridge.Middleware[string, string, *bulb](m))

	res := machine.Cast(&bulb{}).Fire(context.Background(), "toggle")
	if res.Err != nil {
		t.Fatalf("Fire error = %v", res.Err)
	}

	got := csink.RecordsOf[bridge.Transition](bucket)
	if len(got) != 1 {
		t.Fatalf("manifold received %d transitions, want 1", len(got))
	}
	want := bridge.Transition{Machine: "switch", Event: "toggle", From: "off", To: "on"}
	if got[0] != want {
		t.Fatalf("transition = %+v, want %+v", got[0], want)
	}
}

func TestMiddlewareNestsEmitSpanUnderTransitionSpan(t *testing.T) {
	t.Parallel()

	tr := &recTracer{}
	// The Manifold and the bridge share the same tracer, so the emit span's
	// parent is read from the context the middleware propagated.
	m := csink.NewManifold(csink.WithTracer(tr), csink.WithOutlets(csink.NewBucket()))
	machine := switchMachine(bridge.Middleware[string, string, *bulb](m, bridge.WithTracer(tr)))

	machine.Cast(&bulb{}).Fire(context.Background(), "toggle")

	transition, ok := tr.find("state.transition")
	if !ok {
		t.Fatal("no state.transition span started")
	}
	if transition.parent != "" {
		t.Errorf("state.transition parent = %q, want root", transition.parent)
	}
	emit, ok := tr.find("sink.Sink")
	if !ok {
		t.Fatal("no sink.Sink span started")
	}
	if emit.parent != tr.idOf("state.transition") {
		t.Errorf("sink.Sink parent = %q, want it nested under state.transition (%q)",
			emit.parent, tr.idOf("state.transition"))
	}
}

func TestInspectorFansTransitionOut(t *testing.T) {
	t.Parallel()

	bucket := csink.NewBucket()
	m := csink.NewManifold(csink.WithOutlets(bucket))
	machine := switchMachine() // no middleware

	machine.Cast(&bulb{}, state.WithInspector[string](bridge.Inspector(m))).
		Fire(context.Background(), "toggle")

	got := csink.RecordsOf[bridge.Transition](bucket)
	if len(got) != 1 {
		t.Fatalf("manifold received %d transitions, want 1", len(got))
	}
	if got[0].From != "off" || got[0].To != "on" || got[0].Event != "toggle" {
		t.Fatalf("transition = %+v, want off->on on toggle", got[0])
	}
}
