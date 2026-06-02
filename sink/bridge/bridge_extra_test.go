// SPDX-License-Identifier: Apache-2.0

package bridge_test

import (
	"context"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/bridge"
	"github.com/stablekernel/crucible/state"
)

func TestMiddlewareSkipsFanOutOnError(t *testing.T) {
	t.Parallel()

	bucket := csink.NewBucket()
	m := csink.NewManifold(csink.WithOutlets(bucket))
	machine := switchMachine(bridge.Middleware[string, string, *bulb](m))

	// "explode" has no transition from "off"; strict mode returns an error and
	// the middleware must not fan a transition out.
	res := machine.Cast(&bulb{}).Fire(context.Background(), "explode")
	if res.Err == nil {
		t.Fatal("Fire(unhandled) err = nil, want an error")
	}
	if got := len(bucket.All()); got != 0 {
		t.Fatalf("fanned out %d transitions on error, want 0", got)
	}
}

func TestInspectorIgnoresNonTransitionKinds(t *testing.T) {
	t.Parallel()

	bucket := csink.NewBucket()
	m := csink.NewManifold(csink.WithOutlets(bucket))
	insp := bridge.Inspector(m)

	insp.Inspect(state.InspectionEvent{Kind: state.InspectSnapshot, Machine: "switch", To: "on"})
	insp.Inspect(state.InspectionEvent{Kind: state.InspectEvent, Machine: "switch"})

	if got := len(bucket.All()); got != 0 {
		t.Fatalf("inspector fanned out %d non-transition events, want 0", got)
	}
}

func TestWithSpanNameAndNilTracerIgnored(t *testing.T) {
	t.Parallel()

	bucket := csink.NewBucket()
	m := csink.NewManifold(csink.WithOutlets(bucket))
	machine := switchMachine(bridge.Middleware[string, string, *bulb](
		m,
		bridge.WithSpanName("custom.transition"),
		bridge.WithTracer(nil), // ignored, default no-op tracer stays
	))

	machine.Cast(&bulb{}).Fire(context.Background(), "toggle")
	if got := len(csink.RecordsOf[bridge.Transition](bucket)); got != 1 {
		t.Fatalf("fanned out %d transitions, want 1", got)
	}
}
