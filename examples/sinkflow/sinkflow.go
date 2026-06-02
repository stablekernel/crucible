// SPDX-License-Identifier: Apache-2.0

// Package sinkflow is the flagship example for the Crucible suite: a running
// state machine whose every transition fans out through a sink.Manifold into
// several destinations, all observed through one telemetry tracer and meter and
// one slog logger.
//
// It is the intermingling proof: crucible/state, crucible/sink, and
// crucible/telemetry compose through the crucible/sink/bridge seam without any
// core importing another. Run the test with the recording tracer to see the
// sink emit span nest under the state-transition span, and the induced
// warehouse failure surface on both the slog logger and the sink.failed counter.
package sinkflow

import (
	"context"
	"log/slog"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/bridge"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/telemetry"
)

// Order is the entity the machine advances.
type Order struct {
	ID    string
	Stage string
}

// Stages and events for the order lifecycle.
const (
	Placed    = "placed"
	Preparing = "preparing"
	EnRoute   = "enroute"
	Delivered = "delivered"

	Prepare  = "prepare"
	Dispatch = "dispatch"
	Deliver  = "deliver"
)

// Flow bundles the wired machine and the destinations it fans out to, so a
// caller (the example, the test, the README walkthrough) can drive it and
// inspect what each destination received.
type Flow struct {
	Machine   *state.Machine[string, string, *Order]
	Analytics *csink.Bucket // emulated analytics store
	Audit     *csink.Bucket // emulated audit log
	Warehouse *FlakyOutlet  // emulated warehouse that fails on Dispatch
}

// FlakyOutlet is an emulated destination that fails for one configured event,
// to demonstrate that an outlet failure is observed without derailing fan-out.
type FlakyOutlet struct {
	FailOnEvent string
	Delivered   []bridge.Transition
}

// Sink records the transition, or returns an error when it matches FailOnEvent.
func (f *FlakyOutlet) Sink(_ context.Context, payload any) error {
	tr, ok := payload.(bridge.Transition)
	if !ok {
		return csink.ErrUnregistered
	}
	if tr.Event == f.FailOnEvent {
		return &csink.Error{Outlet: "warehouse", Phase: csink.PhaseApply, PayloadType: "bridge.Transition", Err: errFlaky}
	}
	f.Delivered = append(f.Delivered, tr)
	return nil
}

var errFlaky = errFlakyType{}

type errFlakyType struct{}

func (errFlakyType) Error() string { return "warehouse: dispatch rejected" }

// New wires a Manifold (with the given observability seams) into an order
// machine via the bridge middleware. The same tracer is given to both the
// manifold and the bridge, so emit spans nest under transition spans.
func New(opts NewOptions) *Flow {
	analytics := csink.NewBucket()
	audit := csink.NewBucket()
	warehouse := &FlakyOutlet{FailOnEvent: Dispatch}

	m := csink.NewManifold(
		csink.WithLogger(opts.Logger),
		csink.WithTracer(opts.Tracer),
		csink.WithMeter(opts.Meter),
		csink.WithOutlets(analytics, audit, warehouse),
	)

	machine := state.Forge[string, string, *Order]("order").
		Use(bridge.Middleware[string, string, *Order](m, bridge.WithTracer(opts.Tracer))).
		State(Placed).State(Preparing).State(EnRoute).State(Delivered).
		Initial(Placed).
		CurrentStateFn(func(o *Order) string { return o.Stage }).
		Transition(Placed).On(Prepare).GoTo(Preparing).
		Transition(Preparing).On(Dispatch).GoTo(EnRoute).
		Transition(EnRoute).On(Deliver).GoTo(Delivered).
		Quench(state.Strict())

	return &Flow{Machine: machine, Analytics: analytics, Audit: audit, Warehouse: warehouse}
}

// NewOptions carries the observability seams for the flow. A nil field falls
// back to the corresponding sink no-op default.
type NewOptions struct {
	Logger *slog.Logger
	Tracer telemetry.Tracer
	Meter  telemetry.Meter
}

// Run drives one order from Placed to Delivered, returning the final stage.
func (f *Flow) Run(ctx context.Context) string {
	inst := f.Machine.Cast(&Order{ID: "order-1", Stage: Placed})
	for _, ev := range []string{Prepare, Dispatch, Deliver} {
		inst.Fire(ctx, ev)
	}
	return inst.Current()
}
