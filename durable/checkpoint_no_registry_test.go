package durable_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// noRegistryMachine invokes fn in "loading", which "begin" enters from "idle". Its
// onDone "done" stays in loading, through an Assign that counts it in done, so a
// settled service's state stays active; "poke" stays in loading too, leaving the
// service's activation alone.
func noRegistryMachine(fn state.ServiceFn[svcCtx], done *int) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("noReg").
		Service("fetch", fn).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			*done++
			return in.Entity
		}).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done")).
		Initial("idle").
		Transition("idle").On("begin").GoTo("loading").
		Transition("loading").On("done").GoTo("loading").Assign("noteDone").
		Transition("loading").On("poke").GoTo("loading").
		Quench()
}

var noRegistrySvc = state.InvokeID("noReg", "loading", 0)

// countingRuns returns a ServiceFn that counts its runs.
func countingRuns(runs *int64) state.ServiceFn[svcCtx] {
	return func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		return atomic.AddInt64(runs, 1), nil
	}
}

// TestCheckpoint_WithoutARegistry_KeepsTheSettledServices pins that a checkpoint a
// Handle recovered without WithServiceRegistry writes keeps the settled services the one
// before it held: a Handle with the registry settles loading's service while loading
// stays active, and checkpoints; one recovered without the registry Fires a step that
// leaves the service alone, and checkpoints again; then one recovered with the registry
// finds the service settled, and the ServiceFn doesn't run again.
func TestCheckpoint_WithoutARegistry_KeepsTheSettledServices(t *testing.T) {
	ctx := context.Background()
	var runs int64
	done := 0
	fn := countingRuns(&runs)
	m := noRegistryMachine(fn, &done)
	bare := []durable.Option[string, string, svcCtx]{durable.WithCheckpointEvery[string, string, svcCtx](1)}
	withRegistry := append(append([]durable.Option[string, string, svcCtx]{}, bare...), durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)))
	store, id := durable.NewMemStore(), durable.InstanceID("no-reg")

	h, err := durable.NewRunner(m, store, withRegistry...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "begin"); err != nil {
		t.Fatalf("Fire(begin): %v", err)
	}
	if _, ran, rerr := h.RunService(ctx, noRegistrySvc); !ran || rerr != nil {
		t.Fatalf("RunService = %v, %v; want it settled", ran, rerr)
	}

	nh, err := durable.Recover(ctx, m, store, id, bare...)
	if err != nil {
		t.Fatalf("Recover without the registry: %v", err)
	}
	if _, err = nh.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke) without the registry: %v", err)
	}

	rh, err := durable.Recover(ctx, m, store, id, withRegistry...)
	if err != nil {
		t.Fatalf("Recover with the registry: %v", err)
	}
	_, ran, err := rh.RunService(ctx, noRegistrySvc)
	if err != nil {
		t.Fatalf("RunService after the registry-less checkpoint: %v", err)
	}
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want once: the registry-less checkpoint dropped the settled service", got)
	}
	if ran {
		t.Fatal("RunService ran = true, want nothing in flight for a settled service")
	}
	if done != 1 {
		t.Fatalf("onDone ran %d times, want once", done)
	}
}

// TestRecover_WithoutARegistry_RefusesAJournaledServiceRecord pins that Recover without
// WithServiceRegistry refuses a tail holding a service's settle, rather than replay it
// with no service runner: only a checkpoint that compacted that Record recovers without
// the registry.
func TestRecover_WithoutARegistry_RefusesAJournaledServiceRecord(t *testing.T) {
	ctx := context.Background()
	var runs int64
	done := 0
	fn := countingRuns(&runs)
	m := noRegistryMachine(fn, &done)
	store, id := durable.NewMemStore(), durable.InstanceID("no-reg-tail")
	h, err := durable.NewRunner(m, store, durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))).
		Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "begin"); err != nil {
		t.Fatalf("Fire(begin): %v", err)
	}
	if _, ran, rerr := h.RunService(ctx, noRegistrySvc); !ran || rerr != nil {
		t.Fatalf("RunService = %v, %v; want it settled", ran, rerr)
	}
	if _, err = durable.Recover(ctx, m, store, id); err == nil || !strings.Contains(err.Error(), "requires a service registry") {
		t.Fatalf("Recover without the registry = %v, want it refused for want of a registry", err)
	}
}
