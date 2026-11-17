package durable_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// noRegistryMachine invokes fn in "loading", which "begin" enters from "idle". Its
// onDone "done" stays in loading, through an Assign that counts it, so a settled
// service's state stays active; "poke" stays in loading too, leaving the service's
// activation alone.
func noRegistryMachine(fn state.ServiceFn[svcCtx], a *scnAssigns) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("noReg").
		Service("fetch", fn).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			a.done++
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

// TestCheckpoint_WithoutARegistry_KeepsTheServices pins that a checkpoint a Handle
// recovered without WithServiceRegistry writes keeps the services the one before it
// held, parked or settled: a Handle with the registry parks loading's result, or
// settles it while loading stays active, and checkpoints; one recovered without the
// registry Fires a step that leaves the service alone, and checkpoints again; then one
// recovered with the registry calls RunService. It settles the parked result, or finds
// the service settled, and the ServiceFn doesn't run again.
func TestCheckpoint_WithoutARegistry_KeepsTheServices(t *testing.T) {
	for _, tc := range []struct {
		name string
		park bool
	}{{"parked", true}, {"settled", false}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var runs int64
			a := &scnAssigns{}
			fn := countingUsers(&runs)
			m := noRegistryMachine(fn, a)
			var refuse atomic.Bool
			refuse.Store(tc.park)
			bare := []durable.Option[string, string, svcCtx]{
				durable.WithCheckpointEvery[string, string, svcCtx](1),
				decoderFor("done", func(data []byte) (any, error) {
					if refuse.Load() {
						return nil, errors.New("decoder refused")
					}
					return durable.DecodeAs[scnUser](data)
				}),
			}
			withRegistry := append(slices.Clone(bare), durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)))
			store, id := durable.NewMemStore(), durable.InstanceID("no-reg")

			h, err := durable.NewRunner(m, store, withRegistry...).Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "begin"); err != nil {
				t.Fatalf("Fire(begin): %v", err)
			}
			_, ran, err := h.RunService(ctx, noRegistrySvc)
			if tc.park {
				mustPark(t, "RunService", ran, err)
			} else if !ran || err != nil {
				t.Fatalf("RunService = %v, %v; want it settled", ran, err)
			}

			nh, err := durable.Recover(ctx, m, store, id, bare...)
			if err != nil {
				t.Fatalf("Recover without the registry: %v", err)
			}
			if _, err = nh.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke) without the registry: %v", err)
			}

			refuse.Store(false)
			rh, err := durable.Recover(ctx, m, store, id, withRegistry...)
			if err != nil {
				t.Fatalf("Recover with the registry: %v", err)
			}
			_, ran, err = rh.RunService(ctx, noRegistrySvc)
			if err != nil {
				t.Fatalf("RunService after the registry-less checkpoint: %v", err)
			}
			if got := atomic.LoadInt64(&runs); got != 1 {
				t.Fatalf("the ServiceFn ran %d times, want once: the registry-less checkpoint dropped the %s service", got, tc.name)
			}
			// A parked result settles now; a settled service has nothing in flight.
			if ran != tc.park {
				t.Fatalf("RunService ran = %v, want %v", ran, tc.park)
			}
			if a.done != 1 {
				t.Fatalf("onDone ran %d times, want once", a.done)
			}
		})
	}
}

// TestRecover_WithoutARegistry_RefusesAJournaledServiceRecord pins that Recover without
// WithServiceRegistry refuses a tail holding a service's settle or park, rather than
// replay it with no service runner: only a checkpoint that compacted those Records
// recovers without the registry.
func TestRecover_WithoutARegistry_RefusesAJournaledServiceRecord(t *testing.T) {
	for _, park := range []bool{true, false} {
		ctx := context.Background()
		var runs int64
		fn := countingUsers(&runs)
		m := noRegistryMachine(fn, &scnAssigns{})
		dec := decoderFor("done", func(data []byte) (any, error) {
			if park {
				return nil, errors.New("decoder refused")
			}
			return durable.DecodeAs[scnUser](data)
		})
		store, id := durable.NewMemStore(), durable.InstanceID("no-reg-tail")
		h, err := durable.NewRunner(m, store, dec, durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))).
			Start(ctx, id, svcCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "begin"); err != nil {
			t.Fatalf("Fire(begin): %v", err)
		}
		_, _, _ = h.RunService(ctx, noRegistrySvc)
		if _, err = durable.Recover(ctx, m, store, id, dec); err == nil || !strings.Contains(err.Error(), "requires a service registry") {
			t.Fatalf("Recover without the registry (park %v) = %v, want it refused for want of a registry", park, err)
		}
	}
}
