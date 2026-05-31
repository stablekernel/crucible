package state_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// pctx is a value-semantics context recording which region's reducer observed the
// broadcast event payload.
type pctx struct {
	Seen []string
}

// TestParallel_EventDataReachesHandlingRegion asserts the WithEventData payload is
// broadcast to every region of a parallel state, so the region that actually
// handles the triggering event sees it through AssignCtx.Event — regardless of the
// region's declaration order. This guards the consume-once event-data resolution:
// resolving the payload per region would hand it to whichever region is visited
// first and strip it from the rest, masking a service/actor onDone result from the
// region that routes it.
//
// The machine has two regions of a parallel superstate. Only the SECOND region (in
// declaration order) handles the "deliver" event; its reducer reads the payload.
// The first region declares no transition on "deliver", so under the old
// consume-once-per-region behavior it would have eaten the payload and left the
// second region's reducer with the bare event.
func TestParallel_EventDataReachesHandlingRegion(t *testing.T) {
	const (
		root  = "root"
		aIdle = "aIdle"
		bIdle = "bIdle"
		bDone = "bDone"
	)

	m := state.Forge[string, string, pctx]("parbroadcast").
		Reducer("recordPayload", func(in state.AssignCtx[pctx]) pctx {
			c := in.Entity
			if s, ok := in.Event.(string); ok {
				c.Seen = append(c.Seen, "payload:"+s)
			} else {
				c.Seen = append(c.Seen, "no-payload")
			}
			return c
		}).
		SuperState(root).
		// First region: declares NO transition on "deliver". It must not consume the
		// broadcast payload.
		Region("first").
		Initial(aIdle).
		SubState(aIdle).
		EndRegion().
		// Second region: handles "deliver" and reads the payload in its reducer.
		Region("second").
		Initial(bIdle).
		SubState(bIdle).
		On("deliver").GoTo(bDone).Assign("recordPayload").
		SubState(bDone).
		EndRegion().
		EndSuperState().
		Initial(root).
		Quench()

	inst := m.Cast(pctx{}, state.WithInitialState(root))
	ctx := context.Background()

	// The "second" region handles "deliver" while the "first" region does not. The
	// payload must reach the second region's reducer even though it is declared
	// after the non-handling first region.
	res := inst.Fire(ctx, "deliver", state.WithEventData("result-token"))
	if res.Err != nil {
		t.Fatalf("Fire returned error: %v", res.Err)
	}

	got := inst.Entity().Seen
	if len(got) != 1 || got[0] != "payload:result-token" {
		t.Fatalf("handling region did not observe the broadcast payload: %v", got)
	}
}

// TestParallel_EventDataReachesCrossCuttingExit asserts that when NO region handles
// the event and it bubbles up to a cross-cutting transition on the parallel
// superstate, that transition's Assign still sees the payload. This is the path an
// invoked actor's onDone takes when its completion exits the whole parallel state
// (e.g. a courier drop that ends fulfillment): the regions are offered the event but
// none consume it, so it must reach the compound's exit transition with the payload
// intact rather than stripped by the offer.
func TestParallel_EventDataReachesCrossCuttingExit(t *testing.T) {
	const (
		root  = "root"
		aIdle = "aIdle"
		bIdle = "bIdle"
		done  = "done"
	)

	m := state.Forge[string, string, pctx]("parexit").
		Reducer("recordPayload", func(in state.AssignCtx[pctx]) pctx {
			c := in.Entity
			if s, ok := in.Event.(string); ok {
				c.Seen = append(c.Seen, "payload:"+s)
			} else {
				c.Seen = append(c.Seen, "no-payload")
			}
			return c
		}).
		SuperState(root).
		// Neither region declares a transition on "finish"; it bubbles to the
		// compound's cross-cutting exit.
		Region("first").
		Initial(aIdle).
		SubState(aIdle).
		EndRegion().
		Region("second").
		Initial(bIdle).
		SubState(bIdle).
		EndRegion().
		// Cross-cutting exit on the compound: handles "finish", reads the payload.
		Transition(root).On("finish").GoTo(done).Assign("recordPayload").
		EndSuperState().
		State(done).Final().
		Initial(root).
		Quench()

	inst := m.Cast(pctx{}, state.WithInitialState(root))
	res := inst.Fire(context.Background(), "finish", state.WithEventData("drop-proof"))
	if res.Err != nil {
		t.Fatalf("Fire returned error: %v", res.Err)
	}
	if inst.Current() != done {
		t.Fatalf("cross-cutting exit should land in done; current=%v", inst.Current())
	}

	got := inst.Entity().Seen
	if len(got) != 1 || got[0] != "payload:drop-proof" {
		t.Fatalf("cross-cutting exit Assign did not observe the payload: %v", got)
	}
}

// TestParallel_ExitCancelsOrthogonalRegionTimers asserts that exiting a parallel
// superstate via a cross-cutting transition cancels the armed `after` timers in ALL
// of its regions, not just the primary leaf's spine. A region that armed an SLA
// timer must have it canceled when an orthogonal region's event tears down the whole
// parallel configuration — otherwise the abandoned timer leaks.
func TestParallel_ExitCancelsOrthogonalRegionTimers(t *testing.T) {
	const (
		start   = "start"
		root    = "root"
		spIdle  = "spIdle"
		spNext  = "spNext"
		clkArm  = "clkArm"
		clkFire = "clkFire"
		gone    = "gone"
	)

	m := state.Forge[string, string, pctx]("parexittimer").
		State(start).
		Transition(start).On("enter").GoTo(root).
		SuperState(root).
		// Spine region: a plain event-driven region with no timer.
		Region("spine").
		Initial(spIdle).
		SubState(spIdle).
		On("advance").GoTo(spNext).
		SubState(spNext).
		EndRegion().
		// Clock region: arms an `after` timer.
		Region("clock").
		Initial(clkArm).
		SubState(clkArm).
		After(5 * time.Second).On("elapsed").GoTo(clkFire).
		SubState(clkFire).
		EndRegion().
		// Cross-cutting exit: "abort" exits the whole compound.
		Transition(root).On("abort").GoTo(gone).
		EndSuperState().
		State(gone).Final().
		Initial(start).
		Quench()

	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(pctx{}, state.WithInitialState(start), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	ctx := context.Background()
	sch.Absorb(ctx, inst.StartEffects())

	// Enter the parallel state; the clock region arms its SLA timer on entry.
	enterRes := inst.Fire(ctx, "enter")
	sch.Absorb(ctx, enterRes.Effects)

	if sch.Pending() != 1 {
		t.Fatalf("clock region should arm one timer on entry; pending=%d", sch.Pending())
	}

	// Abort exits the whole parallel state from the spine side. The clock region's
	// armed timer must be canceled even though it lives in the orthogonal region.
	res := inst.Fire(ctx, "abort")
	sch.Absorb(ctx, res.Effects)
	if sch.Pending() != 0 {
		t.Fatalf("exiting the parallel state should cancel the orthogonal region's timer; pending=%d", sch.Pending())
	}
}
