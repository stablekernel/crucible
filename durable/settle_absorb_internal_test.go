package durable

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// These tests answer a review finding on serviceStep's absorb: the ServiceRunner's
// settle absorbs the follow-on effects of its own Fire, so a state its onDone enters
// starts its services, and serviceStep then handed the same effects to the runner
// again, arming each of those services a second time. Replay arms them once: it
// re-settles through the runner and arms only the timers after (replayArm with a nil
// runner). serviceStep must leave the services to the settle, as replay does, while it
// still forgets the settled services and arms the timers and actors.

type absorbCtx struct {
	Notes []string `json:"notes"`
}

// absorbMachine invokes svcA in first; its onDone enters second, which invokes svcB and
// arms a timer, so the settle's follow-on effects start a service and schedule a timer.
func absorbMachine(svcA, svcB state.ServiceFn[absorbCtx]) *state.Machine[string, string, absorbCtx] {
	return state.ForgeFor[absorbCtx]("absorb").
		Service("svcA", svcA).
		Service("svcB", svcB).
		Reducer("note", func(in state.AssignCtx[absorbCtx]) absorbCtx {
			c := in.Entity
			if r, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, r)
			}
			return c
		}).
		State("first").Invoke("svcA", state.WithInvokeOnDone("aDone"), state.WithInvokeOnError("aFail")).
		State("second").Invoke("svcB", state.WithInvokeOnDone("bDone"), state.WithInvokeOnError("bFail")).
		State("done").Final().
		State("failed").Final().
		State("late").Final().
		Initial("first").
		Transition("first").On("aDone").GoTo("second").Assign("note").
		Transition("first").On("aFail").GoTo("failed").
		Transition("second").On("bDone").GoTo("done").Assign("note").
		Transition("second").On("bFail").GoTo("failed").
		Transition("second").After(time.Hour).On("late").GoTo("late").
		Quench()
}

// TestRunService_SettleArmsItsFollowOnServiceOnce: svcA's onDone enters a state that
// invokes svcB. Live, the settle arms svcB once, as the replay of the journal does, so
// the live runner and a recovered one have armed the same activations; a checkpoint
// taken after the settle restores svcB once. svcB's ServiceFn runs exactly once, live
// and on each recovered Handle, and each ends where the live run did, with the
// settle's timer armed.
func TestRunService_SettleArmsItsFollowOnServiceOnce(t *testing.T) {
	aID := state.InvokeID("absorb", "first", 0)
	bID := state.InvokeID("absorb", "second", 0)
	for _, mode := range []string{"live", "journal", "checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var aCalls, bCalls int
			svcA := func(context.Context, state.ServiceCtx[absorbCtx]) (any, error) {
				aCalls++
				return fmt.Sprintf("a-%d", aCalls), nil
			}
			svcB := func(context.Context, state.ServiceCtx[absorbCtx]) (any, error) {
				bCalls++
				return fmt.Sprintf("b-%d", bCalls), nil
			}
			m := absorbMachine(svcA, svcB)
			opts := []Option[string, string, absorbCtx]{
				WithServiceRegistry[string, string, absorbCtx](state.NewRegistry[absorbCtx]().Service("svcA", svcA).Service("svcB", svcB)),
				WithRunnerClock[string, string, absorbCtx](state.NewFakeClock(time.Unix(1_700_000_000, 0))),
			}
			if mode == "checkpoint" {
				opts = append(opts, WithCheckpointEvery[string, string, absorbCtx](1))
			}
			store := NewMemStore()
			h, err := NewRunner(m, store, opts...).Start(ctx, "a1", absorbCtx{}, state.WithInitialState("first"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if ok, rerr := runOnce(ctx, h, aID); rerr != nil || !ok {
				t.Fatalf("RunService(svcA) = %v, %v", ok, rerr)
			}
			if len(h.timers) != 1 {
				t.Errorf("live: the timer table is %v, want second's timer armed", h.timers)
			}

			run := h
			switch mode {
			case "journal":
				if run, err = Recover(ctx, m, store, "a1", opts...); err != nil {
					t.Fatalf("Recover: %v", err)
				}
			case "checkpoint":
				snap, tail, lerr := store.Load(ctx, "a1")
				if lerr != nil || len(tail) != 0 || snap == nil {
					t.Fatalf("Load: %d tail Records (err %v), want the settle in a checkpoint", len(tail), lerr)
				}
				if run, err = Recover(ctx, m, store, "a1", opts...); err != nil {
					t.Fatalf("Recover: %v", err)
				}
			}
			if !run.svc.HasPending(bID) || run.svc.Pending() != 1 {
				t.Fatalf("%s: svcB in flight = %v with %d pending, want svcB alone", mode, run.svc.HasPending(bID), run.svc.Pending())
			}
			if len(run.timers) != 1 {
				t.Errorf("%s: the timer table is %v, want second's timer armed", mode, run.timers)
			}
			if ok, rerr := runOnce(ctx, run, bID); rerr != nil || !ok {
				t.Fatalf("%s: RunService(svcB) = %v, %v", mode, ok, rerr)
			}
			if aCalls != 1 || bCalls != 1 {
				t.Errorf("%s: svcA ran %d times and svcB %d, want once each", mode, aCalls, bCalls)
			}
			if got := run.inst.Current(); got != "done" {
				t.Errorf("%s: ended in %q, want done", mode, got)
			}
			if got, want := run.inst.Snapshot().Context.Notes, []string{"a-1", "b-1"}; !reflect.DeepEqual(got, want) {
				t.Errorf("%s: notes %v, want %v", mode, got, want)
			}
		})
	}
}

// TestRunService_SettleThatReentersItsStateForgetsTheSettle: svcA's onDone leaves and
// re-enters the state that invokes it, so the settle starts a new activation of svcA.
// With the services left to the settle's own absorb, serviceStep must still forget that
// svcA settled, or the checkpoint the step makes records the new activation as settled
// and a Handle recovered from it doesn't run it.
func TestRunService_SettleThatReentersItsStateForgetsTheSettle(t *testing.T) {
	ctx := context.Background()
	calls := 0
	svcA := func(context.Context, state.ServiceCtx[absorbCtx]) (any, error) {
		calls++
		return fmt.Sprintf("a-%d", calls), nil
	}
	m := state.ForgeFor[absorbCtx]("again").
		Service("svcA", svcA).
		State("first").Invoke("svcA", state.WithInvokeOnDone("aDone"), state.WithInvokeOnError("aFail")).
		State("failed").Final().
		Initial("first").
		Transition("first").On("aDone").GoTo("first").Reenter().
		Transition("first").On("aFail").GoTo("failed").
		Quench()
	opts := []Option[string, string, absorbCtx]{
		WithServiceRegistry[string, string, absorbCtx](state.NewRegistry[absorbCtx]().Service("svcA", svcA)),
		WithCheckpointEvery[string, string, absorbCtx](1),
	}
	store := NewMemStore()
	h, err := NewRunner(m, store, opts...).Start(ctx, "r1", absorbCtx{}, state.WithInitialState("first"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	aID := state.InvokeID("again", "first", 0)
	if ok, rerr := runOnce(ctx, h, aID); rerr != nil || !ok {
		t.Fatalf("RunService = %v, %v", ok, rerr)
	}
	if !h.svc.HasPending(aID) || h.settled[aID] {
		t.Fatalf("live: svcA in flight = %v, settled = %v; want a new activation in flight, not settled", h.svc.HasPending(aID), h.settled[aID])
	}
	rh, err := Recover(ctx, m, store, "r1", opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if !rh.svc.HasPending(aID) {
		t.Fatal("recovered from the checkpoint: svcA isn't in flight, want its new activation")
	}
	if ok, rerr := runOnce(ctx, rh, aID); rerr != nil || !ok || calls != 2 {
		t.Fatalf("recovered RunService = %v, %v after %d calls, want svcA run a second time", ok, rerr, calls)
	}
}

// runOnce runs service id through h, reporting whether it ran and any error.
func runOnce[S comparable, E comparable, C any](ctx context.Context, h *Handle[S, E, C], id string) (bool, error) {
	_, ok, err := h.RunService(ctx, id)
	return ok, err
}
