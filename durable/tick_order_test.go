package durable_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestTick_AnActorsParentFireSeesEveryTimersServices pins the order a Tick that fires
// several timers drives its runners in: every fired transition's services start or stop
// first, then the actors spawn, and their parent Fires run, as a Fire that produced all
// of the transitions' effects does, and as replay does. Two timers come due at the same
// instant in the two regions of a parallel state: the left one enters a state that
// spawns a leaver, which sends its parent "leave" as it starts; the right one enters a
// state that invokes fetch. "leave" moves the right region on, out of the state that
// invokes fetch, so fetch must not be left in flight: not live, not after a Recover from
// the journal, and not after one from a checkpoint.
func TestTick_AnActorsParentFireSeesEveryTimersServices(t *testing.T) {
	ctx := context.Background()
	fetch := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { return "ok", nil }
	m := state.ForgeFor[stepCtx]("tickOrder").
		Service("fetch", fetch).
		Actor("leaver").
		State("idle").
		SuperState("par").
		Region("left").
		Initial("l0").
		SubState("l0").
		SubState("l1").InvokeActor("leaver").
		EndRegion().
		Region("right").
		Initial("r0").
		SubState("r0").
		SubState("r1").Invoke("fetch", state.WithInvokeOnDone("fetched")).
		SubState("r2").
		EndRegion().
		EndSuperState().
		Initial("idle").
		Transition("idle").On("go").GoTo("par").
		Transition("l0").After(5 * time.Second).On("leftDue").GoTo("l1").
		Transition("r0").After(5 * time.Second).On("rightDue").GoTo("r1").
		Transition("r1").On("leave").GoTo("r2").
		Transition("r1").On("fetched").GoTo("r1").
		Quench()
	fetchID := state.InvokeID("tickOrder", "r1", 0)
	for _, tc := range []struct {
		name  string
		every int
	}{{"journal", 0}, {"checkpoint", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			clk := state.NewFakeClock(epoch)
			opts := []stepOption{
				durable.WithRunnerClock[string, string, stepCtx](clk),
				durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fetch)),
				durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
					"leaver": func(map[string]any) (state.ActorInstance, error) {
						return &starting{start: []state.Effect{state.SendParent{Event: "leave"}}}, nil
					},
				}),
			}
			if tc.every > 0 {
				opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](tc.every))
			}
			store, id := durable.NewMemStore(), durable.InstanceID("tick-order")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "go"); err != nil {
				t.Fatalf("Fire(go): %v", err)
			}
			clk.Advance(6 * time.Second)
			if _, err = h.Tick(ctx); err != nil {
				t.Fatalf("Tick: %v", err)
			}
			if !slices.Contains(h.Instance().Configuration(), "r2") {
				t.Fatalf("live states = %v, want r2: the leaver's leave moved the right region on", h.Instance().Configuration())
			}
			recovered, err := durable.Recover(ctx, m, store, id, opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			_, liveRan, liveErr := h.RunService(ctx, fetchID)
			_, recRan, recErr := recovered.RunService(ctx, fetchID)
			if liveRan || recRan || liveErr != nil || recErr != nil {
				t.Fatalf("fetch in flight after r1 exited: live ran %v (%v), recovered ran %v (%v); want neither: live and replay must agree, with no service of an exited state in flight",
					liveRan, liveErr, recRan, recErr)
			}
		})
	}
}
