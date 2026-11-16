package durable_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestTick_ATimerThatLeavesAServiceStateStopsTheService pins that a timer-driven
// transition out of a state stops the service that state invokes, as a host Fire's
// exit does: after the Tick, RunService reports no service in flight.
func TestTick_ATimerThatLeavesAServiceStateStopsTheService(t *testing.T) {
	t.Skip("Tick hands a timer transition's effects only to the scheduler, not the service and actor drivers; #236 fixes it")
	ctx := context.Background()
	var runs int
	fetch := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { runs++; return "ok", nil }
	m := state.ForgeFor[stepCtx]("tickExit").
		Service("fetch", fetch).
		State("idle").
		State("fetching").Invoke("fetch", state.WithInvokeOnDone("fetched")).
		State("timedOut").
		Initial("idle").
		Transition("idle").On("begin").GoTo("fetching").
		Transition("fetching").On("fetched").GoTo("fetching").
		Transition("fetching").After(5 * time.Second).On("timeout").GoTo("timedOut").
		Quench()
	clk := state.NewFakeClock(epoch)
	h, err := durable.NewRunner(m, durable.NewMemStore(),
		durable.WithRunnerClock[string, string, stepCtx](clk),
		durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fetch)),
	).Start(ctx, "tick-exit", stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "begin"); err != nil {
		t.Fatalf("Fire(begin): %v", err)
	}
	clk.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := h.Instance().Current(); got != "timedOut" {
		t.Fatalf("state = %q, want timedOut", got)
	}
	if _, ran, err := h.RunService(ctx, state.InvokeID("tickExit", "fetching", 0)); ran || err != nil {
		t.Fatalf("RunService after the timer left fetching = ran %v, %v (runs %d), want no service in flight", ran, err, runs)
	}
}

// TestRecover_ATickEnteredStatesActorMatchesLive pins that Recover leaves a state a
// timer entered with the actors the live run had: live, a Tick that enters greeting
// spawns its greeter, whose hello applies once; Recover then applies it no more and no
// less than live.
func TestRecover_ATickEnteredStatesActorMatchesLive(t *testing.T) {
	t.Skip("Tick hands a timer transition's effects only to the scheduler, not the service and actor drivers; #236 fixes it")
	ctx := context.Background()
	note := func(in state.AssignCtx[stepCtx]) stepCtx {
		c := in.Entity
		c.Notes = append(c.Notes, "hello")
		return c
	}
	m := state.ForgeFor[stepCtx]("tickEnter").
		Reducer("note", note).
		Actor("greeter").
		State("idle").
		State("waiting").
		State("greeting").InvokeActor("greeter").
		Initial("idle").
		Transition("idle").On("go").GoTo("waiting").
		Transition("waiting").After(5 * time.Second).On("due").GoTo("greeting").
		Transition("greeting").On("hello").GoTo("greeting").Assign("note").
		Transition("greeting").On("poke").GoTo("greeting").
		Quench()
	clk := state.NewFakeClock(epoch)
	opts := []stepOption{
		durable.WithRunnerClock[string, string, stepCtx](clk),
		durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
			"greeter": func(map[string]any) (state.ActorInstance, error) { return &reentryGreeter{}, nil },
		}),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("tick-enter")
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
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	live := fmt.Sprint(h.Instance().Entity().Notes)
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := fmt.Sprint(recovered.Instance().Entity().Notes); got != live || live != "[hello]" {
		t.Fatalf("live notes %s, recovered %s; want [hello] for both: the Tick's state spawns its greeter once", live, got)
	}
}
