package durable_test

// The tests for the two gaps of issue #261, an explicit stopActor while the declaring
// state stays active. First: a step that skips a state's spawn because the same step
// stops the actor notes nothing — not even an attempt — so the journal records the
// actor unstarted, and Recover runs it fresh where the live Handle has none. The skip
// comes two ways: a stop later in the step's own effects (the OnDone of a compound the
// step completes, or a stop a later timer of the same Tick declares), or a stop a
// parent Fire an earlier spawn drove, which marks the actor's activation exited. Second:
// a stop that arrives while Recover respawns a batch of running actors, before its
// target respawns, does nothing to it, though the journal holds the stop.
//
// Every row compares the live Handle's actors with the recovered Handle's: neither may
// run an actor the journal ended, and both must hold the same ones.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// stepFetch is a service that succeeds, for the stepCtx fixtures.
func stepFetch(context.Context, state.ServiceCtx[stepCtx]) (any, error) { return "ok", nil }

// stopSkipMachine enters Y, which declares kitchen, by one of three drivings: a Fire of
// "go", the "go" a Tick fires for w's timer, or the "fetched" a RunService settles for
// loading's fetch. Entering Y completes Yc, whose OnDone runs stopActor on kitchen
// while Y stays active — a stop later in the same effects than kitchen's spawn.
func stopSkipMachine() stepMachine {
	return state.ForgeFor[stepCtx]("stopSkip").
		Service("fetch", stepFetch).
		Actor("kitchen").
		State("idle").
		State("w").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("fetched")).
		SuperState("Yc").
		Initial("Y").
		OnDone("crucible.stopActor", map[string]any{"id": "kitchen"}).
		SubState("Y").InvokeActor("kitchen", state.WithInvokeID("kitchen")).Final().
		EndSuperState().
		Initial("idle").
		Transition("idle").On("go").GoTo("Y").
		Transition("idle").On("wait").GoTo("w").
		Transition("w").After(4 * time.Second).On("go").GoTo("Y").
		Transition("idle").On("load").GoTo("loading").
		Transition("loading").On("fetched").GoTo("Y").
		Quench()
}

// TestRecover_ASpawnTheSameStepStops_IsEndedNotUnstarted pins that a spawn a step
// skips because a later effect of the same step stops the actor, while its state stays
// active, counts as ended: the live Handle runs nothing, and Recover mustn't either —
// the journal must record the skipped spawn as attempted and ended, not unstarted, or
// a Recover runs the actor fresh and its behavior a second time.
func TestRecover_ASpawnTheSameStepStops_IsEndedNotUnstarted(t *testing.T) {
	ctx := context.Background()
	for _, by := range []string{"Fire", "Tick", "RunService"} {
		for _, every := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s checkpoint every %d", by, every), func(t *testing.T) {
				m := stopSkipMachine()
				var kitchenSpawns int
				clk := state.NewFakeClock(epoch)
				opts := []stepOption{
					durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
						"kitchen": func(map[string]any) (state.ActorInstance, error) {
							kitchenSpawns++
							return &noteTarget{}, nil
						},
					}),
				}
				if by == "Tick" {
					opts = append(opts, durable.WithRunnerClock[string, string, stepCtx](clk))
				}
				if by == "RunService" {
					opts = append(opts, durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", stepFetch)))
				}
				if every > 0 {
					opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
				}
				store, id := durable.NewMemStore(), durable.InstanceID("stop-same-step")
				h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				switch by {
				case "Fire":
					if _, err = h.Fire(ctx, "go"); err != nil {
						t.Fatalf("Fire(go): %v", err)
					}
				case "Tick":
					if _, err = h.Fire(ctx, "wait"); err != nil {
						t.Fatalf("Fire(wait): %v", err)
					}
					clk.Advance(6 * time.Second)
					if fired, terr := h.Tick(ctx); terr != nil || len(fired) != 1 {
						t.Fatalf("Tick fired %d timer(s), err %v; want w's one", len(fired), terr)
					}
				case "RunService":
					if _, err = h.Fire(ctx, "load"); err != nil {
						t.Fatalf("Fire(load): %v", err)
					}
					if _, ran, rerr := h.RunService(ctx, state.InvokeID("stopSkip", "loading", 0)); !ran || rerr != nil {
						t.Fatalf("RunService = ran %v, err %v; want the fetch settled", ran, rerr)
					}
				}
				if _, ok := h.ActorRef("kitchen"); ok {
					t.Fatal("live: kitchen is running, want none: the step skipped its spawn for the stop the same effects hold")
				}
				if kitchenSpawns != 0 {
					t.Fatalf("live: kitchen spawned %d time(s), want none", kitchenSpawns)
				}
				for round := 1; round <= 2; round++ {
					recovered, rerr := durable.Recover(ctx, m, store, id, opts...)
					if rerr != nil {
						t.Fatalf("Recover %d: %v", round, rerr)
					}
					if _, ok := recovered.ActorRef("kitchen"); ok {
						t.Fatalf("Recover %d runs kitchen, want none: the live Handle has no such actor, so the journal must record the skipped spawn as ended, not unstarted", round)
					}
					if kitchenSpawns != 0 {
						t.Fatalf("Recover %d ran kitchen's behavior %d time(s), want none: a fresh spawn re-runs what the live step skipped", round, kitchenSpawns)
					}
				}
			})
		}
	}
}

// stopByParentMachine enters both — by a Fire of "enter", or by starting in it — whose
// left region runs a1 and whose right region declares kitchen. a1 sends stopK as it
// starts, whose Fire enters X2 and explicitly stops kitchen while Y stays active,
// before the step absorbs kitchen's own spawn.
func stopByParentMachine() stepMachine {
	return state.ForgeFor[stepCtx]("stopByParent").
		Actor("a1").
		Actor("kitchen").
		State("idle").
		SuperState("both").
		Region("left").
		Initial("X").
		SubState("X").InvokeActor("a1", state.WithInvokeID("a1")).
		SubState("X2").
		EndRegion().
		Region("right").
		Initial("Y").
		SubState("Y").InvokeActor("kitchen", state.WithInvokeID("kitchen")).
		EndRegion().
		EndSuperState().
		Initial("idle").
		Transition("idle").On("enter").GoTo("both").
		Transition("X").On("stopK").GoTo("X2").StopActor("kitchen").
		Transition("Y").On("poke").GoTo("Y").
		Quench()
}

// TestRecover_ASpawnAParentFireStops_IsEndedNotUnstarted pins the same for a stop a
// parent Fire an earlier spawn drove: the Fire's stop, arriving before the step absorbs
// kitchen's spawn, notes kitchen's activation as exited, so the step skips the spawn as
// one whose state was left — but Y stays active. The Handle that drove the step runs no
// kitchen; the journal must record the skipped spawn as ended, so a Recover runs none.
// A Start that begins in both leaves the same journal once a step records its sets.
func TestRecover_ASpawnAParentFireStops_IsEndedNotUnstarted(t *testing.T) {
	ctx := context.Background()
	for _, by := range []string{"Fire", "Start"} {
		for _, every := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s checkpoint every %d", by, every), func(t *testing.T) {
				m := stopByParentMachine()
				var kitchenSpawns int
				opts := []stepOption{
					durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
						"a1": func(map[string]any) (state.ActorInstance, error) { return &sender{ev: "stopK"}, nil },
						"kitchen": func(map[string]any) (state.ActorInstance, error) {
							kitchenSpawns++
							return &noteTarget{}, nil
						},
					}),
				}
				if every > 0 {
					opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
				}
				store, id := durable.NewMemStore(), durable.InstanceID("stop-by-parent")
				initial := "idle"
				if by == "Start" {
					initial = "both"
				}
				h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState(initial))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				if by == "Fire" {
					if _, err = h.Fire(ctx, "enter"); err != nil {
						t.Fatalf("Fire(enter): %v", err)
					}
				}
				// A step journals the driven Fire and the actor sets: for a Start, the
				// driven Fire is pending until one runs.
				if _, err = h.Fire(ctx, "poke"); err != nil {
					t.Fatalf("Fire(poke): %v", err)
				}
				if _, ok := h.ActorRef("kitchen"); ok {
					t.Fatal("live: kitchen is running, want none: a1's Fire stopped it before its spawn was absorbed")
				}
				if kitchenSpawns != 0 {
					t.Fatalf("live: kitchen spawned %d time(s), want none", kitchenSpawns)
				}
				for round := 1; round <= 2; round++ {
					recovered, rerr := durable.Recover(ctx, m, store, id, opts...)
					if rerr != nil {
						t.Fatalf("Recover %d: %v", round, rerr)
					}
					if _, ok := recovered.ActorRef("kitchen"); ok {
						t.Fatalf("Recover %d runs kitchen, want none: the live Handle has no such actor, so the journal must record the skipped spawn as ended, not unstarted", round)
					}
					if kitchenSpawns != 0 {
						t.Fatalf("Recover %d ran kitchen's behavior %d time(s), want none", round, kitchenSpawns)
					}
				}
			})
		}
	}
}

// reentryMachine starts in both, whose left region runs nothing in X and whose right
// region runs kitchen in Y. X's 4-second timer enters X3, which runs stopper; Y's
// 6-second timer re-enters right, exiting and re-entering Y. A Tick past both deadlines
// fires them in that order: the go timer's absorb spawns stopper, whose stopK Fire
// enters X2 and explicitly stops kitchen while Y stays active, and the re timer's absorb
// then takes kitchen's spawn for Y's new activation, after the stop its exit emitted.
func reentryMachine() stepMachine {
	return state.ForgeFor[stepCtx]("stopReentry").
		Actor("stopper").
		Actor("kitchen").
		SuperState("both").
		Region("left").
		Initial("X").
		SubState("X").
		SubState("X3").InvokeActor("stopper", state.WithInvokeID("stopper")).
		SubState("X2").
		EndRegion().
		Region("right").
		Initial("R").
		SuperState("R").
		Initial("Y").
		SubState("Y").InvokeActor("kitchen", state.WithInvokeID("kitchen")).
		EndSuperState().
		EndRegion().
		EndSuperState().
		Initial("both").
		Transition("X").After(4 * time.Second).On("go").GoTo("X3").
		Transition("Y").After(6 * time.Second).On("re").GoTo("R").
		Transition("X3").On("stopK").GoTo("X2").StopActor("kitchen").
		Transition("Y").On("poke").GoTo("Y").
		Quench()
}

// TestTick_AnExplicitStopThenAReentry_SpawnsTheNewActivation pins that an explicit stop
// a parent Fire notes during a tick ends only the activation that was current when the
// Fire noted it: a later timer of the same tick that re-enters the state spawns the new
// activation's actor, rather than skipping it as one the stop ended. The live Handle
// runs kitchen again, and a Recover, which replays the tick's timers and then its driven
// Fire, ends in the same configuration and runs kitchen too.
func TestTick_AnExplicitStopThenAReentry_SpawnsTheNewActivation(t *testing.T) {
	ctx := context.Background()
	for _, every := range []int{0, 1} {
		t.Run(fmt.Sprintf("checkpoint every %d", every), func(t *testing.T) {
			m := reentryMachine()
			var kitchenSpawns int
			clk := state.NewFakeClock(epoch)
			opts := []stepOption{
				durable.WithRunnerClock[string, string, stepCtx](clk),
				durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
					"stopper": func(map[string]any) (state.ActorInstance, error) { return &sender{ev: "stopK"}, nil },
					"kitchen": func(map[string]any) (state.ActorInstance, error) {
						kitchenSpawns++
						return &noteTarget{}, nil
					},
				}),
			}
			if every > 0 {
				opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
			}
			store, id := durable.NewMemStore(), durable.InstanceID("stop-reentry")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("both"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			// A step journals kitchen as running, so a Recover respawns it.
			if _, err = h.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke): %v", err)
			}
			if kitchenSpawns != 1 {
				t.Fatalf("setup: kitchen spawned %d times, want its start", kitchenSpawns)
			}
			clk.Advance(7 * time.Second)
			fired, err := h.Tick(ctx)
			if err != nil || len(fired) != 2 {
				t.Fatalf("Tick fired %d timer(s), err %v; want go then re", len(fired), err)
			}
			if _, ok := h.ActorRef("kitchen"); !ok {
				t.Fatal("live: kitchen isn't running, want it running for Y's new activation: the re timer's spawn isn't one the stopK Fire ended")
			}
			if kitchenSpawns != 2 {
				t.Fatalf("live: kitchen spawned %d times, want its start and the new activation's", kitchenSpawns)
			}
			wantSpawns := kitchenSpawns
			for round := 1; round <= 2; round++ {
				recovered, rerr := durable.Recover(ctx, m, store, id, opts...)
				if rerr != nil {
					t.Fatalf("Recover %d: %v", round, rerr)
				}
				if cfg := fmt.Sprint(recovered.Instance().Configuration()); cfg != "[X2 Y]" {
					t.Fatalf("Recover %d configuration %s, want [X2 Y]", round, cfg)
				}
				if _, ok := recovered.ActorRef("kitchen"); !ok {
					t.Fatalf("Recover %d doesn't run kitchen, want it running, as the live Handle does: the replayed stopK Fire precedes the respawn, so it ends nothing the new activation holds", round)
				}
				wantSpawns++
				if kitchenSpawns != wantSpawns {
					t.Fatalf("Recover %d: kitchen spawned %d times, want %d", round, kitchenSpawns, wantSpawns)
				}
			}
			if cfg := fmt.Sprint(h.Instance().Configuration()); cfg != "[X2 Y]" {
				t.Fatalf("live configuration %s, want [X2 Y]", cfg)
			}
		})
	}
}

// earlyStopMachine runs a1 in A1 and c1 in C1, two regions of both. a1's spawn fails
// from its second run on, routing errA, whose Fire leaves A1 for A2 and explicitly
// stops c1 while C1 stays active.
func earlyStopMachine() stepMachine {
	return state.ForgeFor[stepCtx]("earlyStop").
		Actor("a1").
		Actor("c1").
		SuperState("both").
		Region("left").
		Initial("A1").
		SubState("A1").InvokeActor("a1", state.WithInvokeID("a1"), state.WithInvokeOnError("errA")).
		SubState("A2").
		EndRegion().
		Region("right").
		Initial("C1").
		SubState("C1").InvokeActor("c1", state.WithInvokeID("c1")).
		EndRegion().
		EndSuperState().
		Initial("both").
		Transition("A1").On("errA").GoTo("A2").StopActor("c1").
		Transition("C1").On("poke").GoTo("C1").
		Quench()
}

// earlyStopPalette counts each actor's spawns; a1 fails from its second spawn on, as
// when a Recover's respawn finds a dependency gone, routing errA.
func earlyStopPalette(a1Spawns, c1Spawns *int) stepOption {
	return durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
		"a1": func(map[string]any) (state.ActorInstance, error) {
			*a1Spawns++
			if *a1Spawns > 1 {
				return nil, errors.New("gone")
			}
			return &noteTarget{}, nil
		},
		"c1": func(map[string]any) (state.ActorInstance, error) {
			*c1Spawns++
			return &noteTarget{}, nil
		},
	})
}

// TestRecover_AStopBeforeItsTargetRespawns_EndsIt pins that an explicit stop of c1
// ends it wherever the stop arrives. A live Fire of errA, stopping c1 while it runs,
// ends it, and Recover agrees — the journal records it ended. A stop that arrives
// while Recover respawns a batch of running actors must end the actors that batch
// respawned too: a1's respawn fails first, routing errA before c1 respawns, so the
// stop finds nothing running and does nothing — yet the journal holds it. The Handle
// that drove the respawn, and one recovered from its journal, must both hold c1 ended.
func TestRecover_AStopBeforeItsTargetRespawns_EndsIt(t *testing.T) {
	ctx := context.Background()
	t.Run("a live Fire, while its target runs", func(t *testing.T) {
		m := earlyStopMachine()
		var a1Spawns, c1Spawns int
		opts := []stepOption{earlyStopPalette(&a1Spawns, &c1Spawns)}
		store, id := durable.NewMemStore(), durable.InstanceID("early-stop-live")
		h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("both"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "errA"); err != nil {
			t.Fatalf("Fire(errA): %v", err)
		}
		for _, hh := range []struct {
			name string
			h    *durable.Handle[string, string, stepCtx]
		}{{"live", h}} {
			if _, ok := hh.h.ActorRef("c1"); ok {
				t.Fatalf("%s: c1 is running, want it stopped by the errA Fire while C1 stays active", hh.name)
			}
		}
		recovered, rerr := durable.Recover(ctx, m, store, id, opts...)
		if rerr != nil {
			t.Fatalf("Recover: %v", rerr)
		}
		if _, ok := recovered.ActorRef("c1"); ok {
			t.Fatal("recovered: c1 is running, want it ended, as the live Fire stopped it")
		}
		if c1Spawns != 1 {
			t.Fatalf("c1 spawned %d time(s), want only its live start", c1Spawns)
		}
	})
	for _, every := range []int{0, 1} {
		t.Run(fmt.Sprintf("a respawn, checkpoint every %d", every), func(t *testing.T) {
			m := earlyStopMachine()
			var a1Spawns, c1Spawns int
			opts := []stepOption{earlyStopPalette(&a1Spawns, &c1Spawns)}
			if every > 0 {
				opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
			}
			store, id := durable.NewMemStore(), durable.InstanceID("early-stop-respawn")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("both"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			// A step records both actors running, so the Recover respawns them as a
			// batch, a1 first.
			if _, err = h.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke): %v", err)
			}
			h1, rerr := durable.Recover(ctx, m, store, id, opts...)
			if rerr != nil {
				t.Fatalf("Recover: %v", rerr)
			}
			if a1Spawns != 2 {
				t.Fatalf("setup: a1 spawned %d times, want its respawn tried and failed", a1Spawns)
			}
			// The stop a1's failed respawn routed must apply to the batch: c1 ended.
			if _, err = h1.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke) after Recover: %v", err)
			}
			h2, rerr := durable.Recover(ctx, m, store, id, opts...)
			if rerr != nil {
				t.Fatalf("Recover again: %v", rerr)
			}
			for _, hh := range []struct {
				name string
				h    *durable.Handle[string, string, stepCtx]
			}{{"the Handle that drove the respawn", h1}, {"a Handle recovered from its journal", h2}} {
				if _, ok := hh.h.ActorRef("c1"); ok {
					t.Fatalf("%s runs c1, want it ended: a1's failed respawn routed errA, whose stop of c1 the journal holds", hh.name)
				}
				if _, ok := hh.h.ActorRef("a1"); ok {
					t.Fatalf("%s runs a1, want none: its respawn failed and errA left A1", hh.name)
				}
			}
		})
	}
}
