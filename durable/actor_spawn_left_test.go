package durable_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// leftAtStartMachine is a machine that starts in both, two regions running a1 in A and
// a2 in B, and a third holding D, whose poke changes nothing. B's leaveB goes to C, and
// C's backB back to B.
func leftAtStartMachine(name string) stepMachine {
	return state.ForgeFor[stepCtx](name).
		Actor("a1").
		Actor("a2").
		SuperState("both").
		Region("left").Initial("A").
		SubState("A").InvokeActor("a1").
		EndRegion().
		Region("right").Initial("B").
		SubState("B").InvokeActor("a2").
		SubState("C").
		EndRegion().
		Region("third").Initial("D").
		SubState("D").
		EndRegion().
		EndSuperState().
		Initial("both").
		Transition("B").On("leaveB").GoTo("C").
		Transition("C").On("backB").GoTo("B").
		Transition("D").On("poke").GoTo("D").
		Quench()
}

// TestStartAndRecover_AnActorWhoseStateAnEarlierActorLeft_DoesNotSpawn pins that Start
// and Recover, which spawn the configuration's actors, spawn none for a state a parent
// Fire an earlier spawn drove has already left. a1, spawned first, sends the parent
// leaveB as it starts, which exits B before a2's spawn. A Recover from the journal or a
// checkpoint replays that Fire and ends in C, so it never runs a2; Start mustn't, and
// nor must a Recover from the baseline, where a1 starts afresh and sends leaveB again.
// When a1 then sends backB, that Fire spawns a2 for B's new activation, once.
func TestStartAndRecover_AnActorWhoseStateAnEarlierActorLeft_DoesNotSpawn(t *testing.T) {
	for _, every := range []int{0, 1} {
		for _, back := range []bool{false, true} {
			t.Run(fmt.Sprintf("checkpoint every %d back %v", every, back), func(t *testing.T) {
				ctx := context.Background()
				machine := "startLeft"
				m := leftAtStartMachine(machine)
				a1, a2 := state.ActorID(machine, "A", 0), state.ActorID(machine, "B", 0)
				spawned := map[string]int{}
				opts := []stepOption{
					durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
						"a1": func(map[string]any) (state.ActorInstance, error) { spawned["a1"]++; return &leaver{back: back}, nil },
						"a2": func(map[string]any) (state.ActorInstance, error) { spawned["a2"]++; return &noteTarget{}, nil },
					}),
				}
				if every > 0 {
					opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
				}
				wantA2 := map[bool]int{false: 0, true: 1}[back]
				wantCfg := map[bool]string{false: "[A C D]", true: "[A B D]"}[back]
				check := func(name string, h *durable.Handle[string, string, stepCtx]) {
					t.Helper()
					if cfg := fmt.Sprint(h.Instance().Configuration()); cfg != wantCfg {
						t.Fatalf("%s configuration %s, want %s", name, cfg, wantCfg)
					}
					if _, ok := h.ActorRef(a1); !ok {
						t.Fatalf("%s: a1 isn't running, want it running in A", name)
					}
					if _, ok := h.ActorRef(a2); ok != back {
						t.Fatalf("%s: a2 running %v, want %v, as B is active (a2 spawned %d times)", name, ok, back, spawned["a2"])
					}
				}
				store, id := durable.NewMemStore(), durable.InstanceID("start-left")
				h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("both"))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				if spawned["a2"] != wantA2 {
					t.Fatalf("Start spawned a2 %d times, want %d: only for an active B", spawned["a2"], wantA2)
				}
				check("started", h)

				// From the baseline: the start's parent Fires aren't journaled yet, so a1
				// starts afresh and sends leaveB again.
				spawned["a2"] = 0
				fromBase, err := durable.Recover(ctx, m, store, id, opts...)
				if err != nil {
					t.Fatalf("Recover from the baseline: %v", err)
				}
				if spawned["a2"] != wantA2 {
					t.Fatalf("Recover from the baseline spawned a2 %d times, want %d", spawned["a2"], wantA2)
				}
				check("recovered from the baseline", fromBase)

				// A step journals the start's parent Fires; a Recover replays them.
				if _, err = fromBase.Fire(ctx, "poke"); err != nil {
					t.Fatalf("Fire(poke): %v", err)
				}
				if every > 0 && tailLen(t, store, id) != 0 {
					t.Fatalf("setup: %d records past the checkpoint, want none", tailLen(t, store, id))
				}
				check("live", fromBase)
				recovered, err := durable.Recover(ctx, m, store, id, opts...)
				if err != nil {
					t.Fatalf("Recover: %v", err)
				}
				check("recovered", recovered)
			})
		}
	}
}

// failingOnRespawn is an actor behavior that starts once, then fails every later
// spawn, as when a respawn at Recover finds a dependency gone.
func failingOnRespawn(n *int) state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		*n++
		if *n > 1 {
			return nil, errors.New("gone")
		}
		return &noteTarget{}, nil
	}
}

// TestRecover_ARunningActorWhoseStateARespawnsOnErrorLeft_DoesNotRun pins that a Recover
// whose respawn of a running actor fails, routing an onError that leaves another
// region's state, leaves that state's running actor stopped. a1 and a2 were running;
// a1's respawn, first, fails, and its onError leaveB exits B before a2 respawns. The
// next step journals that onError, and a Recover replays it and ends in C, so it never
// runs a2; the Handle that drove it mustn't keep a2 running either. When C goes back to
// B at once, the onError's Fire spawns a2 for B's new activation, and a2 keeps running.
func TestRecover_ARunningActorWhoseStateARespawnsOnErrorLeft_DoesNotRun(t *testing.T) {
	for _, every := range []int{0, 1} {
		for _, back := range []bool{false, true} {
			t.Run(fmt.Sprintf("checkpoint every %d back %v", every, back), func(t *testing.T) {
				ctx := context.Background()
				machine := "respawnLeft"
				b := state.ForgeFor[stepCtx](machine).
					Actor("a1").
					Actor("a2").
					SuperState("both").
					Region("left").Initial("A").
					SubState("A").InvokeActor("a1", state.WithInvokeOnError("leaveB")).
					EndRegion().
					Region("right").Initial("B").
					SubState("B").InvokeActor("a2").
					SubState("C")
				if back {
					b = b.Always().GoTo("B")
				}
				m := b.EndRegion().
					Region("third").Initial("D").
					SubState("D").
					EndRegion().
					EndSuperState().
					Initial("both").
					Transition("B").On("leaveB").GoTo("C").
					Transition("D").On("poke").GoTo("D").
					Quench()
				a2 := state.ActorID(machine, "B", 0)
				wantCfg := map[bool]string{false: "[A C D]", true: "[A B D]"}[back]
				var a1Spawns, a2Spawns int
				opts := []stepOption{
					durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
						"a1": failingOnRespawn(&a1Spawns),
						"a2": func(map[string]any) (state.ActorInstance, error) { a2Spawns++; return &noteTarget{}, nil },
					}),
				}
				if every > 0 {
					opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
				}
				store, id := durable.NewMemStore(), durable.InstanceID("respawn-left")
				h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("both"))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				// A step records both actors running.
				if _, err = h.Fire(ctx, "poke"); err != nil {
					t.Fatalf("Fire(poke): %v", err)
				}
				if _, ok := h.ActorRef(a2); !ok || a1Spawns != 1 {
					t.Fatalf("setup: a2 running %v, a1 spawned %d times; want a2 running, a1 spawned once", ok, a1Spawns)
				}
				live, err := durable.Recover(ctx, m, store, id, opts...)
				if err != nil {
					t.Fatalf("Recover: %v", err)
				}
				if a1Spawns != 2 {
					t.Fatalf("setup: a1 spawned %d times, want its respawn tried", a1Spawns)
				}
				if _, err = live.Fire(ctx, "poke"); err != nil {
					t.Fatalf("Fire(poke) after Recover: %v", err)
				}
				if every > 0 && tailLen(t, store, id) != 0 {
					t.Fatalf("setup: %d records past the checkpoint, want none", tailLen(t, store, id))
				}
				recovered, err := durable.Recover(ctx, m, store, id, opts...)
				if err != nil {
					t.Fatalf("Recover again: %v", err)
				}
				for _, hh := range []struct {
					name string
					h    *durable.Handle[string, string, stepCtx]
				}{{"live", live}, {"recovered", recovered}} {
					if cfg := fmt.Sprint(hh.h.Instance().Configuration()); cfg != wantCfg {
						t.Fatalf("%s configuration %s, want %s", hh.name, cfg, wantCfg)
					}
					if _, ok := hh.h.ActorRef(a2); ok != back {
						t.Fatalf("%s: a2 running %v, want %v: it runs only while B is active", hh.name, ok, back)
					}
				}
			})
		}
	}
}

// TestTick_AnActorWhoseStateALaterTimerLeaves_DoesNotSpawn pins that a Tick spawns no
// actor for a state a later timer of the same tick leaves. P arms leave on entry;
// inside it, w's toS enters S, which runs x. Both are due at one Tick, toS first, so the
// kernel has left S by the time the tick hands toS's effects to the actor system.
// Recover replays the timers and ends in out, so it never runs x; the live tick
// mustn't either, even for a moment before leave's effects stop it.
func TestTick_AnActorWhoseStateALaterTimerLeaves_DoesNotSpawn(t *testing.T) {
	for _, every := range []int{0, 1} {
		t.Run(fmt.Sprintf("checkpoint every %d", every), func(t *testing.T) {
			ctx := context.Background()
			machine := "tickLeft"
			x := state.ActorID(machine, "S", 0)
			m := state.ForgeFor[stepCtx](machine).
				Actor("x").
				State("idle").
				SuperState("P").
				SubState("w").
				SubState("S").InvokeActor("x").
				Initial("w").
				EndSuperState().
				State("out").
				Initial("idle").
				Transition("idle").On("go").GoTo("P").
				Transition("w").After(4 * time.Second).On("toS").GoTo("S").
				Transition("P").After(5 * time.Second).On("leave").GoTo("out").
				Quench()
			clk := state.NewFakeClock(epoch)
			var spawns int
			opts := []stepOption{
				durable.WithRunnerClock[string, string, stepCtx](clk),
				durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
					"x": func(map[string]any) (state.ActorInstance, error) { spawns++; return &noteTarget{}, nil },
				}),
			}
			if every > 0 {
				opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
			}
			store, id := durable.NewMemStore(), durable.InstanceID("tick-left")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "go"); err != nil {
				t.Fatalf("Fire(go): %v", err)
			}
			clk.Advance(6 * time.Second)
			fired, err := h.Tick(ctx)
			if err != nil || len(fired) != 2 {
				t.Fatalf("Tick fired %d timers, err %v; want both", len(fired), err)
			}
			if spawns != 0 {
				t.Fatalf("live tick spawned x %d times, want none: S isn't active after the tick", spawns)
			}
			if every > 0 && tailLen(t, store, id) != 0 {
				t.Fatalf("setup: %d records past the checkpoint, want none", tailLen(t, store, id))
			}
			recovered, err := durable.Recover(ctx, m, store, id, opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			for _, hh := range []struct {
				name string
				h    *durable.Handle[string, string, stepCtx]
			}{{"live", h}, {"recovered", recovered}} {
				if cfg := fmt.Sprint(hh.h.Instance().Configuration()); cfg != "[out]" {
					t.Fatalf("%s configuration %s, want [out]", hh.name, cfg)
				}
				if _, ok := hh.h.ActorRef(x); ok {
					t.Fatalf("%s: x is running, want it stopped with S", hh.name)
				}
			}
			if spawns != 0 {
				t.Fatalf("x spawned %d times, want none", spawns)
			}
		})
	}
}
