package durable_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// recordingActor is an actor that notes every event delivered to it.
type recordingActor struct{ got []any }

func (r *recordingActor) DeliverFire(_ context.Context, ev any) (bool, any) {
	r.got = append(r.got, ev)
	return false, nil
}
func (*recordingActor) ChildEffects() []state.Effect { return nil }
func (*recordingActor) Output() any                  { return nil }

// TestStep_AParentForwardToForwardsTheStepsEvent pins that a parent transition's
// forwardTo forwards the event that drove it, for a host Fire and for a timer a Tick
// fires, whose event is the timer's delayed event. Pair runs child; poke, and the timer
// due, each stay in pair and forward their event to child. Actor deliveries aren't
// replayed: a recovered Handle starts child fresh, from its initial state, so it holds
// nothing the live run forwarded, as for a SendTo.
func TestStep_AParentForwardToForwardsTheStepsEvent(t *testing.T) {
	for _, step := range []string{"Fire", "Tick"} {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			machine := "forward" + step
			childID := state.ActorID(machine, "pair", 0)
			b := state.ForgeFor[stepCtx](machine).
				Actor("child").
				State("idle").
				State("pair").InvokeActor("child").
				Initial("idle").
				Transition("idle").On("go").GoTo("pair")
			want := "poke"
			if step == "Fire" {
				b = b.Transition("pair").On("poke").GoTo("pair").ForwardTo(childID)
			} else {
				want = "due"
				b = b.Transition("pair").After(5 * time.Second).On("due").GoTo("pair").ForwardTo(childID)
			}
			m := b.Quench()
			clk := state.NewFakeClock(epoch)
			var children []*recordingActor
			opts := []stepOption{
				durable.WithRunnerClock[string, string, stepCtx](clk),
				durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
					"child": func(map[string]any) (state.ActorInstance, error) {
						children = append(children, &recordingActor{})
						return children[len(children)-1], nil
					},
				}),
			}
			store, id := durable.NewMemStore(), durable.InstanceID("forward")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "go"); err != nil {
				t.Fatalf("Fire(go): %v", err)
			}
			if step == "Fire" {
				_, err = h.Fire(ctx, "poke")
			} else {
				clk.Advance(6 * time.Second)
				_, err = h.Tick(ctx)
			}
			if err != nil {
				t.Fatalf("%s: %v", step, err)
			}
			if len(children) != 1 || fmt.Sprint(children[0].got) != fmt.Sprint([]any{want}) {
				t.Fatalf("live child got %v, want [%s]: forwardTo forwards the event that drove the transition", children[0].got, want)
			}
			if _, err = durable.Recover(ctx, m, store, id, opts...); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if len(children) != 2 || len(children[1].got) != 0 {
				t.Fatalf("recovered: %d children, the new one got %v; want child respawned fresh, with nothing replayed to it", len(children), children[len(children)-1].got)
			}
		})
	}
}

// leaver is an actor that sends its parent leaveB as it starts, then backB when back.
type leaver struct{ sent, back bool }

func (*leaver) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (l *leaver) ChildEffects() []state.Effect {
	if l.sent {
		return nil
	}
	l.sent = true
	if l.back {
		return []state.Effect{state.SendParent{Event: "leaveB"}, state.SendParent{Event: "backB"}}
	}
	return []state.Effect{state.SendParent{Event: "leaveB"}}
}
func (*leaver) Output() any { return nil }

// TestStep_AnActorWhoseStateAnEarlierActorLeft_DoesNotSpawn pins that a step spawns no
// actor for a state a parent Fire its absorb drove has already left. Two regions enter
// A, which runs a1, and B, which runs a2, by one Fire, or by two timers due in one
// Tick, A's first. a1 sends the parent leaveB as it starts, which exits B before a2's
// spawn is absorbed. Recover spawns the actors of the configuration it ends in, so it
// never runs a2, from the journal or from a checkpoint; the live Handle mustn't either.
// When a1 then sends backB, which enters B again, that Fire spawns a2 for B's new
// activation, once, and a2 runs live and recovered.
func TestStep_AnActorWhoseStateAnEarlierActorLeft_DoesNotSpawn(t *testing.T) {
	for _, step := range []string{"Fire", "Tick"} {
		for _, every := range []int{0, 1} {
			for _, back := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s checkpoint every %d back %v", step, every, back), func(t *testing.T) {
					ctx := context.Background()
					machine := "orphan" + step
					a1, a2 := state.ActorID(machine, "A", 0), state.ActorID(machine, "B", 0)
					b := state.ForgeFor[stepCtx](machine).
						Actor("a1").
						Actor("a2").
						State("idle").
						SuperState("both").
						Region("left")
					if step == "Fire" {
						b = b.Initial("A")
					} else {
						b = b.Initial("wA").SubState("wA")
					}
					b = b.SubState("A").InvokeActor("a1").
						EndRegion().
						Region("right")
					if step == "Fire" {
						b = b.Initial("B")
					} else {
						b = b.Initial("wB").SubState("wB")
					}
					b = b.SubState("B").InvokeActor("a2").
						SubState("C").
						EndRegion().
						EndSuperState().
						Initial("idle").
						Transition("idle").On("go").GoTo("both").
						Transition("B").On("leaveB").GoTo("C").
						Transition("C").On("backB").GoTo("B")
					if step == "Tick" {
						b = b.Transition("wA").After(4 * time.Second).On("toA").GoTo("A").
							Transition("wB").After(5 * time.Second).On("toB").GoTo("B")
					}
					m := b.Quench()
					clk := state.NewFakeClock(epoch)
					spawned := map[string]int{}
					opts := []stepOption{
						durable.WithRunnerClock[string, string, stepCtx](clk),
						durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
							"a1": func(map[string]any) (state.ActorInstance, error) { spawned["a1"]++; return &leaver{back: back}, nil },
							"a2": func(map[string]any) (state.ActorInstance, error) { spawned["a2"]++; return &noteTarget{}, nil },
						}),
					}
					if every > 0 {
						opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
					}
					store, id := durable.NewMemStore(), durable.InstanceID("orphan")
					h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
					if err != nil {
						t.Fatalf("Start: %v", err)
					}
					if _, err = h.Fire(ctx, "go"); err != nil {
						t.Fatalf("Fire(go): %v", err)
					}
					if step == "Tick" {
						clk.Advance(6 * time.Second)
						fired, tickErr := h.Tick(ctx)
						if tickErr != nil || len(fired) != 2 {
							t.Fatalf("Tick fired %d timers, err %v; want both", len(fired), tickErr)
						}
					}
					if spawned["a2"] != map[bool]int{false: 0, true: 1}[back] {
						t.Fatalf("live spawned a2 %d times, want it spawned only for an active B", spawned["a2"])
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
						name, h := hh.name, hh.h
						want := map[bool]string{false: "[A C]", true: "[A B]"}[back]
						if cfg := fmt.Sprint(h.Instance().Configuration()); cfg != want {
							t.Fatalf("%s configuration %s, want %s", name, cfg, want)
						}
						if _, ok := h.ActorRef(a1); !ok {
							t.Fatalf("%s: a1 isn't running, want it running in A", name)
						}
						if _, ok := h.ActorRef(a2); ok != back {
							t.Fatalf("%s: a2 running %v, want %v, as B is active (a2 spawned %d times)", name, ok, back, spawned["a2"])
						}
					}
				})
			}
		}
	}
}

// TestStep_AForwardToOnAParentFireAnActorOrASettleDrives pins that a forwardTo on a
// parent transition an actor drives — a message the actor sent the parent, from
// DeliverToActor or its start — or a service settle's onDone drives, forwards the
// event that drove the transition, not nil (#260): the actor-driven Fire runs inside
// the actor system, whose absorb carried no event, and a settle's effects went
// through absorbSettle with none.
func TestStep_AForwardToOnAParentFireAnActorOrASettleDrives(t *testing.T) {
	for _, step := range []string{"DeliverToActor", "RunService"} {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			machine := "driven" + step
			childID := state.ActorID(machine, "outer", 0)
			fetch := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { return "ok", nil }
			b := state.ForgeFor[stepCtx](machine).
				Actor("child").
				Actor("driver").
				Service("fetch", fetch)
			invokes := b.SuperState("outer").InvokeActor("child")
			if step == "RunService" {
				// The service settles through onDone, whose transition forwards.
				invokes = invokes.SubState("loading").Invoke("fetch", state.WithInvokeOnDone("loaded")).
					Initial("loading").
					EndSuperState().
					Initial("outer").
					Transition("loading").On("loaded").GoTo("loading").ForwardTo(childID)
			} else {
				// The driver tells the parent "poke" as it starts, whose transition forwards.
				invokes = invokes.SubState("running").InvokeActor("driver").
					Initial("running").
					EndSuperState().
					Initial("outer").
					Transition("running").On("poke").GoTo("running").ForwardTo(childID)
			}
			m := invokes.Quench()
			var children []*recordingActor
			palette := map[string]state.ActorBehavior{
				"child": func(map[string]any) (state.ActorInstance, error) {
					children = append(children, &recordingActor{})
					return children[len(children)-1], nil
				},
			}
			opts := []stepOption{
				durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fetch)),
				durable.WithActorPalette[string, string, stepCtx](palette),
			}
			if step == "DeliverToActor" {
				palette["driver"] = func(map[string]any) (state.ActorInstance, error) {
					return &sender{ev: "poke"}, nil
				}
			}
			start := "loading"
			if step == "DeliverToActor" {
				start = "running"
			}
			store, id := durable.NewMemStore(), durable.InstanceID("driven-forward")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState(start))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			// The driver's start message fired the parent's "poke" transition at
			// Start; the child it forwarded to holds what arrived. The service settle
			// fires "loaded", whose transition forwards it.
			want := "poke"
			if step == "RunService" {
				want = "loaded"
				if _, ran, runErr := h.RunService(ctx, state.InvokeID(machine, "loading", 0)); !ran || runErr != nil {
					t.Fatalf("RunService = %v, %v", ran, runErr)
				}
			}
			if len(children) != 1 || fmt.Sprint(children[0].got) != fmt.Sprint([]any{want}) {
				t.Fatalf("live child got %v, want [%s]: the forwardTo forwarded nil, not the event that drove the transition", children[0].got, want)
			}
			// A Recover respawns the child fresh. The settle's Fire is journaled and
			// replayed without actor deliveries, so the RunService child holds nothing
			// replayed to it, as for a host Fire's forward. The driver's start message
			// is another matter: no step followed it, so the journal holds nothing of
			// it, and the Recover respawns every actor as new, whose start re-drives
			// the parent Fire and its forward, as Start did live.
			if _, err = durable.Recover(ctx, m, store, id, opts...); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			wantRecovered := []any{}
			if step == "DeliverToActor" {
				wantRecovered = []any{want}
			}
			if len(children) != 2 || fmt.Sprint(children[1].got) != fmt.Sprint(wantRecovered) {
				t.Fatalf("recovered: %d children, the new one got %v, want %v", len(children), children[len(children)-1].got, wantRecovered)
			}
		})
	}
}
