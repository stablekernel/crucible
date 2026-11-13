package durable_test

// The Recover tests for the messages an actor sends its parent as it spawns: every step
// and every Start or Recover journals them, and Recover spawns a running actor again
// without sending them again, so each applies exactly once.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

type greetOption = durable.Option[string, string, actorCtx]

// greetMachine spawns a greeter in "greeting", entered by "go" from idle, by the
// onDone of the finisher that "working" runs, or by the onDone of the service that
// "loading" invokes. The greeter's hello is noted and, with
// leave, moves to "greeted", which stops the greeter; without it, the greeter keeps
// running.
func greetMachine(leave bool) *state.Machine[string, string, actorCtx] {
	note := func(s string) func(state.AssignCtx[actorCtx]) actorCtx {
		return func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			c.Notes = append(c.Notes, s)
			return c
		}
	}
	target := "greeting"
	if leave {
		target = "greeted"
	}
	return state.ForgeFor[actorCtx]("greet").
		Service("fetch", fetchOK).
		Reducer("noteHello", note("hello")).
		Reducer("notePoke", note("poke")).
		Actor("greeter").
		Actor("spawnChild").
		State("idle").
		State("working").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone")).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("fetched")).
		State("greeting").InvokeActor("greeter").
		State("greeted").
		Initial("idle").
		Transition("idle").On("go").GoTo("greeting").
		Transition("working").On("childDone").GoTo("greeting").
		Transition("loading").On("fetched").GoTo("greeting").
		Transition("greeting").On("hello").GoTo(target).Assign("noteHello").
		Transition("greeting").On("poke").GoTo("greeting").Assign("notePoke").
		Transition("greeted").On("poke").GoTo("greeted").Assign("notePoke").
		Quench()
}

// fetchOK is a service that succeeds.
func fetchOK(context.Context, state.ServiceCtx[actorCtx]) (any, error) { return "ok", nil }

// greetPalette binds a greeter, or one that can't start, and the finisher.
func greetPalette(greeterStarts bool) greetOption {
	var runs int64
	greeter := func(map[string]any) (state.ActorInstance, error) { return &greeter{}, nil }
	if !greeterStarts {
		greeter = failingSpawn(nil)
	}
	return durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
		"greeter":    greeter,
		"spawnChild": counterChild(&runs),
	})
}

// wantHellos fails unless the instance's notes hold hello exactly once.
func wantHellos(t *testing.T, what string, h *durable.Handle[string, string, actorCtx]) {
	t.Helper()
	hellos := 0
	for _, n := range h.Instance().Entity().Notes {
		if n == "hello" {
			hellos++
		}
	}
	if hellos != 1 {
		t.Fatalf("%s: notes = %v, want the spawn's hello once", what, h.Instance().Entity().Notes)
	}
}

// TestRecover_AppliesEachSpawnMessageOnce proves the message a greeter sends its parent
// as it spawns, by Start, by a Fire, or by the onDone a delivery or a service settle
// drove, is journaled and applied exactly once across repeated Recovers, whether the
// greeter keeps running (Recover spawns it again without the message) or the message
// ends it (Recover doesn't spawn it, and the journal alone holds the message), with and
// without checkpoints.
func TestRecover_AppliesEachSpawnMessageOnce(t *testing.T) {
	ctx := context.Background()
	for _, leave := range []bool{false, true} {
		for _, every := range []int{0, 1} {
			for _, by := range []string{"Start", "Fire", "onDone", "RunService"} {
				t.Run(fmt.Sprintf("%s/leave=%v/every=%d", by, leave, every), func(t *testing.T) {
					m := greetMachine(leave)
					opts := []greetOption{greetPalette(true), durable.WithServiceRegistry[string, string, actorCtx](state.NewRegistry[actorCtx]().Service("fetch", fetchOK))}
					if every > 0 {
						opts = append(opts, durable.WithCheckpointEvery[string, string, actorCtx](every))
					}
					store, id := durable.NewMemStore(), durable.InstanceID("greet")
					initial := map[string]string{"Start": "greeting", "Fire": "idle", "onDone": "working", "RunService": "loading"}[by]
					h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState(initial))
					if err != nil {
						t.Fatalf("Start: %v", err)
					}
					switch by {
					case "Fire":
						if _, err = h.Fire(ctx, "go"); err != nil {
							t.Fatalf("Fire(go): %v", err)
						}
					case "onDone":
						if found, deliverErr := h.DeliverToActor(ctx, state.ActorRef{ID: state.ActorID("greet", "working", 0)}, "finish"); !found || deliverErr != nil {
							t.Fatalf("DeliverToActor = %v, %v", found, deliverErr)
						}
					case "RunService":
						if _, ran, runErr := h.RunService(ctx, state.InvokeID("greet", "loading", 0)); !ran || runErr != nil {
							t.Fatalf("RunService = %v, %v", ran, runErr)
						}
					}
					if _, err = h.Fire(ctx, "poke"); err != nil {
						t.Fatalf("Fire(poke): %v", err)
					}
					wantHellos(t, "live", h)
					liveState := h.Instance().Current()
					for round := 1; round <= 2; round++ {
						recovered, err := durable.Recover(ctx, m, store, id, opts...)
						if err != nil {
							t.Fatalf("Recover %d: %v", round, err)
						}
						wantHellos(t, fmt.Sprintf("Recover %d", round), recovered)
						if got := recovered.Instance().Current(); got != liveState {
							t.Fatalf("Recover %d state = %q, want the live %q", round, got, liveState)
						}
						if _, err = recovered.Fire(ctx, "poke"); err != nil {
							t.Fatalf("Fire(poke) after Recover %d: %v", round, err)
						}
					}
				})
			}
		}
	}
}

// TestRecover_AnUnstartedActorsSpawnMessageApplies proves an actor whose spawn failed
// with no onError, so it never ran, sends its spawn message live when Recover retries
// the spawn, and the next step journals it: each Recover before that step applies it
// live once, and one after it replays it once without sending it again.
func TestRecover_AnUnstartedActorsSpawnMessageApplies(t *testing.T) {
	ctx := context.Background()
	m := greetMachine(false)
	store, id := durable.NewMemStore(), durable.InstanceID("unstarted")
	h, err := durable.NewRunner(m, store, greetPalette(false)).Start(ctx, id, actorCtx{}, state.WithInitialState("greeting"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	if notes := h.Instance().Entity().Notes; len(notes) != 1 {
		t.Fatalf("live notes = %v, want only the poke, since the greeter never started", notes)
	}
	var recovered *durable.Handle[string, string, actorCtx]
	for round := 1; round <= 2; round++ {
		if recovered, err = durable.Recover(ctx, m, store, id, greetPalette(true)); err != nil {
			t.Fatalf("Recover %d: %v", round, err)
		}
		wantHellos(t, fmt.Sprintf("Recover %d, before a step", round), recovered)
	}
	if _, err = recovered.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke) after Recover: %v", err)
	}
	again, err := durable.Recover(ctx, m, store, id, greetPalette(true))
	if err != nil {
		t.Fatalf("Recover after the step: %v", err)
	}
	wantHellos(t, "Recover after the step", again)
}

// TestDeliverToActor_JournalsEachFireUnderItsActor proves a delivery journals each
// parent Fire it drove under the actor that drove it: the finisher's onDone under the
// finisher, and the hello of the greeter that onDone spawned under the greeter.
func TestDeliverToActor_JournalsEachFireUnderItsActor(t *testing.T) {
	ctx := context.Background()
	store, id := durable.NewMemStore(), durable.InstanceID("correlate")
	h, err := durable.NewRunner(greetMachine(false), store, greetPalette(true)).Start(ctx, id, actorCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	finisher := state.ActorID("greet", "working", 0)
	if found, deliverErr := h.DeliverToActor(ctx, state.ActorRef{ID: finisher}, "finish"); !found || deliverErr != nil {
		t.Fatalf("DeliverToActor = %v, %v", found, deliverErr)
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var got []string
	for _, rec := range tail {
		for _, e := range rec.Entries {
			if e.Kind == state.JournalActorMessage {
				got = append(got, e.CorrelationID)
			}
		}
	}
	want := fmt.Sprint([]string{finisher, state.ActorID("greet", "greeting", 0)})
	if fmt.Sprint(got) != want {
		t.Fatalf("journaled actor entries under %v, want %s", got, want)
	}
}

// sender is an actor that sends its parent ev from its initial entry, then keeps
// running.
type sender struct {
	ev   string
	sent bool
}

func (s *sender) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (s *sender) ChildEffects() []state.Effect {
	if s.sent {
		return nil
	}
	s.sent = true
	return []state.Effect{state.SendParent{Event: s.ev}}
}
func (s *sender) Output() any { return nil }

// TestRecover_ARetriedSpawnsMessageDoesNotRestartARunningActor proves Recover spawns the
// actors that were running before it retries an unstarted one. The parallel state
// declares the hopper, which can't start live, ahead of the greeter that one region
// declares as "v". When Recover's retry starts the hopper, its hop enters the other
// region's state, which declares "v" too. The greeter must already be running there,
// so that spawn is a no-op and its journaled hello applies once, across two Recovers.
func TestRecover_ARetriedSpawnsMessageDoesNotRestartARunningActor(t *testing.T) {
	ctx := context.Background()
	note := func(s string) func(state.AssignCtx[actorCtx]) actorCtx {
		return func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			c.Notes = append(c.Notes, s)
			return c
		}
	}
	m := state.ForgeFor[actorCtx]("cascade").
		Reducer("noteHello", note("hello")).
		Reducer("notePoke", note("poke")).
		Actor("hopper").
		Actor("greeter").
		State("off").
		SuperState("par").InvokeActor("hopper").
		Region("left").
		Initial("p1").
		SubState("p1").InvokeActor("greeter", state.WithInvokeID("v")).
		EndRegion().
		Region("right").
		Initial("q1").
		SubState("q1").
		SubState("q2").InvokeActor("greeter", state.WithInvokeID("v")).
		EndRegion().
		EndSuperState().
		Initial("off").
		Transition("off").On("go").GoTo("par").
		Transition("p1").On("hello").GoTo("p1").Assign("noteHello").
		Transition("p1").On("poke").GoTo("p1").Assign("notePoke").
		Transition("q1").On("hop").GoTo("q2").
		Quench()
	palette := func(hopperStarts bool) greetOption {
		hopper := failingSpawn(nil)
		if hopperStarts {
			hopper = func(map[string]any) (state.ActorInstance, error) { return &sender{ev: "hop"}, nil }
		}
		return durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"hopper":  hopper,
			"greeter": func(map[string]any) (state.ActorInstance, error) { return &sender{ev: "hello"}, nil },
		})
	}
	store, id := durable.NewMemStore(), durable.InstanceID("cascade")
	h, err := durable.NewRunner(m, store, palette(false)).Start(ctx, id, actorCtx{}, state.WithInitialState("off"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	wantHellos(t, "live", h)
	for round := 1; round <= 2; round++ {
		recovered, err := durable.Recover(ctx, m, store, id, palette(true))
		if err != nil {
			t.Fatalf("Recover %d: %v", round, err)
		}
		wantHellos(t, fmt.Sprintf("Recover %d", round), recovered)
		if got := fmt.Sprint(recovered.Instance().Configuration()); got != "[p1 q2]" {
			t.Fatalf("Recover %d configuration = %s, want [p1 q2] after the hopper's hop", round, got)
		}
		if _, err = recovered.Fire(ctx, "poke"); err != nil {
			t.Fatalf("Fire(poke) after Recover %d: %v", round, err)
		}
	}
}
