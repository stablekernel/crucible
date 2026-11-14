package durable_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestActorDrivenFire_ArmsServicesAndTimers proves a state that an actor-driven parent
// Fire enters, by the onDone a delivery settles or by the message an actor sends as Start
// spawns it, has its service in flight and its timer armed, live and on Recover, with and
// without checkpoints: RunService runs the service and a Tick at the deadline fires the
// timer, as for a state a Fire enters. So does one a service's settle enters. Recover
// runs at the deadline, and a checkpoint lands on the step that enters armed, so a clock
// read that step left to the next would move the deadline.
func TestActorDrivenFire_ArmsServicesAndTimers(t *testing.T) {
	ctx := context.Background()
	m := armedMachine()
	svcID := state.InvokeID("armed", "armed", 0)
	for _, by := range []string{"delivery", "Start", "RunService"} {
		for _, where := range []string{"live", "Recover", "Recover/every=2"} {
			for _, check := range []string{"service", "timer"} {
				t.Run(fmt.Sprintf("%s/%s/%s", by, where, check), func(t *testing.T) {
					clk := state.NewFakeClock(epoch)
					opts := armedOptions(clk, true)
					if where == "Recover/every=2" {
						// The checkpoint lands on the step that enters armed, so the tail
						// holds only the poke after it.
						opts = append(opts, durable.WithCheckpointEvery[string, string, actorCtx](2))
					}
					store, id := durable.NewMemStore(), durable.InstanceID("armed")
					initial := map[string]string{"delivery": "working", "Start": "going", "RunService": "loading"}[by]
					h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState(initial))
					if err != nil {
						t.Fatalf("Start: %v", err)
					}
					if _, err = h.Fire(ctx, "poke"); err != nil {
						t.Fatalf("first Fire(poke): %v", err)
					}
					switch by {
					case "delivery":
						if found, deliverErr := h.DeliverToActor(ctx, state.ActorRef{ID: state.ActorID("armed", "working", 0)}, "finish"); !found || deliverErr != nil {
							t.Fatalf("DeliverToActor = %v, %v", found, deliverErr)
						}
					case "RunService":
						if _, ran, runErr := h.RunService(ctx, state.InvokeID("armed", "loading", 0)); !ran || runErr != nil {
							t.Fatalf("RunService(loading) = %v, %v", ran, runErr)
						}
					}
					if _, err = h.Fire(ctx, "poke"); err != nil {
						t.Fatalf("Fire(poke): %v", err)
					}
					if check == "timer" {
						clk.Advance(10 * time.Second)
					}
					if where != "live" {
						if h, err = durable.Recover(ctx, m, store, id, opts...); err != nil {
							t.Fatalf("Recover: %v", err)
						}
					}
					want := "fetched"
					switch check {
					case "service":
						if _, ran, runErr := h.RunService(ctx, svcID); !ran || runErr != nil {
							t.Fatalf("RunService = %v, %v, want it to run the service the state invokes", ran, runErr)
						}
					case "timer":
						want = "timedOut"
						if _, tickErr := h.Tick(ctx); tickErr != nil {
							t.Fatalf("Tick: %v", tickErr)
						}
					}
					if got := h.Instance().Current(); got != want {
						t.Fatalf("state = %q, want %q", got, want)
					}
				})
			}
		}
	}
}

// armedMachine enters "armed", which invokes a service and arms a 10s timer, by the
// onDone of the finisher "working" runs, by the "go" the goer "going" runs sends as it
// starts, or by the onDone of the service "loading" invokes.
func armedMachine() *state.Machine[string, string, actorCtx] {
	return state.ForgeFor[actorCtx]("armed").
		Service("fetch", fetchOK).
		Actor("finisher").
		Actor("goer").
		State("working").InvokeActor("finisher", state.WithInvokeOnDone("childDone")).
		State("going").InvokeActor("goer").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("loaded")).
		State("armed").Invoke("fetch", state.WithInvokeOnDone("fetched")).
		State("fetched").
		State("timedOut").
		Initial("working").
		Transition("working").On("childDone").GoTo("armed").
		Transition("working").On("poke").GoTo("working").
		Transition("loading").On("poke").GoTo("loading").
		Transition("going").On("go").GoTo("armed").
		Transition("loading").On("loaded").GoTo("armed").
		Transition("armed").On("poke").GoTo("armed").
		Transition("armed").On("fetched").GoTo("fetched").
		Transition("armed").After(10 * time.Second).On("elapsed").GoTo("timedOut").
		Quench()
}

// armedOptions wires clk, the fetch service, and the finisher and goer, with a goer that
// can't start unless goerStarts.
func armedOptions(clk state.Clock, goerStarts bool) []greetOption {
	goer := failingSpawn(nil)
	if goerStarts {
		goer = func(map[string]any) (state.ActorInstance, error) { return &sender{ev: "go"}, nil }
	}
	return []greetOption{
		durable.WithRunnerClock[string, string, actorCtx](clk),
		durable.WithServiceRegistry[string, string, actorCtx](state.NewRegistry[actorCtx]().Service("fetch", fetchOK)),
		durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"finisher": finishingChild(),
			"goer":     goer,
		}),
	}
}

// TestRecover_ASpawnsFireArmsATimerAtItsRead proves a timer a parent Fire arms when
// Recover's retried spawn drives it, here the go of a goer that couldn't start live, is
// due 10s after the clock read Recover took, and that the next step journals that read:
// a later Recover, its clock 5s past that read, replays the Fire's arming at it, so the
// timer comes due 5s on, and the service the state invokes is in flight.
func TestRecover_ASpawnsFireArmsATimerAtItsRead(t *testing.T) {
	ctx := context.Background()
	m := armedMachine()
	clk := state.NewFakeClock(epoch)
	store, id := durable.NewMemStore(), durable.InstanceID("retried")
	if _, err := durable.NewRunner(m, store, armedOptions(clk, false)...).Start(ctx, id, actorCtx{}, state.WithInitialState("going")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	clk.Advance(time.Hour)
	h, err := durable.Recover(ctx, m, store, id, armedOptions(clk, true)...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := h.Instance().Current(); got != "armed" {
		t.Fatalf("Recover left %q, want armed by the retried goer's go", got)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	clk.Advance(5 * time.Second)
	again, err := durable.Recover(ctx, m, store, id, armedOptions(clk, true)...)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	clk.Advance(5 * time.Second)
	if _, err = again.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := again.Instance().Current(); got != "timedOut" {
		t.Fatalf("state = %q, want timedOut 10s after the first Recover's read", got)
	}
}

// TestTick_DropsTheTimerItFiredPastAStartsRead proves a Tick reconciles the Handle's
// timer table at its own clock read, not at an earlier read the step also records, here
// the one Start took arming the timer a starting actor's go entered. The timer's
// transition stays in its state, so no cancel drops it: the tick must, or the checkpoint
// holds it and a recovered Handle fires it again.
func TestTick_DropsTheTimerItFiredPastAStartsRead(t *testing.T) {
	ctx := context.Background()
	m := tickingMachine()
	clk := state.NewFakeClock(epoch)
	opts := append(armedOptions(clk, true), durable.WithCheckpointEvery[string, string, actorCtx](1))
	store, id := durable.NewMemStore(), durable.InstanceID("tick")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("going"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	clk.Advance(10 * time.Second)
	if results, tickErr := h.Tick(ctx); tickErr != nil || len(results) != 1 {
		t.Fatalf("Tick = %d results, %v, want the timer to fire", len(results), tickErr)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if results, tickErr := recovered.Tick(ctx); tickErr != nil || len(results) != 0 {
		t.Fatalf("recovered Tick = %d results, %v, want none: the timer already fired", len(results), tickErr)
	}
}

// tickingMachine enters "ticking" by the go the goer "going" runs sends as it starts.
// Its 10s timer's transition stays in "ticking", so nothing cancels the timer.
func tickingMachine() *state.Machine[string, string, actorCtx] {
	return state.ForgeFor[actorCtx]("tick").
		Actor("goer").
		State("going").InvokeActor("goer").
		State("ticking").
		Initial("going").
		Transition("going").On("go").GoTo("ticking").
		Transition("ticking").After(10 * time.Second).On("elapsed").GoTo("ticking").
		Transition("ticking").On("poke").GoTo("ticking").
		Quench()
}

// TestRecover_DropsTheTimerAReplayedTickFired proves Recover's replay of a tick drops the
// timer it fired from the recovered Handle's timer table at the tick's own clock read,
// not at the earlier read Start took arming it, which the tick's step also records: a
// checkpoint the recovered Handle takes doesn't hold the timer, so a Recover from it
// doesn't fire it again.
func TestRecover_DropsTheTimerAReplayedTickFired(t *testing.T) {
	ctx := context.Background()
	m := tickingMachine()
	clk := state.NewFakeClock(epoch)
	store, id := durable.NewMemStore(), durable.InstanceID("replayedTick")
	h, err := durable.NewRunner(m, store, armedOptions(clk, true)...).Start(ctx, id, actorCtx{}, state.WithInitialState("going"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	clk.Advance(10 * time.Second)
	if results, tickErr := h.Tick(ctx); tickErr != nil || len(results) != 1 {
		t.Fatalf("Tick = %d results, %v, want the timer to fire", len(results), tickErr)
	}
	checkpointing := append(armedOptions(clk, true), durable.WithCheckpointEvery[string, string, actorCtx](1))
	if h, err = durable.Recover(ctx, m, store, id, checkpointing...); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	if h, err = durable.Recover(ctx, m, store, id, checkpointing...); err != nil {
		t.Fatalf("Recover from the checkpoint: %v", err)
	}
	if results, tickErr := h.Tick(ctx); tickErr != nil || len(results) != 0 {
		t.Fatalf("Tick = %d results, %v, want none: the timer already fired", len(results), tickErr)
	}
}

// TestRecover_KeepsAReplayedActorFiresTimerDeadline proves the deadline of a timer that
// replay arms for an actor-driven parent Fire, here the go a goer sent as Start spawned
// it, is the one the live run armed: a checkpoint the recovered Handle takes persists
// it, so a Recover from that checkpoint fires the timer at its deadline, not before.
func TestRecover_KeepsAReplayedActorFiresTimerDeadline(t *testing.T) {
	ctx := context.Background()
	m := armedMachine()
	clk := state.NewFakeClock(epoch)
	store, id := durable.NewMemStore(), durable.InstanceID("deadline")
	h, err := durable.NewRunner(m, store, armedOptions(clk, true)...).Start(ctx, id, actorCtx{}, state.WithInitialState("going"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	clk.Advance(5 * time.Second)
	checkpointing := append(armedOptions(clk, true), durable.WithCheckpointEvery[string, string, actorCtx](1))
	if h, err = durable.Recover(ctx, m, store, id, checkpointing...); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke) after Recover: %v", err)
	}
	if h, err = durable.Recover(ctx, m, store, id, checkpointing...); err != nil {
		t.Fatalf("Recover from the checkpoint: %v", err)
	}
	if results, tickErr := h.Tick(ctx); tickErr != nil || len(results) != 0 {
		t.Fatalf("Tick 5s before the deadline = %d results, %v, want none", len(results), tickErr)
	}
	clk.Advance(5 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick at the deadline: %v", err)
	}
	if got := h.Instance().Current(); got != "timedOut" {
		t.Fatalf("state = %q, want timedOut at the deadline", got)
	}
}

// TestRecover_StartsAServiceAnActorsMessageReentered proves a service that settled, then
// started again because an actor's message re-entered its state, is recorded as in
// flight, not settled, so Recover starts it: by a checkpoint the live run took, and by
// one a recovered Handle took after replaying the run.
func TestRecover_StartsAServiceAnActorsMessageReentered(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[actorCtx]("relay").
		Service("fetch", fetchOK).
		Actor("pinger").
		SuperState("outer").InvokeActor("pinger").
		SubState("fetching").Invoke("fetch", state.WithInvokeOnDone("fetched")).
		Initial("fetching").
		EndSuperState().
		Initial("outer").
		Transition("fetching").On("fetched").GoTo("fetching").
		Transition("fetching").On("pong").GoTo("fetching").Reenter().
		Transition("fetching").On("poke").GoTo("fetching").
		Quench()
	svcID := state.InvokeID("relay", "fetching", 0)
	for _, where := range []string{"live checkpoint", "replayed checkpoint"} {
		t.Run(where, func(t *testing.T) {
			var msgs int64
			opts := []greetOption{
				durable.WithServiceRegistry[string, string, actorCtx](state.NewRegistry[actorCtx]().Service("fetch", fetchOK)),
				durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{"pinger": pingerChild(&msgs)}),
			}
			checkpointing := append(opts[:len(opts):len(opts)], durable.WithCheckpointEvery[string, string, actorCtx](1))
			live := checkpointing
			if where == "replayed checkpoint" {
				live = opts
			}
			store, id := durable.NewMemStore(), durable.InstanceID("relay")
			h, err := durable.NewRunner(m, store, live...).Start(ctx, id, actorCtx{}, state.WithInitialState("fetching"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, ran, runErr := h.RunService(ctx, svcID); !ran || runErr != nil {
				t.Fatalf("RunService = %v, %v", ran, runErr)
			}
			if found, deliverErr := h.DeliverToActorByID(ctx, state.ActorID("relay", "outer", 0), "ping"); !found || deliverErr != nil {
				t.Fatalf("DeliverToActorByID = %v, %v", found, deliverErr)
			}
			if where == "replayed checkpoint" {
				if h, err = durable.Recover(ctx, m, store, id, checkpointing...); err != nil {
					t.Fatalf("Recover from the journal: %v", err)
				}
			}
			if _, err = h.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke): %v", err)
			}
			recovered, err := durable.Recover(ctx, m, store, id, checkpointing...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if _, ran, runErr := recovered.RunService(ctx, svcID); !ran || runErr != nil {
				t.Fatalf("RunService after Recover = %v, %v, want the service the pong restarted in flight", ran, runErr)
			}
		})
	}
}

// TestRecover_ReplaysAnOlderJournalsActorFireWithoutArming proves a journal an older
// version wrote, which didn't arm the timers an actor-driven parent Fire's state
// declares and took no clock read for them, replays as it did, in Recover and in
// StateAt: the Fire arms nothing, so it doesn't take the read a later step recorded,
// and the timer that step armed comes due at its recorded deadline, when a recorded
// Tick fires it. The journal is this version's, put back in the older shape: no clock
// read for the onDone's timer, and no recorded actor sets.
func TestRecover_ReplaysAnOlderJournalsActorFireWithoutArming(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[actorCtx]("older").
		Actor("finisher").
		State("working").InvokeActor("finisher", state.WithInvokeOnDone("childDone")).
		State("armed").
		State("waiting").
		State("timedOut").
		State("late").
		Initial("working").
		Transition("working").On("childDone").GoTo("armed").
		Transition("armed").After(10 * time.Second).On("elapsed").GoTo("timedOut").
		Transition("armed").On("leave").GoTo("waiting").
		Transition("waiting").After(30 * time.Second).On("overdue").GoTo("late").
		Quench()
	clk := state.NewFakeClock(epoch)
	opts := []greetOption{
		durable.WithRunnerClock[string, string, actorCtx](clk),
		durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{"finisher": finishingChild()}),
	}
	written, id := durable.NewMemStore(), durable.InstanceID("older")
	h, err := durable.NewRunner(m, written, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if found, deliverErr := h.DeliverToActorByID(ctx, state.ActorID("older", "working", 0), "finish"); !found || deliverErr != nil {
		t.Fatalf("DeliverToActorByID = %v, %v", found, deliverErr)
	}
	clk.Advance(5 * time.Second)
	if _, err = h.Fire(ctx, "leave"); err != nil {
		t.Fatalf("Fire(leave): %v", err)
	}
	clk.Advance(30 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := h.Instance().Current(); got != "late" {
		t.Fatalf("live state = %q, want late", got)
	}

	baseline, tail, err := written.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 3 {
		t.Fatalf("journal holds %d Records, want the delivery, the leave and the tick", len(tail))
	}
	older := durable.NewMemStore()
	if err = older.Checkpoint(ctx, id, baseline, durable.BaselineStep); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	for i, rec := range tail {
		rec.EndedActors, rec.UnstartedActors = nil, nil
		if i == 0 {
			// The older version armed nothing for the onDone, so took no read.
			var entries []state.JournalEntry
			for _, e := range rec.Entries {
				if e.Kind != state.JournalClockRead {
					entries = append(entries, e)
				}
			}
			if len(entries) == len(rec.Entries) {
				t.Fatal("the delivery recorded no clock read for the onDone's timer")
			}
			rec.Entries = entries
		}
		if _, err = older.Append(ctx, id, rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	view, err := durable.StateAt(ctx, m, older, id, tail[2].Step+tail[2].TickSteps, opts...)
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if got := view.Instance().Current(); got != "late" {
		t.Fatalf("StateAt state = %q, want late: the recorded Tick should fire waiting's timer", got)
	}
	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "late" {
		t.Fatalf("recovered state = %q, want late: the recorded Tick should fire waiting's timer", got)
	}
}
