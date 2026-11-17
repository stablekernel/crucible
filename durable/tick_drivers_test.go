package durable_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestTick_ATimerThatLeavesAServiceStateStopsTheService pins that a timer-driven
// transition out of a state stops the service that state invokes, as a host Fire's
// exit does: after the Tick, RunService reports no service in flight.
func TestTick_ATimerThatLeavesAServiceStateStopsTheService(t *testing.T) {
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

// TestRecover_AnOlderVersionsTickReplaysAsItRan pins that replay hands a tick's timer
// transitions to the service runner only for a Record of a format that did so live: an
// older version's Tick left the service of a state its timer left in flight, so a
// journal of format 1 replays with the service still in flight, as that run had it,
// and a later settle that run journaled still replays. This version's journal replays
// with the service stopped, as its live Tick left it.
func TestRecover_AnOlderVersionsTickReplaysAsItRan(t *testing.T) {
	ctx := context.Background()
	fetch := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { return "ok", nil }
	m := state.ForgeFor[stepCtx]("tickExit").
		Service("fetch", fetch).
		State("idle").
		State("fetching").Invoke("fetch", state.WithInvokeOnDone("fetched")).
		State("timedOut").
		Initial("idle").
		Transition("idle").On("begin").GoTo("fetching").
		Transition("fetching").After(5 * time.Second).On("timeout").GoTo("timedOut").
		Quench()
	clk := state.NewFakeClock(epoch)
	opts := []stepOption{
		durable.WithRunnerClock[string, string, stepCtx](clk),
		durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fetch)),
	}
	st := durable.NewMemStore()
	h, err := durable.NewRunner(m, st, opts...).Start(ctx, "i1", stepCtx{}, state.WithInitialState("idle"))
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
	id := state.InvokeID("tickExit", "fetching", 0)

	snap, recs, err := st.Load(ctx, "i1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	old := durable.NewMemStore()
	if err = old.Checkpoint(ctx, "i1", withCheckpointFormat(t, snap, 1), -1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	for _, rec := range recs {
		rec.Format = 1
		if _, err = old.Append(ctx, "i1", rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	cur, err := durable.Recover(ctx, m, st, "i1", opts...)
	if err != nil {
		t.Fatalf("Recover this version's journal: %v", err)
	}
	if _, ran, rerr := cur.RunService(ctx, id); ran || rerr != nil {
		t.Fatalf("RunService after recovering this version's journal = %v, %v; want nothing in flight", ran, rerr)
	}
	oh, err := durable.Recover(ctx, m, old, "i1", opts...)
	if err != nil {
		t.Fatalf("Recover the older journal: %v", err)
	}
	if _, ran, rerr := oh.RunService(ctx, id); !ran || rerr != nil {
		t.Fatalf("RunService after recovering the older journal = %v, %v; want the service in flight, as that version's Tick left it", ran, rerr)
	}
	if _, err = durable.Recover(ctx, m, old, "i1", opts...); err != nil {
		t.Fatalf("Recover after the settle the older run left in flight: %v", err)
	}
}

// TestRecover_AnOlderVersionsTickThenACheckpointAndASettle_FailsRecover pins today's
// upgrade path for a journal an older version wrote, of format 0 or 1, whose tick left
// fetching with fetch in flight, as that version's Tick did. Recover replays the tick
// as it ran, so the recovered Handle still has fetch in flight. Its next checkpoint, of
// this version's format, holds only the configuration's services, and timedOut declares
// none, so fetch isn't in it. RunService settles fetch after that checkpoint, and the
// settle's Record follows it in the tail, so every later Recover fails replaying a
// settle with no service in flight. This pins the current behavior for a decision: a
// Recover that stops, after its walk, the services such a tick left in flight would
// recover, as would replaying the tick with this version's rules, though that one fails
// on an older tail that settles fetch after the tick.
func TestRecover_AnOlderVersionsTickThenACheckpointAndASettle_FailsRecover(t *testing.T) {
	for _, format := range []int{0, 1} {
		t.Run(fmt.Sprintf("format %d", format), func(t *testing.T) {
			ctx := context.Background()
			fetch := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { return "ok", nil }
			m := state.ForgeFor[stepCtx]("tickUpgrade").
				Service("fetch", fetch).
				State("idle").
				State("fetching").Invoke("fetch", state.WithInvokeOnDone("fetched")).
				State("timedOut").
				Initial("idle").
				Transition("idle").On("begin").GoTo("fetching").
				Transition("fetching").After(5 * time.Second).On("timeout").GoTo("timedOut").
				Transition("timedOut").On("poke").GoTo("timedOut").
				Quench()
			clk := state.NewFakeClock(epoch)
			opts := []stepOption{
				durable.WithRunnerClock[string, string, stepCtx](clk),
				durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fetch)),
			}
			st := durable.NewMemStore()
			h, err := durable.NewRunner(m, st, opts...).Start(ctx, "i1", stepCtx{}, state.WithInitialState("idle"))
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
			snap, recs, err := st.Load(ctx, "i1")
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			old := durable.NewMemStore()
			if err = old.Checkpoint(ctx, "i1", withCheckpointFormat(t, snap, format), -1); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			for _, rec := range recs {
				rec.Format = format
				if _, err = old.Append(ctx, "i1", rec); err != nil {
					t.Fatalf("Append: %v", err)
				}
			}

			// Checkpoint after poke, not after the settle that follows it.
			checkpointing := append(opts[:len(opts):len(opts)], durable.WithCheckpointEvery[string, string, stepCtx](2))
			oh, err := durable.Recover(ctx, m, old, "i1", checkpointing...)
			if err != nil {
				t.Fatalf("Recover the older journal: %v", err)
			}
			if _, err = oh.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke): %v", err)
			}
			if n := tailLen(t, old, "i1"); n != 0 {
				t.Fatalf("setup: %d records after poke, want a checkpoint that compacted the tail", n)
			}
			id := state.InvokeID("tickUpgrade", "fetching", 0)
			if _, ran, rerr := oh.RunService(ctx, id); !ran || rerr != nil {
				t.Fatalf("RunService = %v, %v; want fetch in flight, as the older Tick left it", ran, rerr)
			}
			if n := tailLen(t, old, "i1"); n != 1 {
				t.Fatalf("setup: %d records after the settle, want only the settle past the checkpoint", n)
			}
			_, err = durable.Recover(ctx, m, old, "i1", opts...)
			if err == nil || !strings.Contains(err.Error(), "no in-flight service to settle") {
				t.Fatalf("Recover after the settle = %v; today it fails with no in-flight service to settle", err)
			}
		})
	}
}

// TestTick_AnActorsParentFireRefusedByAContext_RecordsNothing pins the ctx rule for a
// parent Fire an actor drives as a Tick spawns it: a timer enters greeting, whose
// greeter sends hello as it starts, and a middleware refuses hello under a context it
// derived and ended. Live never applied hello, so the Tick fails the Handle and records
// nothing, as Fire does: a recovered Handle re-arms the timer rather than replay a tick
// whose actor Fire was cut short.
func TestTick_AnActorsParentFireRefusedByAContext_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	note := func(in state.AssignCtx[stepCtx]) stepCtx {
		c := in.Entity
		c.Notes = append(c.Notes, "hello")
		return c
	}
	cancelOnHello := func(next state.FireFunc[string, string, stepCtx]) state.FireFunc[string, string, stepCtx] {
		return func(ctx context.Context, event string) state.FireResult[string] {
			if event == "hello" {
				derived, cancel := context.WithCancel(ctx)
				cancel()
				return next(derived, event)
			}
			return next(ctx, event)
		}
	}
	m := state.ForgeFor[stepCtx]("tickEnter").
		Use(cancelOnHello).
		Reducer("note", note).
		Actor("greeter").
		State("idle").
		State("waiting").
		State("greeting").InvokeActor("greeter").
		Initial("idle").
		Transition("idle").On("go").GoTo("waiting").
		Transition("waiting").After(5 * time.Second).On("due").GoTo("greeting").
		Transition("greeting").On("hello").GoTo("greeting").Assign("note").
		Quench()
	clk := state.NewFakeClock(epoch)
	opts := []stepOption{
		durable.WithRunnerClock[string, string, stepCtx](clk),
		durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
			"greeter": func(map[string]any) (state.ActorInstance, error) { return &reentryGreeter{}, nil },
		}),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("tick-refused")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	before := tailLen(t, store, id)
	clk.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Tick = %v, want ErrHandleFailed wrapping context.Canceled", err)
	}
	if after := tailLen(t, store, id); after != before {
		t.Fatalf("journal grew from %d to %d records, want nothing recorded", before, after)
	}
	rh, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := rh.Instance().Current(); got != "waiting" {
		t.Fatalf("recovered in %q, want waiting: the tick wasn't recorded", got)
	}
}

// TestTick_ATimerThatLeavesAnActorStateStopsTheActor pins that a timer-driven transition
// out of a state stops the actor that state spawned, as a Fire's exit does, and that a
// Handle recovered from the journal agrees: neither has the actor running.
func TestTick_ATimerThatLeavesAnActorStateStopsTheActor(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[stepCtx]("tickLeave").
		Actor("greeter").
		State("idle").
		State("greeting").InvokeActor("greeter").
		State("quiet").
		Initial("idle").
		Transition("idle").On("go").GoTo("greeting").
		Transition("greeting").On("hello").GoTo("greeting").
		Transition("greeting").After(5 * time.Second).On("hush").GoTo("quiet").
		Quench()
	clk := state.NewFakeClock(epoch)
	opts := []stepOption{
		durable.WithRunnerClock[string, string, stepCtx](clk),
		durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
			"greeter": func(map[string]any) (state.ActorInstance, error) { return &reentryGreeter{}, nil },
		}),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("tick-leave")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	actor := state.ActorID("tickLeave", "greeting", 0)
	if _, ok := h.ActorRef(actor); !ok {
		t.Fatal("entering greeting spawned no greeter")
	}
	clk.Advance(6 * time.Second)
	if _, err = h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if got := h.Instance().Current(); got != "quiet" {
		t.Fatalf("state = %q, want quiet", got)
	}
	if _, ok := h.ActorRef(actor); ok {
		t.Fatal("the greeter still runs after the timer left greeting")
	}
	rh, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, ok := rh.ActorRef(actor); ok {
		t.Fatal("the recovered Handle runs the greeter, which the live Tick stopped")
	}
}

// TestTick_AnActorsParentFireCallingBack_IsRefused pins that a Tick drives the actors a
// timer spawns under its step's context, as Fire does: a greeter a state's entry spawns
// sends hello as it starts, and a middleware calls back into the Handle with the
// context hello's parent Fire carries. On Fire and on Tick that call gets
// ErrReentrantStep at once, instead of waiting on the step lock its step holds, which
// deadlocks.
func TestTick_AnActorsParentFireCallingBack_IsRefused(t *testing.T) {
	for _, via := range []string{"Fire", "Tick"} {
		t.Run(via, func(t *testing.T) {
			ctx := context.Background()
			var h *durable.Handle[string, string, stepCtx]
			var viewErr error
			viewOnHello := func(next state.FireFunc[string, string, stepCtx]) state.FireFunc[string, string, stepCtx] {
				return func(ctx context.Context, event string) state.FireResult[string] {
					if event == "hello" {
						viewErr = h.View(ctx, func(*state.Instance[string, string, stepCtx]) {})
					}
					return next(ctx, event)
				}
			}
			m := state.ForgeFor[stepCtx]("tickCallback").
				Use(viewOnHello).
				Actor("greeter").
				State("idle").
				State("waiting").
				State("greeting").InvokeActor("greeter").
				Initial("idle").
				Transition("idle").On("go").GoTo("waiting").
				Transition("waiting").On("now").GoTo("greeting").
				Transition("waiting").After(5 * time.Second).On("due").GoTo("greeting").
				Transition("greeting").On("hello").GoTo("greeting").
				Quench()
			clk := state.NewFakeClock(epoch)
			h, err := durable.NewRunner(m, durable.NewMemStore(),
				durable.WithRunnerClock[string, string, stepCtx](clk),
				durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
					"greeter": func(map[string]any) (state.ActorInstance, error) { return &reentryGreeter{}, nil },
				}),
			).Start(ctx, "tick-callback", stepCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "go"); err != nil {
				t.Fatalf("Fire(go): %v", err)
			}
			var stepErr error
			done := make(chan struct{})
			go func() {
				defer close(done)
				if via == "Fire" {
					_, stepErr = h.Fire(ctx, "now")
					return
				}
				clk.Advance(6 * time.Second)
				_, stepErr = h.Tick(ctx)
			}()
			waitFor(t, done, via)
			if stepErr != nil {
				t.Fatalf("%s: %v", via, stepErr)
			}
			if !errors.Is(viewErr, durable.ErrReentrantStep) {
				t.Fatalf("the middleware's View = %v, want ErrReentrantStep", viewErr)
			}
			if got := h.Instance().Current(); got != "greeting" {
				t.Fatalf("state = %q, want greeting", got)
			}
		})
	}
}
