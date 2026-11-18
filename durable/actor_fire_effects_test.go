package durable_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// TestFire_DispatchesTheEffectsOfAParentFireAnActorDrove pins that a domain effect a
// parent Fire emits, where an actor drove that Fire, is dispatched like the event
// Fire's own: here a failed spawn's onError transition emits a notify.
func TestFire_DispatchesTheEffectsOfAParentFireAnActorDrove(t *testing.T) {
	ctx := context.Background()
	type cx struct{}
	m := state.ForgeFor[cx]("actorEffects").
		Action("notify", func(state.ActionCtx[cx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		Actor("child").
		State("idle").
		State("spawning").InvokeActor("child", state.WithInvokeOnError("spawnFailed")).
		State("failed").
		Initial("idle").
		Transition("idle").On("start").GoTo("spawning").
		Transition("spawning").On("spawnFailed").GoTo("failed").Do("notify").
		Quench()
	rec := newEffectRecorder()
	h, err := durable.NewRunner(m, durable.NewMemStore(),
		durable.WithEffectHandler[string, string, cx](rec.handler()),
		durable.WithActorPalette[string, string, cx](map[string]state.ActorBehavior{"child": failingSpawn(nil)}),
	).Start(ctx, "actor-effects", cx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	if got := h.Instance().Current(); got != "failed" {
		t.Fatalf("state = %q, want failed by the onError", got)
	}
	if got := len(rec.values); got != 1 {
		t.Fatalf("dispatched %d effects, want the onError transition's notify", got)
	}
}

// actorEffectsEnd is one seam that drives a parent Fire an actor brought about — a
// Fire, a Tick, a RunService settle, or a DeliverToActor settle — with the machine
// that makes it, the state it starts in, how to drive it, and the state it ends in.
// Each machine routes one transition that emits a notify, so the seam's step owes
// exactly one domain effect, whatever driver effects also pass through it.
type actorEffectsEnd struct {
	name    string
	machine func() *state.Machine[string, string, actorCtx]
	initial string
	drive   func(t *testing.T, h *durable.Handle[string, string, actorCtx], clk *state.FakeClock)
	final   string
}

// notifyAction builds the action every end's parent transition runs, emitting the
// notify the step must dispatch.
func notifyAction() func(state.ActionCtx[actorCtx]) (state.Effect, error) {
	return func(state.ActionCtx[actorCtx]) (state.Effect, error) {
		return notifyEffect{To: "ops@example.com"}, nil
	}
}

// TestActorDrivenFire_DispatchesItsDomainEffects pins that a domain effect a parent
// Fire an actor drove emits is dispatched however the actor was driven — a delivery's
// settle, a timer that spawned the actor, a service settle that spawned it — live by
// the step that drove it, and by Recover's replay of the journal that step wrote, so
// an effect that never landed applies exactly once across a crash (#257).
func TestActorDrivenFire_DispatchesItsDomainEffects(t *testing.T) {
	ctx := context.Background()
	ends := []actorEffectsEnd{
		{
			// Fire: idle -start-> spawning, whose child can't start, so the onError
			// spawnFailed drives the notify transition.
			name: "Fire",
			machine: func() *state.Machine[string, string, actorCtx] {
				return state.ForgeFor[actorCtx]("fireEffects").
					Action("notify", notifyAction()).
					Actor("child").
					State("idle").
					State("spawning").InvokeActor("child", state.WithInvokeOnError("spawnFailed")).
					State("failed").
					Initial("idle").
					Transition("idle").On("start").GoTo("spawning").
					Transition("spawning").On("spawnFailed").GoTo("failed").Do("notify").
					Quench()
			},
			initial: "idle",
			drive: func(t *testing.T, h *durable.Handle[string, string, actorCtx], _ *state.FakeClock) {
				if _, err := h.Fire(ctx, "start"); err != nil {
					t.Fatalf("Fire(start): %v", err)
				}
			},
			final: "failed",
		},
		{
			// DeliverToActor: working's finisher completes on "finish", so the settle's
			// childDone drives the notify transition.
			name: "DeliverToActor",
			machine: func() *state.Machine[string, string, actorCtx] {
				return state.ForgeFor[actorCtx]("deliverEffects").
					Action("notify", notifyAction()).
					Actor("finisher").
					State("working").InvokeActor("finisher", state.WithInvokeOnDone("childDone")).
					State("done").
					Initial("working").
					Transition("working").On("childDone").GoTo("done").Do("notify").
					Quench()
			},
			initial: "working",
			drive: func(t *testing.T, h *durable.Handle[string, string, actorCtx], _ *state.FakeClock) {
				found, err := h.DeliverToActor(ctx, state.ActorRef{ID: state.ActorID("deliverEffects", "working", 0)}, "finish")
				if !found || err != nil {
					t.Fatalf("DeliverToActor = %v, %v", found, err)
				}
			},
			final: "done",
		},
		{
			// Tick: waiting's 10s timer enters spawning, whose child can't start, so the
			// onError spawnFailed drives the notify transition within the tick's step.
			name: "Tick",
			machine: func() *state.Machine[string, string, actorCtx] {
				return state.ForgeFor[actorCtx]("tickEffects").
					Action("notify", notifyAction()).
					Actor("child").
					State("waiting").
					State("spawning").InvokeActor("child", state.WithInvokeOnError("spawnFailed")).
					State("failed").
					Initial("waiting").
					Transition("waiting").After(10 * time.Second).On("elapsed").GoTo("spawning").
					Transition("spawning").On("spawnFailed").GoTo("failed").Do("notify").
					Quench()
			},
			initial: "waiting",
			drive: func(t *testing.T, h *durable.Handle[string, string, actorCtx], clk *state.FakeClock) {
				clk.Advance(10 * time.Second)
				if _, err := h.Tick(ctx); err != nil {
					t.Fatalf("Tick: %v", err)
				}
			},
			final: "failed",
		},
		{
			// RunService: loading's fetch settles, and the loaded transition it drives
			// enters spawning, whose child can't start, so the onError spawnFailed drives
			// the notify transition within the settle's step.
			name: "RunService",
			machine: func() *state.Machine[string, string, actorCtx] {
				return state.ForgeFor[actorCtx]("serviceEffects").
					Action("notify", notifyAction()).
					Service("fetch", fetchOK).
					Actor("child").
					State("loading").Invoke("fetch", state.WithInvokeOnDone("loaded")).
					State("spawning").InvokeActor("child", state.WithInvokeOnError("spawnFailed")).
					State("failed").
					Initial("loading").
					Transition("loading").On("loaded").GoTo("spawning").
					Transition("spawning").On("spawnFailed").GoTo("failed").Do("notify").
					Quench()
			},
			initial: "loading",
			drive: func(t *testing.T, h *durable.Handle[string, string, actorCtx], _ *state.FakeClock) {
				if _, ran, err := h.RunService(ctx, state.InvokeID("serviceEffects", "loading", 0)); !ran || err != nil {
					t.Fatalf("RunService = %v, %v, want it to settle the fetch service", ran, err)
				}
			},
			final: "failed",
		},
	}
	for _, end := range ends {
		for _, where := range []string{"live", "Recover"} {
			t.Run(end.name+"/"+where, func(t *testing.T) {
				clk := state.NewFakeClock(epoch)
				opts := []greetOption{
					durable.WithRunnerClock[string, string, actorCtx](clk),
					durable.WithServiceRegistry[string, string, actorCtx](state.NewRegistry[actorCtx]().Service("fetch", fetchOK)),
					durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
						"child":    failingSpawn(nil),
						"finisher": finishingChild(),
					}),
				}
				// The live leg wires the handler up front, so the step that drives the
				// parent Fire dispatches the notify itself. The Recover leg leaves it out,
				// modeling a crash between the step's append and its dispatch: nothing is
				// marked, so Recover's replay must send the effect the journal names.
				rec := newEffectRecorder()
				if where == "live" {
					opts = append(opts, durable.WithEffectHandler[string, string, actorCtx](rec.handler()))
				}
				store, id := durable.NewMemStore(), durable.InstanceID("actorEffects-"+end.name)
				h, err := durable.NewRunner(end.machine(), store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState(end.initial))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				end.drive(t, h, clk)
				if got := h.Instance().Current(); got != end.final {
					t.Fatalf("state = %q, want %q", got, end.final)
				}
				if where == "Recover" {
					h, err = durable.Recover(ctx, end.machine(), store, id,
						append(opts, durable.WithEffectHandler[string, string, actorCtx](rec.handler()))...)
					if err != nil {
						t.Fatalf("Recover: %v", err)
					}
					if got := h.Instance().Current(); got != end.final {
						t.Fatalf("recovered state = %q, want %q", got, end.final)
					}
				}
				if got := len(rec.values); got != 1 {
					t.Fatalf("%s dispatched %d effects (%v), want the parent transition's notify once", where, got, rec.values)
				}
			})
		}
	}
}

// TestActorDrivenFire_EffectIDsPutTheStepsOwnFirst pins the id scheme's order rule: a
// step whose own transition emits an effect, and a parent Fire an actor drove that
// emits another, dispatches them under ids that name the step's own effect first, then
// the driven Fire's — and Recover, replaying the same step, stamps and dispatches the
// same ids in the same order, so an effect that never landed applies exactly once
// across the crash.
func TestActorDrivenFire_EffectIDsPutTheStepsOwnFirst(t *testing.T) {
	ctx := context.Background()
	// idle -start-> spawning runs "charge" (the step's own effect); the spawn fails, so
	// its onError drives spawning -spawnFailed-> failed, which runs "notify" (the driven
	// Fire's).
	machine := func() *state.Machine[string, string, actorCtx] {
		return state.ForgeFor[actorCtx]("ordered").
			Action("charge", func(state.ActionCtx[actorCtx]) (state.Effect, error) {
				return chargeEffect{Cents: 500}, nil
			}).
			Action("notify", notifyAction()).
			Actor("child").
			State("idle").
			State("spawning").InvokeActor("child", state.WithInvokeOnError("spawnFailed")).
			State("failed").Final().
			Initial("idle").
			Transition("idle").On("start").GoTo("spawning").Do("charge").
			Transition("spawning").On("spawnFailed").GoTo("failed").Do("notify").
			Quench()
	}
	want := []string{"0#0#demo.charge", "0#1#demo.notify"}
	for _, where := range []string{"live", "Recover"} {
		t.Run(where, func(t *testing.T) {
			// The live leg wires the handler up front; the Recover leg leaves it out,
			// modeling a crash between the step's append and its dispatch, so replay
			// must stamp the ids itself.
			rec := newEffectRecorder()
			opts := []greetOption{
				durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{"child": failingSpawn(nil)}),
			}
			if where == "live" {
				opts = append(opts, durable.WithEffectHandler[string, string, actorCtx](rec.handler()))
			}
			store, id := durable.NewMemStore(), durable.InstanceID("ordered-effects-"+where)
			h, err := durable.NewRunner(machine(), store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "start"); err != nil {
				t.Fatalf("Fire(start): %v", err)
			}
			if got := h.Instance().Current(); got != "failed" {
				t.Fatalf("state = %q, want failed by the onError", got)
			}
			if where == "Recover" {
				h, err = durable.Recover(ctx, machine(), store, id,
					append(opts, durable.WithEffectHandler[string, string, actorCtx](rec.handler()))...)
				if err != nil {
					t.Fatalf("Recover: %v", err)
				}
			}
			if got := rec.order; !slices.Equal(got, want) {
				t.Fatalf("%s dispatched %v, want %v: the step's own effect first, then the driven Fire's", where, got, want)
			}
			for _, id := range want {
				if n := rec.count(id); n != 1 {
					t.Fatalf("%s: effect %q applied %d times, want once", where, id, n)
				}
			}
		})
	}
}
