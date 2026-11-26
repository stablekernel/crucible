package durable_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// The domain effects of the parent Fires an actor spawned by Start or Recover drives.
// Those Fires sit pending on the Handle and journal as `before` entries of the next
// step, so they belong to no step that dispatches: today no live path dispatches their
// effects, and Recover's replay arms them without sending them. These tests pin the
// wanted behavior — the effect reaches the handler exactly once, live and on Recover,
// under the same id both sides — while the timing question is open (#269).

// spawnDrivenEnd is one flavor of spawn-driven parent Fire at Start: the machine, the
// actor palette it needs, the state it starts in, and the state the driven Fire
// enters.
type spawnDrivenEnd struct {
	name    string
	machine func() *state.Machine[string, string, actorCtx]
	palette greetOption
	initial string
	final   string
}

// spawnDrivenEnds are two of the three ways an actor Start spawns drives a parent
// Fire: a spawn that can't start routes its declared onError, and a spawn that succeeds
// sends its parent a message. (The third, the settle of an actor that finishes as the
// spawns run, is named in the design note's questions.) Each driven transition emits a
// notify, so the seam owes exactly one domain effect.
var spawnDrivenEnds = []spawnDrivenEnd{
	{
		// Start begins in spawning, whose child can't start, so the declared onError
		// drives spawning -spawnFailed-> failed, which emits a notify.
		name: "a failed spawn's onError",
		machine: func() *state.Machine[string, string, actorCtx] {
			return state.ForgeFor[actorCtx]("spawnNotify").
				Action("notify", notifyAction()).
				Actor("child").
				State("spawning").InvokeActor("child", state.WithInvokeOnError("spawnFailed")).
				State("failed").
				Initial("spawning").
				Transition("spawning").On("spawnFailed").GoTo("failed").Do("notify").
				Transition("failed").On("flush").GoTo("failed").
				Quench()
		},
		palette: durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"child": failingSpawn(nil),
		}),
		initial: "spawning",
		final:   "failed",
	},
	{
		// Start begins in greeting, whose greeter sends its parent "hello" as it spawns,
		// so the message drives greeting -hello-> greeted, which emits a notify.
		name: "a spawn's message",
		machine: func() *state.Machine[string, string, actorCtx] {
			return state.ForgeFor[actorCtx]("greetNotify").
				Action("notify", notifyAction()).
				Actor("greeter").
				State("greeting").InvokeActor("greeter").
				State("greeted").
				Initial("greeting").
				Transition("greeting").On("hello").GoTo("greeted").Do("notify").
				Transition("greeted").On("flush").GoTo("greeted").
				Quench()
		},
		palette: durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"greeter": func(map[string]any) (state.ActorInstance, error) { return &greeter{}, nil },
		}),
		initial: "greeting",
		final:   "greeted",
	},
}

// TestStartAndRecover_DispatchTheEffectsOfAFireTheirSpawnsDrive pins that a domain
// effect a parent Fire emits, where an actor that Start or Recover spawned drove that
// Fire, is dispatched exactly once — live, and by Recover's replay of the journal the
// next step wrote — under the same EffectID on both sides, so an effect that never
// landed applies exactly once across a crash (#269).
func TestStartAndRecover_DispatchTheEffectsOfAFireTheirSpawnsDrive(t *testing.T) {
	t.Skip("pending #269: when do a Start- or Recover-spawn's driven effects dispatch - with the next step, or at the spawning call's return")
	ctx := context.Background()
	for _, end := range spawnDrivenEnds {
		// legIDs collects each leg's EffectID, so the flavor's two legs can be shown to
		// agree.
		legIDs := map[string]string{}
		for _, where := range []string{"live", "Recover"} {
			t.Run(end.name+"/"+where, func(t *testing.T) {
				// The live leg wires the handler up front, so the live run itself must
				// dispatch the notify, whichever moment the ruling settles on. The Recover
				// leg leaves it out, modeling a crash before the dispatch: nothing is
				// marked, so Recover's replay must send the effect the journal names.
				rec := newEffectRecorder()
				opts := []greetOption{end.palette}
				if where == "live" {
					opts = append(opts, durable.WithEffectHandler[string, string, actorCtx](rec.handler()))
				}
				store, id := durable.NewMemStore(), durable.InstanceID("spawn-driven-"+end.name)
				h, err := durable.NewRunner(end.machine(), store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState(end.initial))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				// The follow-up step journals the pending Fire, and with it the effect
				// the journal must come to name.
				if _, err = h.Fire(ctx, "flush"); err != nil {
					t.Fatalf("Fire(flush): %v", err)
				}
				if got := h.Instance().Current(); got != end.final {
					t.Fatalf("state = %q, want %q by the driven Fire", got, end.final)
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
					t.Fatalf("%s dispatched %d effects (%v), want the driven transition's notify once", where, got, rec.values)
				}
				if got := rec.ids(); len(got) != 1 {
					t.Fatalf("%s dispatched under ids %v, want one", where, got)
				}
				legIDs[where] = rec.ids()[0]
			})
		}
		// The two legs of one flavor — the live dispatch and the Recover re-dispatch —
		// must stamp the same EffectID, whatever the timing choice settles the id to.
		if legIDs["live"] == "" || legIDs["live"] != legIDs["Recover"] {
			t.Errorf("live id %q, recovered id %q, want the same non-empty EffectID", legIDs["live"], legIDs["Recover"])
		}
	}
}
