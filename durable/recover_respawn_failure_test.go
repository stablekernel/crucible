package durable_test

// The Recover tests for a running actor whose respawn fails: the journal already holds
// every parent Fire its start drove, so it stays running in the journal, and a later
// Recover spawns it again quietly rather than as new.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// flakyGreeterPalette binds a greeter that fails to start while *fail is set, and the
// finisher.
func flakyGreeterPalette(fail *bool, opts ...greetOption) []greetOption {
	palette := durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
		"greeter": func(map[string]any) (state.ActorInstance, error) {
			if *fail {
				return nil, errors.New("the greeter can't start yet")
			}
			return &greeter{}, nil
		},
		"spawnChild": finishingChild(),
	})
	return append([]greetOption{palette}, opts...)
}

// TestRecover_ARunningActorWhoseRespawnFailsStaysRunning proves a greeter that was
// running, whose respawn fails at one or more Recovers with no onError to catch it,
// stays running in the journal, with and without checkpoints: the next Recover spawns
// it again without sending its hello, so the hello applies once.
func TestRecover_ARunningActorWhoseRespawnFailsStaysRunning(t *testing.T) {
	ctx := context.Background()
	for _, every := range []int{0, 1} {
		for _, failures := range []int{1, 2} {
			t.Run(fmt.Sprintf("every=%d/failures=%d", every, failures), func(t *testing.T) {
				fail := false
				var extra []greetOption
				if every > 0 {
					extra = append(extra, durable.WithCheckpointEvery[string, string, actorCtx](every))
				}
				opts := flakyGreeterPalette(&fail, extra...)
				m := greetMachine(false)
				store, id := durable.NewMemStore(), durable.InstanceID("respawn-fails")
				h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("greeting"))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				if _, err = h.Fire(ctx, "poke"); err != nil {
					t.Fatalf("Fire(poke): %v", err)
				}
				wantHellos(t, "live", h)

				fail = true
				for round := 1; round <= failures; round++ {
					failed, err := durable.Recover(ctx, m, store, id, opts...)
					if err != nil {
						t.Fatalf("Recover %d with the greeter failing: %v", round, err)
					}
					wantHellos(t, fmt.Sprintf("Recover %d with the greeter failing", round), failed)
					if _, err = failed.Fire(ctx, "poke"); err != nil {
						t.Fatalf("Fire(poke) after Recover %d: %v", round, err)
					}
				}

				fail = false
				for round := 1; round <= 2; round++ {
					recovered, err := durable.Recover(ctx, m, store, id, opts...)
					if err != nil {
						t.Fatalf("Recover %d with the greeter starting: %v", round, err)
					}
					wantHellos(t, fmt.Sprintf("Recover %d with the greeter starting", round), recovered)
					if _, err = recovered.Fire(ctx, "poke"); err != nil {
						t.Fatalf("Fire(poke) after the greeter started again: %v", err)
					}
				}
			})
		}
	}
}

// anyGreetMachine is greetMachine(false)'s greeting state for a machine whose events
// are interfaces, so its invoke's onError is nil rather than the zero event, and a
// failed spawn escalates with no parent Fire.
func anyGreetMachine() *state.Machine[string, any, actorCtx] {
	return state.Forge[string, any, actorCtx]("anyGreet").
		Reducer("noteHello", func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			c.Notes = append(c.Notes, "hello")
			return c
		}).
		Actor("greeter").
		State("greeting").InvokeActor("greeter").
		Initial("greeting").
		Transition("greeting").On("hello").GoTo("greeting").Assign("noteHello").
		Transition("greeting").On("poke").GoTo("greeting").
		Quench()
}

// hellosAfterAFailedRespawn runs a greeter live in m's greeting, then Recovers on
// failCtx with the greeter's behavior returning fail's error, steps that Handle with
// poke when the Recover succeeds, and returns the hellos a last Recover, with the
// greeter starting, holds.
func hellosAfterAFailedRespawn[E comparable](t *testing.T, m *state.Machine[string, E, actorCtx], poke E, failCtx context.Context, fail func() error) int {
	t.Helper()
	ctx := context.Background()
	failing := false
	opts := []durable.Option[string, E, actorCtx]{durable.WithActorPalette[string, E, actorCtx](map[string]state.ActorBehavior{
		"greeter": func(map[string]any) (state.ActorInstance, error) {
			if failing {
				return nil, fail()
			}
			return &greeter{}, nil
		},
	})}
	store, id := durable.NewMemStore(), durable.InstanceID("respawn-failure")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("greeting"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, poke); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}

	failing = true
	if failed, recoverErr := durable.Recover(failCtx, m, store, id, opts...); recoverErr == nil {
		// The Handle outlives the Recover's context: its next step journals what it holds.
		if _, err = failed.Fire(ctx, poke); err != nil {
			t.Fatalf("Fire(poke) after the failed respawn: %v", err)
		}
	} else if !errors.Is(recoverErr, failCtx.Err()) || failCtx.Err() == nil {
		t.Fatalf("Recover with the greeter failing = %v, want success, or the error of its ended context", recoverErr)
	}

	failing = false
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover with the greeter starting: %v", err)
	}
	hellos := 0
	for _, n := range recovered.Instance().Entity().Notes {
		if n == "hello" {
			hellos++
		}
	}
	return hellos
}

// TestRecover_ARunningActorWhoseRespawnEscalatesStaysRunning proves a running greeter
// with no onError at all, whose failed respawn escalates and fires the parent nothing,
// stays running in the journal: the next Recover spawns it again without its hello.
func TestRecover_ARunningActorWhoseRespawnEscalatesStaysRunning(t *testing.T) {
	transient := func() error { return errors.New("the greeter can't start yet") }
	if got := hellosAfterAFailedRespawn(t, anyGreetMachine(), any("poke"), context.Background(), transient); got != 1 {
		t.Fatalf("hellos = %d, want the live spawn's one", got)
	}
}

// TestRecover_ARunningActorWhoseRespawnAContextEndsStaysRunning proves a running greeter
// whose respawn fails because Recover's context ended, as a behavior that heeds its
// context reports, stays running in the journal: with the zero-event onError the
// Recover fails, and with none it succeeds, and either way the next Recover spawns the
// greeter again without its hello.
func TestRecover_ARunningActorWhoseRespawnAContextEndsStaysRunning(t *testing.T) {
	ended := func() (context.Context, func() error) {
		turn, cancel := context.WithCancel(context.Background())
		return turn, func() error {
			cancel() // the deadline passes as the respawn runs
			return turn.Err()
		}
	}
	t.Run("zero-event onError", func(t *testing.T) {
		turn, fail := ended()
		if got := hellosAfterAFailedRespawn(t, greetMachine(false), "poke", turn, fail); got != 1 {
			t.Fatalf("hellos = %d, want the live spawn's one", got)
		}
	})
	t.Run("no onError", func(t *testing.T) {
		turn, fail := ended()
		if got := hellosAfterAFailedRespawn(t, anyGreetMachine(), any("poke"), turn, fail); got != 1 {
			t.Fatalf("hellos = %d, want the live spawn's one", got)
		}
	})
}
