package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// The rule for a spawn that failed to start: Recover retries it only when nothing was
// declared to catch the failure. A spawn whose declared onError the parent refused,
// for any reason but a context ending, is done, since the journal holds the refusal.
// A context-refused onError retries (TestRecover_RetriesAGuardedSpawnOnlyWhenAContext-
// RefusedItsOnError).

// denyAs is a middleware that refuses event with err, as a policy layer might.
func denyAs(event string, err error) state.Middleware[string, string, stepCtx] {
	return func(next state.FireFunc[string, string, stepCtx]) state.FireFunc[string, string, stepCtx] {
		return func(ctx context.Context, ev string) state.FireResult[string] {
			if ev == event {
				return state.FireResult[string]{Err: err}
			}
			return next(ctx, ev)
		}
	}
}

// spawnRuleMachine is idle -start-> spawning, which invokes child with opts. spawning
// handles spawnFailed only when handles, and poke always.
func spawnRuleMachine(handles bool, mw state.Middleware[string, string, stepCtx], opts ...state.InvokeOption) stepMachine {
	b := state.ForgeFor[stepCtx]("spawnRule").Actor("child")
	if mw != nil {
		b = b.Use(mw)
	}
	b = b.State("idle").
		State("spawning").InvokeActor("child", opts...).
		State("failed").Final().
		Initial("idle").
		Transition("idle").On("start").GoTo("spawning").
		Transition("spawning").On("poke").GoTo("spawning")
	if handles {
		b = b.Transition("spawning").On("spawnFailed").GoTo("failed")
	}
	return b.Quench()
}

// spawnRuleCalls runs start live with a child behavior that fails its first spawn and
// starts after, then Recovers once, and returns how many times the behavior ran. A
// nonzero every checkpoints after every that many steps, so the rule holds from a
// checkpoint's recorded sets as well as the tail.
func spawnRuleCalls(t *testing.T, m stepMachine, every ...int) int {
	t.Helper()
	ctx := context.Background()
	store, id := durable.NewMemStore(), durable.InstanceID("spawn-rule")
	var calls int
	opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
		"child": func(in map[string]any) (state.ActorInstance, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("the child can't start yet")
			}
			return finishingChild()(in)
		},
	})}
	for _, n := range every {
		if n > 0 {
			opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](n))
		}
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	if calls != 1 {
		t.Fatalf("live ran the behavior %d times, want once", calls)
	}
	if _, err = durable.Recover(ctx, m, store, id, opts...); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	return calls
}

func TestSpawnRule_NoOnErrorRetries(t *testing.T) {
	if got := spawnRuleCalls(t, spawnRuleMachine(false, nil)); got != 2 {
		t.Fatalf("behavior ran %d times, want Recover to retry it once", got)
	}
}

func TestSpawnRule_GuardRefusedOnErrorIsDone(t *testing.T) {
	m := state.ForgeFor[stepCtx]("spawnRuleGuard").
		Guard("never", func(state.GuardCtx[stepCtx]) bool { return false }).
		Actor("child").
		State("idle").
		State("spawning").InvokeActor("child", state.WithInvokeOnError("spawnFailed")).
		State("failed").Final().
		Initial("idle").
		Transition("idle").On("start").GoTo("spawning").
		Transition("spawning").On("spawnFailed").GoTo("failed").When("never").
		Transition("spawning").On("poke").GoTo("spawning").
		Quench()
	if got := spawnRuleCalls(t, m); got != 1 {
		t.Fatalf("behavior ran %d times, want Recover to leave it alone", got)
	}
}

func TestSpawnRule_DeclaredOnErrorWithNoTransitionIsDone(t *testing.T) {
	for _, every := range []int{0, 1} {
		if got := spawnRuleCalls(t, spawnRuleMachine(false, nil, state.WithInvokeOnError("spawnFailed")), every); got != 1 {
			t.Fatalf("every %d: behavior ran %d times, want Recover to leave it alone", every, got)
		}
	}
}

func TestSpawnRule_DenialWordedLikeNoTransitionIsDone(t *testing.T) {
	denial := &state.InvalidTransitionError{Event: "spawnFailed", Reason: "no transition declared for this state and event"}
	m := spawnRuleMachine(true, denyAs("spawnFailed", denial), state.WithInvokeOnError("spawnFailed"))
	if got := spawnRuleCalls(t, m); got != 1 {
		t.Fatalf("behavior ran %d times, want Recover to leave it alone", got)
	}
}

// TestSpawnRule_ActorThatFailsWithNoOnErrorStaysEnded pins that an invoked actor that
// ran and then failed with no onError declared is recorded as ended, so no Recover
// spawns it again.
func TestSpawnRule_ActorThatFailsWithNoOnErrorStaysEnded(t *testing.T) {
	ctx := context.Background()
	for _, every := range []int{0, 1} {
		m := state.ForgeFor[stepCtx]("failsLater").
			Actor("child").
			State("idle").
			State("working").InvokeActor("child", state.WithInvokeID("kid")).
			Initial("idle").
			Transition("idle").On("start").GoTo("working").
			Transition("working").On("poke").GoTo("working").
			Quench()
		store, id := durable.NewMemStore(), durable.InstanceID("fails-later")
		var runs int64
		opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"child": failChild(&runs)})}
		if every > 0 {
			opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
		}
		h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "start"); err != nil {
			t.Fatalf("Fire(start): %v", err)
		}
		if ok, err := h.DeliverToActorByID(ctx, "kid", "finish"); !ok || err != nil {
			t.Fatalf("DeliverToActorByID = %v, %v, want the actor found", ok, err)
		}
		for round := 1; round <= 2; round++ {
			if _, err := durable.Recover(ctx, m, store, id, opts...); err != nil {
				t.Fatalf("every %d, Recover %d: %v", every, round, err)
			}
			// A respawn would run the behavior again.
			if runs != 1 {
				t.Fatalf("every %d, Recover %d: behavior ran %d times, want the live run only", every, round, runs)
			}
		}
	}
}

// spawnActionMachine is idle -start-> spawning, whose start transition spawns child
// with opts; spawning handles spawnFailed only when handles.
func spawnActionMachine(handles bool, opts ...state.SpawnOption) stepMachine {
	b := state.ForgeFor[stepCtx]("spawnAction").
		Actor("child").
		State("idle").
		State("spawning").
		State("failed").Final().
		Initial("idle").
		Transition("idle").On("start").GoTo("spawning").Spawn("child", "kid", opts...).
		Transition("spawning").On("poke").GoTo("spawning")
	if handles {
		b = b.Transition("spawning").On("spawnFailed").GoTo("failed")
	}
	return b.Quench()
}

// An actor a transition spawns, rather than one a state invokes, doesn't return on
// Recover, so its failed spawn isn't retried either, declared onError or not.
func TestSpawnRule_SpawnActionNoOnErrorIsNotRetried(t *testing.T) {
	if got := spawnRuleCalls(t, spawnActionMachine(false)); got != 1 {
		t.Fatalf("behavior ran %d times, want Recover to leave a spawned actor alone", got)
	}
}

func TestSpawnRule_SpawnActionDeclaredOnErrorWithNoTransitionIsDone(t *testing.T) {
	if got := spawnRuleCalls(t, spawnActionMachine(false, state.WithSpawnOnError("spawnFailed"))); got != 1 {
		t.Fatalf("behavior ran %d times, want Recover to leave it alone", got)
	}
}
