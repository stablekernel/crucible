package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestActorSystem_WithParentFireObserver_SeesEachParentFire proves the observer gets
// every parent Fire the system drives, in fire order, a refused one included with its
// error, and marks the Fire that settled the actor: here a child's message to the
// parent, then its onDone, which a guard refuses.
func TestActorSystem_WithParentFireObserver_SeesEachParentFire(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("watcher").
		Guard("never", func(state.GuardCtx[*trec]) bool { return false }).
		Actor("relay").
		State("watching").InvokeActor("relay", state.WithInvokeOnDone("relayDone")).
		State("finished").Final().
		Initial("watching").
		Transition("watching").On("pong").GoTo("watching").
		Transition("watching").On("relayDone").GoTo("finished").When("never").
		Quench()
	parent := m.Cast(&trec{}, state.WithInitialState("watching"))
	var seen []state.ParentFire[string]
	sys := state.NewActorSystem(parent).
		Register("relay", relayBehavior()).
		WithParentFireObserver(func(pf state.ParentFire[string]) { seen = append(seen, pf) })
	sys.Absorb(ctx, parent.StartEffects())
	ref, ok := sys.Ref(state.ActorID("watcher", "watching", 0))
	if !ok {
		t.Fatal("the relay child didn't spawn")
	}

	sys.Deliver(ctx, ref, "ping") // the child sends the parent "pong"
	sys.Deliver(ctx, ref, "finish")

	if len(seen) != 2 {
		t.Fatalf("observed %d parent Fires, want 2 (the message, then the onDone)", len(seen))
	}
	if r := seen[0].Result; r.Err != nil || r.Trace.Event != "pong" || seen[0].Settle {
		t.Fatalf("first = %q, err %v, settle %v, want the applied pong message", r.Trace.Event, r.Err, seen[0].Settle)
	}
	if seen[1].Result.Err == nil || !seen[1].Settle {
		t.Fatalf("second = err %v, settle %v, want the guard-refused onDone, settling", seen[1].Result.Err, seen[1].Settle)
	}
	if got := parent.Current(); got != "watching" {
		t.Fatalf("parent = %q, want watching after the refused onDone", got)
	}
}
