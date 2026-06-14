package state

// This white-box test pins the actor-tree quiescence guard: SnapshotActors must
// refuse (with a typed *NonQuiescentActorError) to snapshot an actor whose mailbox
// holds a queued, in-flight message, because the v1.0 snapshot does not persist the
// reserved mailbox slot and would otherwise silently drop the message. A quiesced
// (drained) tree must still snapshot cleanly. It is an internal test so it can stage
// a non-empty mailbox directly — the synchronous host driver drains every mailbox at
// its quiescent points, so there is no public seam that leaves one queued.

import (
	"context"
	"errors"
	"testing"
)

// quiescChildCtx is a JSON-marshalable child context so the child snapshot
// round-trips.
type quiescChildCtx struct {
	Steps int `json:"steps"`
}

// buildQuiescChildMachine builds a flat child: idle --advance--> mid --finish--> done.
func buildQuiescChildMachine() *Machine[string, string, *quiescChildCtx] {
	return ForgeFor[*quiescChildCtx]("quiescchild").
		Action("step", func(c ActionCtx[*quiescChildCtx]) (Effect, error) {
			c.Entity.Steps++
			return nil, nil
		}).
		State("idle").
		State("mid").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("advance").GoTo("mid").Do("step").
		Transition("mid").On("finish").GoTo("done").Do("step").
		Quench()
}

// buildQuiescParentMachine builds a parent that invokes the child actor on entry to
// supervising.
func buildQuiescParentMachine() *Machine[string, string, map[string]any] {
	return ForgeFor[map[string]any]("quiescparent").
		State("idle").
		State("supervising").InvokeActor("quiescchild",
		WithInvokeOnDone("childDone"), WithInvokeOnError("childErr")).
		State("complete").
		Initial("idle").
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childDone").GoTo("complete").
		Quench()
}

// spawnQuiescSystem casts the parent, spawns its child actor, and returns the
// system plus the spawned child's id.
func spawnQuiescSystem(t *testing.T) (*ActorSystem[string, string, map[string]any], string) {
	t.Helper()
	ctx := context.Background()
	m := buildQuiescParentMachine()

	childBehavior := func() ActorBehavior {
		cm := buildQuiescChildMachine()
		return func(map[string]any) (ActorInstance, error) {
			inst := cm.Cast(&quiescChildCtx{}, WithInitialState("idle"))
			return NewActor(inst, nil), nil
		}
	}

	parent := m.Cast(map[string]any{}, WithInitialState("idle"))
	sys := NewActorSystem(parent).Register("quiescchild", childBehavior())
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)

	id := ActorID(m.Name(), "supervising", 0)
	if _, ok := sys.Ref(id); !ok {
		t.Fatalf("no actor ref for spawned child id %q", id)
	}
	return sys, id
}

// TestSnapshotActors_QuiescedTreeSnapshotsFine asserts a drained tree (every
// mailbox empty) snapshots cleanly: the guard does not fire on a quiesced tree.
func TestSnapshotActors_QuiescedTreeSnapshotsFine(t *testing.T) {
	sys, id := spawnQuiescSystem(t)

	// Mailbox is empty (the spawn drained nothing into it). Snapshot must succeed.
	snaps, err := sys.SnapshotActors()
	if err != nil {
		t.Fatalf("SnapshotActors on a quiesced tree: %v", err)
	}
	if _, ok := snaps[id]; !ok {
		t.Fatalf("quiesced snapshot missing child %q; got keys %v", id, keysOf(snaps))
	}
}

// TestSnapshotActors_NonQuiescentTreeReturnsTypedError stages a queued, in-flight
// message in the child's mailbox (the future async backlog the v1.0 snapshot does
// not persist) and asserts SnapshotActors refuses with a typed
// *NonQuiescentActorError naming the offending actor and the queue depth, rather
// than silently producing a lossy snapshot.
func TestSnapshotActors_NonQuiescentTreeReturnsTypedError(t *testing.T) {
	sys, id := spawnQuiescSystem(t)

	// Stage an undelivered message directly in the child's mailbox. The synchronous
	// driver never leaves one queued, so this models the future async crash-mid-
	// delivery backlog the guard exists to protect.
	sys.mu.Lock()
	ra := sys.actors[id]
	ra.mailbox = append(ra.mailbox, envelope{event: "finish", sender: parentActorID})
	sys.mu.Unlock()

	snaps, err := sys.SnapshotActors()
	if err == nil {
		t.Fatalf("SnapshotActors on a non-quiesced tree returned nil error (lossy snapshot); snaps=%v", keysOf(snaps))
	}
	var nqe *NonQuiescentActorError
	if !errors.As(err, &nqe) {
		t.Fatalf("error = %T (%v), want *NonQuiescentActorError", err, err)
	}
	if nqe.ActorID != id {
		t.Errorf("NonQuiescentActorError.ActorID = %q, want %q", nqe.ActorID, id)
	}
	if nqe.Queued != 1 {
		t.Errorf("NonQuiescentActorError.Queued = %d, want 1", nqe.Queued)
	}
	if snaps != nil {
		t.Errorf("SnapshotActors returned a snapshot alongside the error: %v", keysOf(snaps))
	}
}
