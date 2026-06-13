package durable_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// actorCtx is a JSON-marshalable context for the actor record/replay proofs. Notes
// folds each settled actor's routed output / error and each parent-driving message
// so a recovered instance's context can be compared for byte-identity against the
// live run.
type actorCtx struct {
	Notes []string `json:"notes"`
}

// counterChild builds an actor behavior whose completion OUTPUT differs on each
// spawn-and-run: the output func reads a shared counter and returns a fresh, higher
// value every time it is invoked. It is the heart of the actor record/replay proof:
// if recovery re-ran the actor, the recovered parent context would fold a fresh
// (higher) output; byte-identity to the live run is only possible if the recorded
// output was replayed and the actor behavior was not re-invoked. The child machine
// is working -(finish)-> done(final), so the actor settles when delivered "finish".
func counterChild(runs *int64) state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		child := state.Forge[string, string, actorCtx]("child").
			State("working").
			State("done").Final().
			Initial("working").
			Transition("working").On("finish").GoTo("done").
			Quench()
		inst := child.Cast(actorCtx{}, state.WithInitialState[string]("working"))
		return state.NewActor(inst, func(*state.Instance[string, string, actorCtx]) any {
			n := atomic.AddInt64(runs, 1)
			return fmt.Sprintf("out-%d", n)
		}), nil
	}
}

// counterPalette binds counterChild under "spawnChild" so the durable Runner can
// resolve and run the same child behavior the parent machine declares.
func counterPalette(runs *int64) map[string]state.ActorBehavior {
	return map[string]state.ActorBehavior{"spawnChild": counterChild(runs)}
}

// supervisorMachine is a single-actor parent: supervising [InvokeActor spawnChild]
// -(childDone)-> complete, with an onError path to failed. The onDone transition
// folds the child output into context via an Assign reading AssignCtx.Event, so the
// routed output is observable in the recovered parent snapshot.
func supervisorMachine() *state.Machine[string, string, actorCtx] {
	return state.Forge[string, string, actorCtx]("supervisor").
		Reducer("recordChild", func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			if o, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "child:"+o)
			}
			return c
		}).
		Reducer("recordFail", func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			if e, ok := in.Event.(error); ok {
				c.Notes = append(c.Notes, "fail:"+e.Error())
			}
			return c
		}).
		Actor("spawnChild").
		State("supervising").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childFail")).
		State("complete").Final().
		State("failed").Final().
		Initial("supervising").
		Transition("supervising").On("childDone").GoTo("complete").Assign("recordChild").
		Transition("supervising").On("childFail").GoTo("failed").Assign("recordFail").
		Quench()
}

// liveSupervisorRun starts a supervisor instance, delivers "finish" to its spawned
// child (settling it through onDone), and returns the live parent snapshot bytes,
// the instance id, and the store for recovery.
func liveSupervisorRun(t *testing.T, runs *int64) ([]byte, durable.InstanceID, durable.Store) {
	t.Helper()
	ctx := context.Background()
	m := supervisorMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("sup-1")

	runner := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, actorCtx](counterPalette(runs)))
	h, err := runner.Start(ctx, id, actorCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("supervisor", "supervising", 0))
	if !ok {
		t.Fatalf("no actor ref for spawned child")
	}
	delivered, err := h.DeliverToActor(ctx, ref, "finish")
	if err != nil {
		t.Fatalf("DeliverToActor: %v", err)
	}
	if !delivered {
		t.Fatalf("DeliverToActor reported the actor was not running")
	}
	if got := h.Instance().Current(); got != "complete" {
		t.Fatalf("after child done, want complete, got %q", got)
	}
	snap, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	return snap, id, store
}

// TestActor_DoneReplay_ByteIdentical_NotReInvoked is the acceptance gate. An actor
// whose output differs each run is spawned and settled live (recording output O),
// the instance is recovered, and the recovery is asserted to be (a) byte-identical
// to the live parent snapshot AND (b) achieved WITHOUT re-running the actor
// behavior: the live run count is 1 and recovery does not increment it.
func TestActor_DoneReplay_ByteIdentical_NotReInvoked(t *testing.T) {
	ctx := context.Background()
	var runs int64
	liveSnap, id, store := liveSupervisorRun(t, &runs)

	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("live run invoked the actor %d times, want 1", got)
	}

	// Recover with a palette whose actor would yield a DIFFERENT output if re-run
	// (the same counter, already at 1). Byte-identity proves the recorded output was
	// replayed, not a fresh run.
	m := supervisorMachine()
	h, err := durable.Recover(ctx, m, store, id,
		durable.WithActorPalette[string, string, actorCtx](counterPalette(&runs)))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("recovery re-ran the actor: run count %d, want 1", got)
	}
	if got := h.Instance().Current(); got != "complete" {
		t.Fatalf("recovered state = %q, want complete", got)
	}
	recSnap, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot(recovered): %v", err)
	}
	if string(recSnap) != string(liveSnap) {
		t.Fatalf("recovered snapshot not byte-identical to live:\n live=%s\n  rec=%s", liveSnap, recSnap)
	}
}

// failChild builds an actor behavior that fails by panicking while handling
// "finish", so the ActorSystem settles it through onError. runs counts how many
// times the behavior was instantiated, proving recovery does not re-run it.
func failChild(runs *int64) state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		atomic.AddInt64(runs, 1)
		child := state.Forge[string, string, actorCtx]("child").
			Action("boom", func(state.ActionCtx[actorCtx]) (state.Effect, error) { panic("actor boom") }).
			State("working").
			State("done").Final().OnEntry("boom").
			Initial("working").
			Transition("working").On("finish").GoTo("done").
			Quench()
		inst := child.Cast(actorCtx{}, state.WithInitialState[string]("working"))
		return state.NewActor(inst, nil), nil
	}
}

// TestActor_ErrorReplay_ByteIdentical drives the onError path: a failing actor
// settles through onError, recording the error payload; recovery replays the
// recorded error (not a fresh run) and reaches a byte-identical parent snapshot.
func TestActor_ErrorReplay_ByteIdentical(t *testing.T) {
	ctx := context.Background()
	var runs int64
	m := supervisorMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("sup-err")

	palette := map[string]state.ActorBehavior{"spawnChild": failChild(&runs)}
	runner := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, actorCtx](palette))
	h, err := runner.Start(ctx, id, actorCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("supervisor", "supervising", 0))
	if !ok {
		t.Fatalf("no actor ref for spawned child")
	}
	if _, err = h.DeliverToActor(ctx, ref, "finish"); err != nil {
		t.Fatalf("DeliverToActor: %v", err)
	}
	if got := h.Instance().Current(); got != "failed" {
		t.Fatalf("after actor error, want failed, got %q", got)
	}
	liveSnap, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("live error run instantiated the actor %d times, want 1", got)
	}

	rh, err := durable.Recover(ctx, supervisorMachine(), store, id,
		durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"spawnChild": failChild(&runs),
		}))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("recovery re-ran the failing actor: run count %d, want 1", got)
	}
	recSnap, err := state.MarshalSnapshot(rh.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot(recovered): %v", err)
	}
	if string(recSnap) != string(liveSnap) {
		t.Fatalf("recovered error snapshot not byte-identical:\n live=%s\n  rec=%s", liveSnap, recSnap)
	}
}

// pingerChild builds an actor that, on "ping", sends a "pong" MESSAGE to its parent
// (SendParent) carrying a counter-derived value, then completes. The message drives
// a parent transition independent of the actor's own onDone, exercising the
// message-driven seam (an actor sending a message that advances the parent). msgs
// counts how many times the behavior was instantiated.
func pingerChild(msgs *int64) state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		atomic.AddInt64(msgs, 1)
		child := state.Forge[string, string, actorCtx]("pinger").
			State("ready").
			State("done").Final().
			Initial("ready").
			Transition("ready").On("ping").GoTo("done").SendParent("pong").
			Quench()
		inst := child.Cast(actorCtx{}, state.WithInitialState[string]("ready"))
		return state.NewActor(inst, nil), nil
	}
}

// messengerMachine is a parent driven by a child MESSAGE rather than the child's
// onDone: it spawns a pinger, and a "pong" message the pinger sends advances it from
// listening to heard, folding the message into context.
func messengerMachine() *state.Machine[string, string, actorCtx] {
	return state.Forge[string, string, actorCtx]("messenger").
		Reducer("recordPong", func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			if s, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "msg:"+s)
			}
			return c
		}).
		Actor("spawnPinger").
		State("listening").InvokeActor("spawnPinger", state.WithInvokeOnDone("pingerDone"), state.WithInvokeOnError("pingerFail")).
		State("heard").
		State("failed").Final().
		Initial("listening").
		Transition("listening").On("pong").GoTo("heard").Assign("recordPong").
		Transition("listening").On("pingerFail").GoTo("failed").
		Transition("heard").On("pingerDone").GoTo("heard").
		Quench()
}

// TestActor_MessageDrivesParent_Replay records an actor MESSAGE that drives a parent
// transition (not the actor's onDone) and asserts the recovered parent snapshot is
// byte-identical with the actor behavior not re-instantiated.
func TestActor_MessageDrivesParent_Replay(t *testing.T) {
	ctx := context.Background()
	var msgs int64
	m := messengerMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("msg-1")

	palette := map[string]state.ActorBehavior{"spawnPinger": pingerChild(&msgs)}
	runner := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, actorCtx](palette))
	h, err := runner.Start(ctx, id, actorCtx{}, state.WithInitialState("listening"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("messenger", "listening", 0))
	if !ok {
		t.Fatalf("no actor ref for spawned pinger")
	}
	if _, err = h.DeliverToActor(ctx, ref, "ping"); err != nil {
		t.Fatalf("DeliverToActor: %v", err)
	}
	if got := h.Instance().Current(); got != "heard" {
		t.Fatalf("after pong message, want heard, got %q", got)
	}
	liveSnap, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}
	if got := atomic.LoadInt64(&msgs); got != 1 {
		t.Fatalf("live message run instantiated the actor %d times, want 1", got)
	}

	rh, err := durable.Recover(ctx, messengerMachine(), store, id,
		durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"spawnPinger": pingerChild(&msgs),
		}))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := atomic.LoadInt64(&msgs); got != 1 {
		t.Fatalf("recovery re-instantiated the messaging actor: count %d, want 1", got)
	}
	recSnap, err := state.MarshalSnapshot(rh.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot(recovered): %v", err)
	}
	if string(recSnap) != string(liveSnap) {
		t.Fatalf("recovered message snapshot not byte-identical:\n live=%s\n  rec=%s", liveSnap, recSnap)
	}
}

// twoActorMachine spawns two child actors and settles on the SECOND one's done,
// proving multiple actors in one instance record and replay in order. Each child
// settles through its own onDone correlated by its own actor id.
func twoActorMachine() *state.Machine[string, string, actorCtx] {
	return state.Forge[string, string, actorCtx]("twin").
		Reducer("note", func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			if o, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, o)
			}
			return c
		}).
		Actor("childA").
		Actor("childB").
		State("first").InvokeActor("childA", state.WithInvokeOnDone("aDone"), state.WithInvokeOnError("aFail")).
		State("second").InvokeActor("childB", state.WithInvokeOnDone("bDone"), state.WithInvokeOnError("bFail")).
		State("done").Final().
		State("failed").Final().
		Initial("first").
		Transition("first").On("aDone").GoTo("second").Assign("note").
		Transition("second").On("bDone").GoTo("done").Assign("note").
		Transition("first").On("aFail").GoTo("failed").
		Transition("second").On("bFail").GoTo("failed").
		Quench()
}

// TestActor_MultipleActors_OneInstance_Replay records two sequential actor
// settlements in one instance and asserts the recovered snapshot is byte-identical
// with neither actor behavior re-run.
func TestActor_MultipleActors_OneInstance_Replay(t *testing.T) {
	ctx := context.Background()
	var aRuns, bRuns int64
	m := twoActorMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("twin-1")

	palette := map[string]state.ActorBehavior{
		"childA": counterChild(&aRuns),
		"childB": counterChild(&bRuns),
	}
	runner := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, actorCtx](palette))
	h, err := runner.Start(ctx, id, actorCtx{}, state.WithInitialState("first"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	refA, ok := h.ActorRef(state.ActorID("twin", "first", 0))
	if !ok {
		t.Fatalf("no ref for childA")
	}
	if _, err = h.DeliverToActor(ctx, refA, "finish"); err != nil {
		t.Fatalf("DeliverToActor(A): %v", err)
	}
	if got := h.Instance().Current(); got != "second" {
		t.Fatalf("after childA done, want second, got %q", got)
	}
	refB, ok := h.ActorRef(state.ActorID("twin", "second", 0))
	if !ok {
		t.Fatalf("no ref for childB")
	}
	if _, err = h.DeliverToActor(ctx, refB, "finish"); err != nil {
		t.Fatalf("DeliverToActor(B): %v", err)
	}
	if got := h.Instance().Current(); got != "done" {
		t.Fatalf("after both actors, want done, got %q", got)
	}
	liveSnap, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}

	rh, err := durable.Recover(ctx, twoActorMachine(), store, id,
		durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"childA": counterChild(&aRuns),
			"childB": counterChild(&bRuns),
		}))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if a, b := atomic.LoadInt64(&aRuns), atomic.LoadInt64(&bRuns); a != 1 || b != 1 {
		t.Fatalf("recovery re-ran actors: aRuns=%d bRuns=%d, want 1,1", a, b)
	}
	recSnap, err := state.MarshalSnapshot(rh.Instance().Snapshot())
	if err != nil {
		t.Fatalf("MarshalSnapshot(recovered): %v", err)
	}
	if string(recSnap) != string(liveSnap) {
		t.Fatalf("two-actor recovered snapshot not byte-identical:\n live=%s\n  rec=%s", liveSnap, recSnap)
	}
}

// TestActor_CrashAfterSettle_ResumeContinues models a crash AFTER one actor settled
// but before the next: the live run settles childA (persisted), then a fresh process
// recovers and drives childB to completion. The resumed instance reaches the final
// state with the first output replayed (not re-run) and the second freshly run once.
func TestActor_CrashAfterSettle_ResumeContinues(t *testing.T) {
	ctx := context.Background()
	var aRuns, bRuns int64
	m := twoActorMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("twin-crash")

	palette := map[string]state.ActorBehavior{
		"childA": counterChild(&aRuns),
		"childB": counterChild(&bRuns),
	}
	runner := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, actorCtx](palette))
	h, err := runner.Start(ctx, id, actorCtx{}, state.WithInitialState("first"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	refA, ok := h.ActorRef(state.ActorID("twin", "first", 0))
	if !ok {
		t.Fatalf("no ref for childA")
	}
	if _, err = h.DeliverToActor(ctx, refA, "finish"); err != nil {
		t.Fatalf("DeliverToActor(A): %v", err)
	}
	// Simulate a crash here: drop the live handle. childA has settled and persisted;
	// childB has not yet been driven.

	rh, err := durable.Recover(ctx, twoActorMachine(), store, id,
		durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"childA": counterChild(&aRuns),
			"childB": counterChild(&bRuns),
		}))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := rh.Instance().Current(); got != "second" {
		t.Fatalf("resumed at %q, want second (mid-flow)", got)
	}
	if a := atomic.LoadInt64(&aRuns); a != 1 {
		t.Fatalf("childA re-run on resume: aRuns=%d, want 1", a)
	}
	// Drive the second actor live on the resumed handle.
	refB, ok := rh.ActorRef(state.ActorID("twin", "second", 0))
	if !ok {
		t.Fatalf("no ref for childB after resume")
	}
	if _, err = rh.DeliverToActor(ctx, refB, "finish"); err != nil {
		t.Fatalf("DeliverToActor(B) after resume: %v", err)
	}
	if got := rh.Instance().Current(); got != "done" {
		t.Fatalf("after resumed childB, want done, got %q", got)
	}
	if b := atomic.LoadInt64(&bRuns); b != 1 {
		t.Fatalf("childB run %d times, want exactly 1", b)
	}

	// A second recovery now replays BOTH recorded actors, re-running neither.
	rh2, err := durable.Recover(ctx, twoActorMachine(), store, id,
		durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"childA": counterChild(&aRuns),
			"childB": counterChild(&bRuns),
		}))
	if err != nil {
		t.Fatalf("Recover (second): %v", err)
	}
	if a, b := atomic.LoadInt64(&aRuns), atomic.LoadInt64(&bRuns); a != 1 || b != 1 {
		t.Fatalf("second recovery re-ran actors: aRuns=%d bRuns=%d, want 1,1", a, b)
	}
	if got := rh2.Instance().Current(); got != "done" {
		t.Fatalf("second recovery state = %q, want done", got)
	}
}

// idleActorMachine is a minimal single-actor parent whose initial state "idle"
// invokes one child and waits: the child's onDone ("childDone") drives idle ->
// done. Nothing settles the actor at Start, so a crash-and-recover lands back in
// "idle" with the child still in flight — the case that depends on StartEffects
// re-spawning the restored configuration's actor so a post-recover DeliverToActor
// finds it.
func idleActorMachine() *state.Machine[string, string, actorCtx] {
	return state.Forge[string, string, actorCtx]("idler").
		Reducer("note", func(in state.AssignCtx[actorCtx]) actorCtx {
			c := in.Entity
			if o, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, o)
			}
			return c
		}).
		Actor("spawnChild").
		State("idle").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childFail")).
		State("done").Final().
		State("failed").Final().
		Initial("idle").
		Transition("idle").On("childDone").GoTo("done").Assign("note").
		Transition("idle").On("childFail").GoTo("failed").
		Quench()
}

// TestActor_RecoverWithInvoke_DeliverAfterRecover proves StartEffects re-spawns a
// restored configuration's actor on Recover. The machine starts in "idle" with an
// invoked child that is never settled before the crash; a fresh process recovers
// (which must re-spawn the still-running child) and a DeliverToActor to the
// recovered handle drives the child's onDone, advancing idle -> done. If Recover
// did not re-arm StartEffects, the child would not be running and the delivery
// would find no actor — exactly the regression this guards.
func TestActor_RecoverWithInvoke_DeliverAfterRecover(t *testing.T) {
	ctx := context.Background()
	var runs int64
	m := idleActorMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("idle-1")

	palette := map[string]state.ActorBehavior{"spawnChild": counterChild(&runs)}
	runner := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, actorCtx](palette))
	h, err := runner.Start(ctx, id, actorCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := h.Instance().Current(); got != "idle" {
		t.Fatalf("after Start, want idle, got %q", got)
	}
	// Simulate a crash here: drop the live handle. The child was spawned at Start
	// but never driven, so the persisted configuration is still "idle".

	rh, err := durable.Recover(ctx, idleActorMachine(), store, id,
		durable.WithActorPalette[string, string, actorCtx](map[string]state.ActorBehavior{
			"spawnChild": counterChild(&runs),
		}))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := rh.Instance().Current(); got != "idle" {
		t.Fatalf("resumed at %q, want idle", got)
	}

	// The recovered configuration's actor must be re-spawned: deliver to it and
	// assert it lands and drives the parent's onDone.
	ref, ok := rh.ActorRef(state.ActorID("idler", "idle", 0))
	if !ok {
		t.Fatalf("no ref for re-spawned child after resume")
	}
	delivered, err := rh.DeliverToActor(ctx, ref, "finish")
	if err != nil {
		t.Fatalf("DeliverToActor after resume: %v", err)
	}
	if !delivered {
		t.Fatalf("DeliverToActor reported the re-spawned actor was not running")
	}
	if got := rh.Instance().Current(); got != "done" {
		t.Fatalf("after delivering to re-spawned child, want done, got %q", got)
	}
}

// TestActor_DeliverToActor_NoPalette asserts DeliverToActor reports an error when no
// actor palette was wired, so a misconfigured host fails loudly rather than silently
// skipping the actor.
func TestActor_DeliverToActor_NoPalette(t *testing.T) {
	ctx := context.Background()
	m := supervisorMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("no-palette")
	runner := durable.NewRunner(m, store) // no WithActorPalette
	h, err := runner.Start(ctx, id, actorCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.DeliverToActor(ctx, state.ActorRef{ID: "x"}, "finish"); err == nil {
		t.Fatalf("DeliverToActor with no palette should error")
	}
}

// TestActor_DeliverToActor_NotRunning asserts DeliverToActor is a no-op (false, no
// error) for a ref that names no running actor.
func TestActor_DeliverToActor_NotRunning(t *testing.T) {
	ctx := context.Background()
	var runs int64
	m := supervisorMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("not-running")
	runner := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, actorCtx](counterPalette(&runs)))
	h, err := runner.Start(ctx, id, actorCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	delivered, err := h.DeliverToActor(ctx, state.ActorRef{ID: "no-such-actor"}, "finish")
	if err != nil {
		t.Fatalf("DeliverToActor for unknown ref errored: %v", err)
	}
	if delivered {
		t.Fatalf("DeliverToActor for unknown ref reported delivered")
	}
}

// TestActor_DeliverToActorByID drives the spawned child by raw id (not ref) and
// asserts it settles through onDone exactly as the ref-keyed path does.
func TestActor_DeliverToActorByID(t *testing.T) {
	ctx := context.Background()
	var runs int64
	m := supervisorMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("by-id")
	runner := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, actorCtx](counterPalette(&runs)))
	h, err := runner.Start(ctx, id, actorCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	delivered, err := h.DeliverToActorByID(ctx, state.ActorID("supervisor", "supervising", 0), "finish")
	if err != nil {
		t.Fatalf("DeliverToActorByID: %v", err)
	}
	if !delivered {
		t.Fatalf("DeliverToActorByID reported the actor was not running")
	}
	if got := h.Instance().Current(); got != "complete" {
		t.Fatalf("after child done via id, want complete, got %q", got)
	}
}

// TestActor_ActorRef_NoPalette asserts ActorRef reports false on a handle with no
// actor palette wired (it runs no actors), so a host probing for a child gets a
// clean negative rather than a panic.
func TestActor_ActorRef_NoPalette(t *testing.T) {
	ctx := context.Background()
	m := supervisorMachine()
	store := durable.NewMemStore()
	runner := durable.NewRunner(m, store) // no palette
	h, err := runner.Start(ctx, "no-pal", actorCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, ok := h.ActorRef("anything"); ok {
		t.Fatalf("ActorRef reported a running actor with no palette wired")
	}
}

// corruptingActorStore wraps a Store and corrupts the first recorded actor-message
// payload returned by Load, so recovery exercises the actor replay decode-error path.
type corruptingActorStore struct {
	durable.Store
	corrupted bool
}

func (s *corruptingActorStore) Load(ctx context.Context, id durable.InstanceID) ([]byte, []durable.Record, error) {
	snap, tail, err := s.Store.Load(ctx, id)
	if err != nil {
		return snap, tail, err
	}
	for i := range tail {
		for j := range tail[i].Entries {
			if tail[i].Entries[j].Kind == state.JournalActorMessage {
				tail[i].Entries[j].Payload = []byte("{not-json")
				s.corrupted = true
			}
		}
	}
	return snap, tail, nil
}

// TestActor_Replay_CorruptPayload asserts recovery surfaces a decode error when a
// recorded actor transition payload is corrupt, rather than silently diverging.
func TestActor_Replay_CorruptPayload(t *testing.T) {
	ctx := context.Background()
	var runs int64
	_, id, base := liveSupervisorRun(t, &runs)
	store := &corruptingActorStore{Store: base}

	if _, err := durable.Recover(ctx, supervisorMachine(), store, id,
		durable.WithActorPalette[string, string, actorCtx](counterPalette(&runs))); err == nil {
		t.Fatal("Recover with corrupt actor payload should error")
	}
	if !store.corrupted {
		t.Fatal("no actor-message entry was corrupted; test did not exercise the path")
	}
}

// TestActor_Determinism asserts two independent live runs of the same supervisor
// produce byte-identical snapshots, confirming the seam introduces no ordering
// nondeterminism of its own.
func TestActor_Determinism(t *testing.T) {
	var r1, r2 int64
	s1, _, _ := liveSupervisorRun(t, &r1)
	s2, _, _ := liveSupervisorRun(t, &r2)
	if string(s1) != string(s2) {
		t.Fatalf("two live runs diverged:\n run1=%s\n run2=%s", s1, s2)
	}
}
