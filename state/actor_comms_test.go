package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// relayEntity is the entity a messaging child actor is bound to; it has no fields
// because the child's behavior is expressed entirely through its transitions.
type relayEntity struct{}

// relayChildMachine builds a child machine that exercises the actor-communication
// send actions. From "ready" it:
//   - "ping": sends "pong" to its parent (sendParent).
//   - "ask": responds "answer" to whoever sent the "ask" (respond-to-sender).
//   - "fwd": forwards the "fwd" event verbatim to the actor named by the "peer"
//     systemId (forwardTo).
//   - "finish": reaches the final "done" state.
//
// The transitions are internal self-edges (no GoTo) so the child keeps running and
// can handle further events; only "finish" moves it to a final state.
func relayChildMachine() *state.Machine[string, string, *relayEntity] {
	return state.ForgeFor[*relayEntity]("relay").
		State("ready").
		State("done").Final().
		Initial("ready").
		Transition("ready").On("ping").GoTo("ready").SendParent("pong").
		Transition("ready").On("ask").GoTo("ready").Respond("answer").
		Transition("ready").On("fwd").GoTo("ready").ForwardTo("", state.WithSendToSystemID("peer")).
		Transition("ready").On("finish").GoTo("done").
		Quench()
}

// relayBehavior returns an ActorBehavior that Casts a fresh relay child per spawn.
func relayBehavior() state.ActorBehavior {
	cm := relayChildMachine()
	return func(input map[string]any) (state.ActorInstance, error) {
		inst := cm.Cast(&relayEntity{}, state.WithInitialState("ready"))
		return state.NewActor(inst, nil), nil
	}
}

// commParentMachine builds a parent that spawns a relay child on entering
// "running" and drives messaging to/from it. Its transitions:
//   - "begin": idle -> running (entry spawns the child actor).
//   - "tell": running self-edge that sends "ping" to the child (sendTo by id).
//   - "pong": running self-edge recording that the child's reply arrived.
//   - "askChild": running self-edge that sends "ask" to the child (sendTo by id).
//   - "answer": running self-edge recording the child's respond reply.
//   - "answered": running self-edge a peer child fires (records a forwarded relay).
//
// The childID is the stable actor id of the invoke on "running"; the parent
// addresses the child by it via SendTo.
func commParentMachine(childID string) *state.Machine[string, string, *trec] {
	return state.ForgeFor[*trec]("parent").
		Action("note", func(c state.ActionCtx[*trec]) (state.Effect, error) {
			c.Entity.notes = append(c.Entity.notes, "note")
			return nil, nil
		}).
		Action("gotPong", func(c state.ActionCtx[*trec]) (state.Effect, error) {
			c.Entity.notes = append(c.Entity.notes, "pong")
			return nil, nil
		}).
		Action("gotAnswer", func(c state.ActionCtx[*trec]) (state.Effect, error) {
			c.Entity.notes = append(c.Entity.notes, "answer")
			return nil, nil
		}).
		State("idle").
		State("running").InvokeActor("relay", state.WithInvokeOnDone("relayDone"), state.WithInvokeOnError("relayErr")).
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("begin").GoTo("running").
		Transition("running").On("tell").GoTo("running").SendTo(childID, "ping").
		Transition("running").On("pong").GoTo("running").Do("gotPong").
		Transition("running").On("askChild").GoTo("running").SendTo(childID, "ask").
		Transition("running").On("answer").GoTo("running").Do("gotAnswer").
		Transition("running").On("relayed").GoTo("running").Do("note").
		Quench()
}

// startCommParent forges the parent, spawns the relay child, and returns the live
// parent instance and its ActorSystem ready in the "running" state.
func startCommParent(t *testing.T) (*state.Instance[string, string, *trec], *state.ActorSystem[string, string, *trec], *trec, string) {
	t.Helper()
	ctx := context.Background()
	m := commParentMachine("relay-child")
	childID := state.ActorID(m.Name(), "running", 0)
	m = commParentMachine(childID)

	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).Register("relay", relayBehavior())

	res := parent.Fire(ctx, "begin")
	sys.Absorb(ctx, res.Effects)
	if parent.Current() != "running" {
		t.Fatalf("parent state = %q, want running", parent.Current())
	}
	if sys.Running() != 1 {
		t.Fatalf("running actors = %d, want 1", sys.Running())
	}
	return parent, sys, entity, childID
}

// TestComm_SendToChildDelivers asserts a parent's sendTo routes an event into the
// addressed child actor's mailbox, and the child handling it (here, replying with
// sendParent) confirms delivery.
func TestComm_SendToChildDelivers(t *testing.T) {
	ctx := context.Background()
	parent, sys, entity, _ := startCommParent(t)

	// "tell" makes the parent sendTo("ping") to the child; the child handles "ping"
	// by sendParent("pong"), which the parent records.
	res := parent.Fire(ctx, "tell")
	sys.Absorb(ctx, res.Effects)

	if len(entity.notes) != 1 || entity.notes[0] != "pong" {
		t.Fatalf("notes = %v, want [pong] (sendTo did not reach the child)", entity.notes)
	}
}

// TestComm_ChildSendParentReachesParent asserts a child actor's sendParent is
// routed to the parent instance's Fire.
func TestComm_ChildSendParentReachesParent(t *testing.T) {
	ctx := context.Background()
	_, sys, entity, childID := startCommParent(t)

	// Deliver "ping" straight to the child; it runs sendParent("pong").
	ref, ok := sys.Ref(childID)
	if !ok {
		t.Fatalf("no ref for child %q", childID)
	}
	if !sys.Deliver(ctx, ref, "ping") {
		t.Fatal("Deliver to child returned false")
	}
	if len(entity.notes) != 1 || entity.notes[0] != "pong" {
		t.Fatalf("notes = %v, want [pong] (sendParent did not reach the parent)", entity.notes)
	}
}

// TestComm_RespondRepliesToSender asserts respond replies to the sender of the
// event the actor is currently handling: the parent sends "ask", the child
// responds "answer", and the reply lands back on the parent (the sender).
func TestComm_RespondRepliesToSender(t *testing.T) {
	ctx := context.Background()
	parent, sys, entity, _ := startCommParent(t)

	res := parent.Fire(ctx, "askChild")
	sys.Absorb(ctx, res.Effects)

	if len(entity.notes) != 1 || entity.notes[0] != "answer" {
		t.Fatalf("notes = %v, want [answer] (respond did not reach the sender)", entity.notes)
	}
}

// TestComm_RespondNoSenderIsNoop asserts respond is a no-op when the event the
// actor handles has no identifiable sender (delivered directly by the host).
func TestComm_RespondNoSenderIsNoop(t *testing.T) {
	ctx := context.Background()
	_, sys, entity, childID := startCommParent(t)

	// Deliver "ask" directly to the child (no actor sender). The child's respond has
	// no origin to reply to, so nothing reaches the parent.
	ref, _ := sys.Ref(childID)
	sys.Deliver(ctx, ref, "ask")

	if len(entity.notes) != 0 {
		t.Fatalf("notes = %v, want [] (respond with no sender should be a no-op)", entity.notes)
	}
}

// TestComm_ForwardDeliversTypedEvent asserts the forwarded event is actually the
// current event by observing a side effect: a forwarder forwards "ping" to a peer
// whose "ping" runs sendParent, reaching the parent.
func TestComm_ForwardDeliversTypedEvent(t *testing.T) {
	ctx := context.Background()
	// A forwarder child that, on "ping", forwards "ping" to the "peer" systemId.
	fwdMachine := state.ForgeFor[*relayEntity]("fwd").
		State("ready").
		Initial("ready").
		Transition("ready").On("ping").GoTo("ready").ForwardTo("", state.WithSendToSystemID("peer")).
		Quench()

	m := state.ForgeFor[*trec]("parent").
		Action("gotPong", func(c state.ActionCtx[*trec]) (state.Effect, error) {
			c.Entity.notes = append(c.Entity.notes, "pong")
			return nil, nil
		}).
		State("running").
		Initial("running").
		CurrentStateFn(func(*trec) string { return "running" }).
		Transition("running").On("pong").GoTo("running").Do("gotPong").
		Quench()

	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("running"))
	sys := state.NewActorSystem(parent).
		Register("fwd", func(map[string]any) (state.ActorInstance, error) {
			return state.NewActor(fwdMachine.Cast(&relayEntity{}, state.WithInitialState("ready")), nil), nil
		}).
		Register("peer", relayBehavior())

	sys.Absorb(ctx, []state.Effect{
		state.SpawnActor{ID: "fwd-a", Src: state.Ref{Name: "fwd"}},
		state.SpawnActor{ID: "peer-b", Src: state.Ref{Name: "peer"}, SystemID: "peer"},
	})

	refA, _ := sys.Ref("fwd-a")
	if !sys.Deliver(ctx, refA, "ping") {
		t.Fatal("Deliver to forwarder returned false")
	}
	// fwd-a forwarded "ping" to peer-b; peer-b ran sendParent("pong"); the parent
	// recorded it. This proves forwardTo delivered the verbatim current event.
	if len(entity.notes) != 1 || entity.notes[0] != "pong" {
		t.Fatalf("notes = %v, want [pong] (forwarded event did not reach the peer)", entity.notes)
	}
}

// TestComm_StopActorStopsActor asserts the StopActor action stops a spawned actor
// from a transition.
func TestComm_StopActorStopsActor(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("parent").
		State("running").
		Initial("running").
		CurrentStateFn(func(*trec) string { return "running" }).
		Transition("running").On("kill").GoTo("running").StopActor("victim").
		Quench()

	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState("running"))
	sys := state.NewActorSystem(parent).Register("relay", relayBehavior())
	sys.Absorb(ctx, []state.Effect{
		state.SpawnActor{ID: "victim", Src: state.Ref{Name: "relay"}},
	})
	if !sys.IsRunning("victim") {
		t.Fatal("victim should be running after spawn")
	}

	res := parent.Fire(ctx, "kill")
	sys.Absorb(ctx, res.Effects)
	if sys.IsRunning("victim") {
		t.Fatal("victim should be stopped after StopActor")
	}
	if sys.Running() != 0 {
		t.Fatalf("running = %d, want 0", sys.Running())
	}
}

// TestComm_IRRoundTrip asserts a machine using every send/stop action round-trips
// losslessly through JSON: the built-in action refs (and their structural target /
// event params) serialize and reload byte-for-byte identically, and the rehydrated
// machine binds against an empty registry (the comm built-ins need no binding).
func TestComm_IRRoundTrip(t *testing.T) {
	build := func() *state.Machine[string, string, *trec] {
		return state.ForgeFor[*trec]("comms").
			State("a").
			State("b").
			Initial("a").
			Transition("a").On("toB").GoTo("b").
			SendTo("child-1", "ping").
			SendParent("up").
			Respond("ack").
			ForwardTo("child-2").
			StopActor("child-1").
			Transition("b").On("sys").GoTo("a").
			SendTo("", "hello", state.WithSendToSystemID("named")).
			ForwardTo("", state.WithSendToSystemID("named")).
			Quench()
	}

	m := build()
	json1, err := m.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}

	ir, err := state.LoadFromJSON[string, string, *trec](json1)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	// The comm built-ins are kernel-handled and need no registry binding; an empty
	// registry resolves the machine's (only) refs.
	m2 := ir.Provide(state.NewRegistry[*trec]()).Quench()
	if m2 == nil {
		t.Fatal("Provide().Quench() returned nil")
	}

	json2, err := m2.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("re-ToJSON err = %v", err)
	}
	if string(json1) != string(json2) {
		t.Fatalf("IR not lossless:\n  first:  %s\n  second: %s", json1, json2)
	}

	// And the rehydrated machine actually emits the comm effects when fired.
	entity := &trec{}
	inst := m2.Cast(entity, state.WithInitialState("a"), state.WithFullTrace[string]())
	res := inst.Fire(context.Background(), "toB")
	if res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}
	var sawSendTo, sawSendParent, sawRespond, sawForward, sawStop bool
	for _, eff := range res.Effects {
		switch eff.(type) {
		case state.SendTo:
			sawSendTo = true
		case state.SendParent:
			sawSendParent = true
		case state.RespondToSender:
			sawRespond = true
		case state.ForwardEvent:
			sawForward = true
		case state.StopActor:
			sawStop = true
		}
	}
	if !sawSendTo || !sawSendParent || !sawRespond || !sawForward || !sawStop {
		t.Fatalf("missing comm effects: sendTo=%v sendParent=%v respond=%v forward=%v stop=%v",
			sawSendTo, sawSendParent, sawRespond, sawForward, sawStop)
	}

	// The trace records a microstep for each send/forward/stop the transition ran.
	want := []string{"actor.send.child-1", "actor.sendParent", "actor.respond", "actor.forward.child-2", "actor.stop.child-1"}
	for _, ms := range want {
		if !containsStr(res.Trace.Microsteps, ms) {
			t.Fatalf("trace microsteps %v missing %q", res.Trace.Microsteps, ms)
		}
	}
}

// containsStr reports whether xs contains s.
func containsStr(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// listenerMachine is a child that answers the event a forwardTo relays: on "drove"
// it tells its parent "heard", proving the forwarded event arrived typed.
func listenerMachine() *state.Machine[string, string, *relayEntity] {
	return state.ForgeFor[*relayEntity]("listener").
		State("ready").
		Initial("ready").
		Transition("ready").On("drove").GoTo("ready").SendParent("heard").
		Quench()
}

// forwardParentMachine builds a parent whose "running" transition on "drove" — the
// event its driver actor drives the parent with — forwards "drove" to the listener
// the compound "outer" state invokes, and records the listener's "heard" reply. The
// listener is invoked by "outer" and the driver by its child "running", so the
// listener is running before the driver spawns and can receive what the driver's
// parent Fire forwards.
func forwardParentMachine(driverOpts ...state.InvokeOption) *state.Machine[string, string, *trec] {
	return state.ForgeFor[*trec]("fwdparent").
		Action("heard", func(c state.ActionCtx[*trec]) (state.Effect, error) {
			c.Entity.notes = append(c.Entity.notes, "heard")
			return nil, nil
		}).
		Actor("driver").
		SuperState("outer").InvokeActor("listener", state.WithSystemID("listener")).
		SubState("running").InvokeActor("driver", driverOpts...).
		Initial("running").
		EndSuperState().
		Initial("outer").
		Transition("running").On("drove").GoTo("running").ForwardTo("", state.WithSendToSystemID("listener")).
		Transition("running").On("heard").GoTo("running").Do("heard").
		Quench()
}

// forwardHarness casts a forwardParentMachine, spawns its actors, and returns the
// parent, its system, the entity, and the driver's actor id.
func forwardHarness(t *testing.T, driver state.ActorBehavior, driverOpts ...state.InvokeOption) (
	*state.Instance[string, string, *trec], *state.ActorSystem[string, string, *trec], *trec, string,
) {
	t.Helper()
	ctx := context.Background()
	m := forwardParentMachine(driverOpts...)
	entity := &trec{}
	parent := m.Cast(entity, state.WithInitialState[string]("outer"))
	sys := state.NewActorSystem(parent).
		Register("listener", func(map[string]any) (state.ActorInstance, error) {
			return state.NewActor(listenerMachine().Cast(&relayEntity{}, state.WithInitialState[string]("ready")), nil), nil
		}).
		Register("driver", driver)
	sys.Absorb(ctx, parent.StartEffects())
	driverID := state.ActorID(m.Name(), "running", 0)
	if parent.Current() != "running" {
		t.Fatalf("parent state = %q, want running", parent.Current())
	}
	return parent, sys, entity, driverID
}

// TestComm_ForwardOnAParentFireAnActorDrives proves a forwardTo on a parent
// transition an actor drives forwards the event that drove the transition, not nil:
// a message the actor sent the parent, its own settle's onDone, and a failed spawn's
// declared onError alike (#260).
func TestComm_ForwardOnAParentFireAnActorDrives(t *testing.T) {
	ctx := context.Background()
	t.Run("a message the actor sent", func(t *testing.T) {
		// The driver tells its parent "drove" as it starts.
		driver := func(map[string]any) (state.ActorInstance, error) {
			return &startSender{ev: "drove"}, nil
		}
		_, sys, entity, _ := forwardHarness(t, driver)
		if got := sys.Running(); got != 2 {
			t.Fatalf("running actors = %d, want the listener and the driver", got)
		}
		if len(entity.notes) != 1 || entity.notes[0] != "heard" {
			t.Fatalf("notes = %v, want [heard]: the forwardTo handed the listener nil, not the actor's message", entity.notes)
		}
	})
	t.Run("the actor's settle", func(t *testing.T) {
		// The driver reaches its final state on "finish", settling through onDone.
		driverChild := state.ForgeFor[*relayEntity]("driverChild").
			State("ready").
			State("done").Final().
			Initial("ready").
			Transition("ready").On("finish").GoTo("done").
			Quench()
		driver := func(map[string]any) (state.ActorInstance, error) {
			return state.NewActor(driverChild.Cast(&relayEntity{}, state.WithInitialState[string]("ready")), nil), nil
		}
		_, sys, entity, driverID := forwardHarness(t, driver, state.WithInvokeOnDone("drove"))
		if !sys.DeliverByID(ctx, driverID, "finish") {
			t.Fatal("DeliverByID returned false; the driver was running")
		}
		if len(entity.notes) != 1 || entity.notes[0] != "heard" {
			t.Fatalf("notes = %v, want [heard]: the forwardTo handed the listener nil, not the settle's onDone", entity.notes)
		}
	})
	t.Run("a failed spawn's onError", func(t *testing.T) {
		driver := func(map[string]any) (state.ActorInstance, error) {
			return nil, errors.New("the driver cannot start")
		}
		_, _, entity, _ := forwardHarness(t, driver, state.WithInvokeOnError("drove"))
		if got := len(entity.notes); got != 1 || entity.notes[0] != "heard" {
			t.Fatalf("notes = %v, want [heard]: the forwardTo handed the listener nil, not the declared onError", entity.notes)
		}
	})
}

// startSender is an actor that tells its parent ev from its initial entry, then
// keeps running.
type startSender struct {
	ev   string
	sent bool
}

func (*startSender) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (s *startSender) ChildEffects() []state.Effect {
	if s.sent {
		return nil
	}
	s.sent = true
	return []state.Effect{state.SendParent{Event: s.ev}}
}
func (*startSender) Output() any { return nil }
