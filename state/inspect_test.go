package state_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// recordingInspector collects every inspection event it receives, so a test can
// assert on the live stream an instance (and its ActorSystem) produced.
type recordingInspector struct {
	events []state.InspectionEvent
}

func (r *recordingInspector) Inspect(ev state.InspectionEvent) {
	r.events = append(r.events, ev)
}

// ofKind returns the recorded events of a single kind, preserving order.
func (r *recordingInspector) ofKind(k state.InspectKind) []state.InspectionEvent {
	var out []state.InspectionEvent
	for _, ev := range r.events {
		if ev.Kind == k {
			out = append(out, ev)
		}
	}
	return out
}

// TestInspector_ObservesTransitions asserts a registered inspector receives the
// event/transition/snapshot stream for each Fire, with the transition carrying the
// from/to leaves and the live Trace.
func TestInspector_ObservesTransitions(t *testing.T) {
	insp := &recordingInspector{}
	m := buildDocMachine()
	doc := &Document{Status: Draft, ReviewerID: strptr("rev-1")}
	inst := m.Cast(doc, state.WithInspector[DocState](insp))

	inst.Fire(context.Background(), Submit)

	events := insp.ofKind(state.InspectEvent)
	if len(events) != 1 {
		t.Fatalf("event observations = %d, want 1", len(events))
	}
	if events[0].Event != "Submit" {
		t.Fatalf("event = %q, want Submit", events[0].Event)
	}

	trans := insp.ofKind(state.InspectTransition)
	if len(trans) != 1 {
		t.Fatalf("transition observations = %d, want 1", len(trans))
	}
	if trans[0].From != "Draft" || trans[0].To != "Submitted" {
		t.Fatalf("transition from/to = %q/%q, want Draft/Submitted", trans[0].From, trans[0].To)
	}
	if trans[0].Trace == nil {
		t.Fatal("transition event carried no Trace")
	}
	if trans[0].Trace.Event != "Submit" {
		t.Fatalf("trace event = %q, want Submit", trans[0].Trace.Event)
	}

	snaps := insp.ofKind(state.InspectSnapshot)
	if len(snaps) != 1 {
		t.Fatalf("snapshot observations = %d, want 1", len(snaps))
	}
	if snaps[0].To != "Submitted" {
		t.Fatalf("snapshot to = %q, want Submitted", snaps[0].To)
	}
	if len(snaps[0].Configuration) != 1 || snaps[0].Configuration[0] != "Submitted" {
		t.Fatalf("snapshot config = %v, want [Submitted]", snaps[0].Configuration)
	}
}

// TestInspector_DefaultNoOp asserts an instance cast without WithInspector never
// calls an inspector — inspection is off by default — and that Fire behaves
// identically with and without one (the result is unchanged).
func TestInspector_DefaultNoOp(t *testing.T) {
	m := buildDocMachine()

	plain := m.Cast(&Document{Status: Draft}, state.WithInitialState(Draft))
	resPlain := plain.Fire(context.Background(), Submit)

	insp := &recordingInspector{}
	observed := m.Cast(&Document{Status: Draft}, state.WithInitialState(Draft), state.WithInspector[DocState](insp))
	resObserved := observed.Fire(context.Background(), Submit)

	if resPlain.NewState != resObserved.NewState {
		t.Fatalf("inspected Fire changed result: %v vs %v", resPlain.NewState, resObserved.NewState)
	}
	if len(insp.events) == 0 {
		t.Fatal("inspector recorded nothing for an observed Fire")
	}
}

// TestInspectorFunc_Adapter asserts the InspectorFunc closure adapter receives the
// same stream a struct inspector would.
func TestInspectorFunc_Adapter(t *testing.T) {
	var transitions int
	insp := state.InspectorFunc(func(ev state.InspectionEvent) {
		if ev.Kind == state.InspectTransition {
			transitions++
		}
	})
	m := buildDocMachine()
	inst := m.Cast(&Document{Status: Draft}, state.WithInitialState(Draft), state.WithInspector[DocState](insp))

	inst.Fire(context.Background(), Submit)

	if transitions != 1 {
		t.Fatalf("InspectorFunc transition count = %d, want 1", transitions)
	}
}

// TestInspector_ObservesActorLifecycleAndMessages wires the same inspector to a
// parent instance and its ActorSystem, then drives a spawn + a parent->child send,
// asserting the actor-spawned and message sent/delivered observations surface.
func TestInspector_ObservesActorLifecycleAndMessages(t *testing.T) {
	ctx := context.Background()
	m := commParentMachine("relay-child")
	childID := state.ActorID(m.Name(), "running", 0)
	m = commParentMachine(childID)

	insp := &recordingInspector{}
	parent := m.Cast(&trec{}, state.WithInitialState("idle"), state.WithInspector[string](insp))
	sys := state.NewActorSystem(parent).
		Register("relay", relayBehavior()).
		WithActorInspector(insp)

	// Spawn the child by entering "running".
	res := parent.Fire(ctx, "begin")
	sys.Absorb(ctx, res.Effects)

	spawned := insp.ofKind(state.InspectActor)
	if len(spawned) == 0 {
		t.Fatal("no actor lifecycle observation after spawn")
	}
	var sawSpawn bool
	for _, ev := range spawned {
		if ev.ActorPhase == state.ActorSpawned && ev.ActorID == childID {
			sawSpawn = true
		}
	}
	if !sawSpawn {
		t.Fatalf("no ActorSpawned for %q in %+v", childID, spawned)
	}

	// Parent sends "ping" to the child; the child replies via sendParent, so we see
	// messages flow in both directions.
	res = parent.Fire(ctx, "tell")
	sys.Absorb(ctx, res.Effects)

	msgs := insp.ofKind(state.InspectMessage)
	if len(msgs) == 0 {
		t.Fatal("no message observations after a parent->child send")
	}
	var sawSent, sawDelivered bool
	for _, ev := range msgs {
		if ev.MessagePhase == state.MessageSent {
			sawSent = true
		}
		if ev.MessagePhase == state.MessageDelivered {
			sawDelivered = true
		}
	}
	if !sawSent || !sawDelivered {
		t.Fatalf("message phases: sent=%v delivered=%v, want both", sawSent, sawDelivered)
	}
}
