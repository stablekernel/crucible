package state_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// startGreeter is an actor that sends its parent "hello" from its initial entry, then
// keeps running.
type startGreeter struct{ sent bool }

func (g *startGreeter) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (g *startGreeter) ChildEffects() []state.Effect {
	if g.sent {
		return nil
	}
	g.sent = true
	return []state.Effect{state.SendParent{Event: "hello"}}
}
func (g *startGreeter) Output() any { return nil }

// TestActorSystem_AbsorbRespawn_DropsStartMessages proves an actor AbsorbRespawn spawns
// runs, but the message it sends the parent as it starts doesn't reach the parent, as it
// does under Absorb, while a spawn that can't start still routes its onError.
func TestActorSystem_AbsorbRespawn_DropsStartMessages(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("respawn").
		Actor("greeter").
		Actor("broken").
		State("greeting").InvokeActor("greeter").InvokeActor("broken", state.WithInvokeOnError("brokenFailed")).
		State("hellod").
		State("failed").
		Initial("greeting").
		Transition("greeting").On("hello").GoTo("hellod").
		Transition("greeting").On("brokenFailed").GoTo("failed").
		Quench()
	behaviors := func(sys *state.ActorSystem[string, string, *trec], broken bool) {
		sys.Register("greeter", func(map[string]any) (state.ActorInstance, error) { return &startGreeter{}, nil })
		sys.Register("broken", func(map[string]any) (state.ActorInstance, error) {
			if broken {
				return nil, errors.New("can't start")
			}
			return &startGreeter{sent: true}, nil
		})
	}

	live := m.Cast(&trec{}, state.WithInitialState("greeting"))
	liveSys := state.NewActorSystem(live)
	behaviors(liveSys, false)
	liveSys.Absorb(ctx, live.StartEffects())
	if got := live.Current(); got != "hellod" {
		t.Fatalf("Absorb left the parent in %q, want hellod from the greeter's start", got)
	}

	resumed := m.Cast(&trec{}, state.WithInitialState("greeting"))
	var seen []state.ParentFire[string]
	sys := state.NewActorSystem(resumed).
		WithParentFireObserver(func(pf state.ParentFire[string]) { seen = append(seen, pf) })
	behaviors(sys, false)
	sys.AbsorbRespawn(ctx, resumed.StartEffects())
	if got := resumed.Current(); got != "greeting" {
		t.Fatalf("AbsorbRespawn left the parent in %q, want greeting: the start message should be dropped", got)
	}
	if len(seen) != 0 {
		t.Fatalf("observed %d parent Fires, want none", len(seen))
	}
	if !sys.IsRunning(state.ActorID("respawn", "greeting", 0)) {
		t.Fatal("AbsorbRespawn didn't spawn the greeter")
	}

	failing := m.Cast(&trec{}, state.WithInitialState("greeting"))
	failSys := state.NewActorSystem(failing)
	behaviors(failSys, true)
	failSys.AbsorbRespawn(ctx, []state.Effect{failing.StartEffects()[1]})
	if got := failing.Current(); got != "failed" {
		t.Fatalf("AbsorbRespawn of a spawn that can't start left the parent in %q, want failed by its onError", got)
	}
}

// starter is an actor that sends its first effects from its initial entry, then keeps
// running; finish ends it with no output.
type starter struct{ start []state.Effect }

func (a *starter) DeliverFire(_ context.Context, ev any) (bool, any) { return ev == "finish", nil }
func (a *starter) ChildEffects() []state.Effect {
	out := a.start
	a.start = nil
	return out
}
func (a *starter) Output() any { return nil }

// TestActorSystem_AbsorbRespawn_ParentFiresItDrivesAreAudible proves the quiet of an
// AbsorbRespawn spawn covers only the spawned actor's own start: the onError of a spawn
// that can't start, and the onDone of an actor a respawned one ends as it starts, are
// parent Fires the earlier start didn't send, so the greeter their state spawns reaches
// the parent with its hello, as under Absorb.
func TestActorSystem_AbsorbRespawn_ParentFiresItDrivesAreAudible(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("audible").
		Actor("greeter").
		Actor("broken").
		Actor("finisher").
		Actor("poker").
		State("failing").InvokeActor("broken", state.WithInvokeOnError("brokenFailed")).
		State("pair").InvokeActor("finisher", state.WithInvokeOnDone("finished")).InvokeActor("poker").
		State("greeting").InvokeActor("greeter").
		State("hellod").
		Initial("failing").
		Transition("failing").On("brokenFailed").GoTo("greeting").
		Transition("pair").On("finished").GoTo("greeting").
		Transition("greeting").On("hello").GoTo("hellod").
		Quench()
	finisher := state.ActorID("audible", "pair", 0)
	for _, from := range []string{"failing", "pair"} {
		t.Run(from, func(t *testing.T) {
			inst := m.Cast(&trec{}, state.WithInitialState(from))
			sys := state.NewActorSystem(inst)
			sys.Register("greeter", func(map[string]any) (state.ActorInstance, error) { return &startGreeter{}, nil })
			sys.Register("broken", func(map[string]any) (state.ActorInstance, error) { return nil, errors.New("can't start") })
			sys.Register("finisher", func(map[string]any) (state.ActorInstance, error) { return &starter{}, nil })
			sys.Register("poker", func(map[string]any) (state.ActorInstance, error) {
				return &starter{start: []state.Effect{state.SendTo{TargetID: finisher, Event: "finish"}}}, nil
			})
			sys.AbsorbRespawn(ctx, inst.StartEffects())
			if got := inst.Current(); got != "hellod" {
				t.Fatalf("AbsorbRespawn from %s left the parent in %q, want hellod: the greeter its onError or onDone spawned should reach the parent", from, got)
			}
		})
	}
}

// TestActorSystem_AbsorbRespawn_NestedStartFiresStayQuiet proves the quiet of an
// AbsorbRespawn spawn covers a nested actor the respawned one spawns as it starts: the
// onError of a nested spawn that can't start, and the onDone of a nested actor the
// respawned one ends as it starts, are parent Fires its earlier start drove, so neither
// fires the parent again, though each does under Absorb.
func TestActorSystem_AbsorbRespawn_NestedStartFiresStayQuiet(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("nested").
		Actor("host").
		State("hosting").InvokeActor("host").
		State("heard").
		Initial("hosting").
		Transition("hosting").On("nestedFailed").GoTo("heard").
		Transition("hosting").On("nestedDone").GoTo("heard").
		Quench()
	starts := map[string][]state.Effect{
		"fails": {state.SpawnActor{ID: "child", Src: state.Ref{Name: "broken"}, OnError: "nestedFailed"}},
		"settles": {
			state.SpawnActor{ID: "child", Src: state.Ref{Name: "finisher"}, OnDone: "nestedDone"},
			state.SendTo{TargetID: "child", Event: "finish"},
		},
	}
	for _, how := range []string{"fails", "settles"} {
		t.Run(how, func(t *testing.T) {
			build := func() (*state.Instance[string, string, *trec], *state.ActorSystem[string, string, *trec]) {
				inst := m.Cast(&trec{}, state.WithInitialState("hosting"))
				sys := state.NewActorSystem(inst)
				sys.Register("host", func(map[string]any) (state.ActorInstance, error) { return &starter{start: starts[how]}, nil })
				sys.Register("broken", func(map[string]any) (state.ActorInstance, error) { return nil, errors.New("can't start") })
				sys.Register("finisher", func(map[string]any) (state.ActorInstance, error) { return &starter{}, nil })
				return inst, sys
			}
			live, liveSys := build()
			liveSys.Absorb(ctx, live.StartEffects())
			if got := live.Current(); got != "heard" {
				t.Fatalf("Absorb left the parent in %q, want heard: the nested child's Fire should reach it", got)
			}
			resumed, sys := build()
			var seen []state.ParentFire[string]
			sys.WithParentFireObserver(func(pf state.ParentFire[string]) { seen = append(seen, pf) })
			sys.AbsorbRespawn(ctx, resumed.StartEffects())
			if got := resumed.Current(); got != "hosting" {
				t.Fatalf("AbsorbRespawn left the parent in %q, want hosting: the nested child's Fire is part of the host's start", got)
			}
			if len(seen) != 0 {
				t.Fatalf("observed %d parent Fires, want none", len(seen))
			}
		})
	}
}

// TestActorSystem_AbsorbRespawn_QuietResumesAfterANewFire proves a parent Fire
// AbsorbRespawn lets through, the onDone of a respawned actor another one's start ends,
// hears the parent only while it and its effects run: the hello the second actor's start
// sends next is still part of that start, and is dropped.
func TestActorSystem_AbsorbRespawn_QuietResumesAfterANewFire(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[*trec]("resume").
		Actor("finisher").
		Actor("poker").
		State("pair").InvokeActor("finisher", state.WithInvokeOnDone("finished")).InvokeActor("poker").
		State("finishedState").
		State("hellod").
		Initial("pair").
		Transition("pair").On("finished").GoTo("finishedState").
		Transition("finishedState").On("hello").GoTo("hellod").
		Quench()
	finisher := state.ActorID("resume", "pair", 0)
	inst := m.Cast(&trec{}, state.WithInitialState("pair"))
	sys := state.NewActorSystem(inst)
	sys.Register("finisher", func(map[string]any) (state.ActorInstance, error) { return &starter{}, nil })
	sys.Register("poker", func(map[string]any) (state.ActorInstance, error) {
		return &starter{start: []state.Effect{
			state.SendTo{TargetID: finisher, Event: "finish"},
			state.SendParent{Event: "hello"},
		}}, nil
	})
	sys.AbsorbRespawn(ctx, inst.StartEffects())
	if got := inst.Current(); got != "finishedState" {
		t.Fatalf("AbsorbRespawn left the parent in %q, want finishedState: the finisher's onDone is new, but the poker's hello is part of its start", got)
	}
}

// pinger is an actor that sends its parent "hello" each time it's delivered "ping".
type pinger struct{ out []state.Effect }

func (p *pinger) DeliverFire(_ context.Context, ev any) (bool, any) {
	if ev == "ping" {
		p.out = []state.Effect{state.SendParent{Event: "hello"}}
	}
	return false, nil
}

func (p *pinger) ChildEffects() []state.Effect {
	out := p.out
	p.out = nil
	return out
}

func (p *pinger) Output() any { return nil }

// TestActorSystem_AbsorbRespawn_QuietEndsWithIt proves the quiet of an AbsorbRespawn
// spawn ends when it returns, even for the context the actor's behavior got: a delivery
// made later with that context reaches the parent with the message it drives.
func TestActorSystem_AbsorbRespawn_QuietEndsWithIt(t *testing.T) {
	m := state.ForgeFor[*trec]("after").
		Actor("pinger").
		State("waiting").InvokeActor("pinger").
		State("hellod").
		Initial("waiting").
		Transition("waiting").On("hello").GoTo("hellod").
		Quench()
	inst := m.Cast(&trec{}, state.WithInitialState("waiting"))
	sys := state.NewActorSystem(inst)
	var captured context.Context
	sys.RegisterContext("pinger", func(ctx context.Context, _ map[string]any) (state.ActorInstance, error) {
		captured = ctx
		return &pinger{}, nil
	})
	sys.AbsorbRespawn(context.Background(), inst.StartEffects())
	if !sys.DeliverByID(captured, state.ActorID("after", "waiting", 0), "ping") {
		t.Fatal("the pinger isn't running")
	}
	if got := inst.Current(); got != "hellod" {
		t.Fatalf("a delivery after AbsorbRespawn left the parent in %q, want hellod: its hello should reach the parent", got)
	}
}
