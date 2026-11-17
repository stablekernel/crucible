package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// pingTarget is an actor that notes a ping delivered under a live context, and
// refuses one whose context ended, as an actor backed by an Instance does.
type pingTarget struct{ pinged bool }

func (p *pingTarget) DeliverFire(ctx context.Context, ev any) (bool, any) {
	if ev != "ping" || ctx.Err() != nil {
		return false, nil
	}
	p.pinged = true
	return true, nil
}
func (p *pingTarget) ChildEffects() []state.Effect { return nil }
func (p *pingTarget) Output() any                  { return nil }

// pingSender is an actor that pings its sibling as it starts.
type pingSender struct {
	target string
	sent   bool
}

func (p *pingSender) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (p *pingSender) ChildEffects() []state.Effect {
	if p.sent {
		return nil
	}
	p.sent = true
	return []state.Effect{state.SendTo{TargetID: p.target, Event: "ping"}}
}
func (p *pingSender) Output() any { return nil }

// TestTick_AContextThatEndsWhileTheTickSpawnsActors_RecordsNothing pins that Tick
// checks its context after it hands the fired timers' effects to the actor system, as
// RunService does after its absorb. A timer enters pair, which spawns target and then
// sender, whose behavior runs as the deadline passes and which pings target as it
// starts. Target refuses the ping, its context having ended, and no parent Fire reports
// that. Journaling the tick would have Recover deliver the ping under a live context,
// which live never applied, so the Tick fails the Handle and records nothing, and a
// recovered Handle re-arms the timer.
func TestTick_AContextThatEndsWhileTheTickSpawnsActors_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	const machine = "tickSpawnCtx"
	targetID := state.ActorID(machine, "pair", 0)
	m := state.ForgeFor[stepCtx](machine).
		Actor("target").
		Actor("sender").
		State("idle").
		State("waiting").
		State("pair").InvokeActor("target").InvokeActor("sender").
		Initial("idle").
		Transition("idle").On("go").GoTo("waiting").
		Transition("waiting").After(5 * time.Second).On("due").GoTo("pair").
		Quench()
	clk := state.NewFakeClock(epoch)
	cancel := func() {}
	var targets []*pingTarget
	opts := []stepOption{
		durable.WithRunnerClock[string, string, stepCtx](clk),
		durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
			"target": func(map[string]any) (state.ActorInstance, error) {
				targets = append(targets, &pingTarget{})
				return targets[len(targets)-1], nil
			},
			"sender": func(map[string]any) (state.ActorInstance, error) {
				cancel() // the deadline passes as sender starts
				return &pingSender{target: targetID}, nil
			},
		}),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("tick-spawn-ctx")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	clk.Advance(6 * time.Second)
	turn, c := context.WithCancel(ctx)
	cancel = c
	_, tickErr := h.Tick(turn)
	if len(targets) != 1 || targets[0].pinged {
		t.Fatalf("setup: live spawned %d targets, pinged %v; want one, unpinged", len(targets), len(targets) > 0 && targets[0].pinged)
	}

	cancel = func() {}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if !errors.Is(tickErr, durable.ErrHandleFailed) {
		recPinged := len(targets) > 1 && targets[len(targets)-1].pinged
		t.Fatalf("Tick = %v, want ErrHandleFailed: the context ended while it spawned actors, and live target refused the ping that recovered target (pinged %v) applied",
			tickErr, recPinged)
	}
	if got := recovered.Instance().Current(); got != "waiting" || len(targets) != 1 {
		t.Fatalf("recovered in %q, %d targets spawned; want waiting with none respawned: the tick recorded nothing", got, len(targets)-1)
	}
	if _, err = recovered.Tick(ctx); err != nil {
		t.Fatalf("Tick after Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "pair" || len(targets) != 2 || !targets[1].pinged {
		t.Fatalf("after the re-armed timer fired: state %q, %d targets; want pair, with the new target pinged", got, len(targets))
	}
}

// TestFire_AContextThatEndsWhileTheFireSpawnsActors_RecordsNothing pins that Fire
// checks its context after it hands the event's effects to the actor system, as Tick
// and RunService do after their absorbs. The event enters pair, which spawns target
// and then sender, whose behavior runs as the deadline passes and which pings target
// as it starts. Target refuses the ping, its context having ended, and no parent Fire
// reports that. Journaling the Fire would have Recover deliver the ping under a live
// context, which live never applied, so the Fire fails the Handle and records nothing,
// and the event fires again on a recovered Handle.
func TestFire_AContextThatEndsWhileTheFireSpawnsActors_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	const machine = "fireSpawnCtx"
	targetID := state.ActorID(machine, "pair", 0)
	m := state.ForgeFor[stepCtx](machine).
		Actor("target").
		Actor("sender").
		State("idle").
		State("pair").InvokeActor("target").InvokeActor("sender").
		Initial("idle").
		Transition("idle").On("go").GoTo("pair").
		Quench()
	cancel := func() {}
	var targets []*pingTarget
	opts := []stepOption{
		durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
			"target": func(map[string]any) (state.ActorInstance, error) {
				targets = append(targets, &pingTarget{})
				return targets[len(targets)-1], nil
			},
			"sender": func(map[string]any) (state.ActorInstance, error) {
				cancel() // the deadline passes as sender starts
				return &pingSender{target: targetID}, nil
			},
		}),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("fire-spawn-ctx")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	before := tailLen(t, store, id)
	turn, c := context.WithCancel(ctx)
	cancel = c
	_, fireErr := h.Fire(turn, "go")
	if len(targets) != 1 || targets[0].pinged {
		t.Fatalf("setup: live spawned %d targets, pinged %v; want one, unpinged", len(targets), len(targets) > 0 && targets[0].pinged)
	}

	cancel = func() {}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if !errors.Is(fireErr, durable.ErrHandleFailed) || !errors.Is(fireErr, context.Canceled) {
		recPinged := len(targets) > 1 && targets[len(targets)-1].pinged
		t.Fatalf("Fire = %v, want ErrHandleFailed wrapping context.Canceled: the context ended while it spawned actors, and live target refused the ping that recovered target (pinged %v) applied",
			fireErr, recPinged)
	}
	wantTail(t, store, id, before)
	if got := recovered.Instance().Current(); got != "idle" || len(targets) != 1 {
		t.Fatalf("recovered in %q, %d targets spawned; want idle with none respawned: the Fire recorded nothing", got, len(targets)-1)
	}
	if _, err = recovered.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire after Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "pair" || len(targets) != 2 || !targets[1].pinged {
		t.Fatalf("after the event fired again: state %q, %d targets; want pair, with the new target pinged", got, len(targets))
	}
}

// TestFire_ContextEndedWithNoActorPalette_Records pins that the recheck is for a Fire
// whose actors spawned. Without WithActorPalette, a state that declares an actor spawns
// none, so a Fire whose context ends once the kernel applied its event, here while the
// step armed its timer, cost nothing and records, as one entering a state with no
// actors does.
func TestFire_ContextEndedWithNoActorPalette_Records(t *testing.T) {
	ctx := context.Background()
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	m := state.ForgeFor[stepCtx]("fireNoPalette").
		Actor("target").
		State("idle").
		State("pair").InvokeActor("target").
		State("done").
		Initial("idle").
		Transition("idle").On("go").GoTo("pair").
		Transition("pair").After(time.Second).On("due").GoTo("done").
		Quench()
	clk := &countingClock{Clock: state.NewFakeClock(epoch)}
	store, id := durable.NewMemStore(), durable.InstanceID("fire-no-palette")
	h, err := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, stepCtx](clk)).
		Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	before := tailLen(t, store, id)
	clk.onRead = func(int) { cancel() } // arming the timer, after the kernel returned
	_, err = h.Fire(turn, "go")
	clk.onRead = nil
	if turn.Err() == nil {
		t.Fatal("the test didn't end the context")
	}
	if err != nil {
		t.Fatalf("Fire = %v, want the step recorded: no actor spawned, so the ended context cost nothing", err)
	}
	wantTail(t, store, id, before+1)
	if got := h.Instance().Current(); got != "pair" {
		t.Fatalf("state = %q, want pair", got)
	}
}
