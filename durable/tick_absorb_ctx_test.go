package durable_test

import (
	"context"
	"strings"
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

// TestTick_AContextThatEndsWhileTheTickSpawnsActors_RecordsAndLosesTheMessage pins that
// Tick doesn't check its context after it hands the fired timers' effects to the actor
// system, unlike RunService: only a parent Fire refused by a context fails it. A timer
// enters pair, which spawns target and then sender, whose behavior runs as the deadline
// passes and which pings target as it starts. Target refuses the ping, its context
// having ended, and the ping is lost live, as actor deliveries are across Recover. The
// tick records, so a retry doesn't run the behaviors again. Recover starts both actors
// fresh, and sender's new start pings the new target.
func TestTick_AContextThatEndsWhileTheTickSpawnsActors_RecordsAndLosesTheMessage(t *testing.T) {
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
	before := tailLen(t, store, id)
	clk.Advance(6 * time.Second)
	turn, c := context.WithCancel(ctx)
	cancel = c
	_, tickErr := h.Tick(turn)
	if turn.Err() == nil || len(targets) != 1 {
		t.Fatalf("setup: context ended %v, live spawned %d targets; want it ended, with one target", turn.Err() != nil, len(targets))
	}
	if tickErr != nil {
		t.Fatalf("Tick = %v, want the step recorded: only a parent Fire refused by a context fails it", tickErr)
	}
	wantTail(t, store, id, before+1)
	if targets[0].pinged {
		t.Fatal("live target was pinged, want the ping it refused under the ended context lost")
	}

	cancel = func() {}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "pair" || len(targets) != 2 || !targets[1].pinged {
		t.Fatalf("recovered in %q, %d targets spawned; want pair, with target respawned and pinged by sender's new start", got, len(targets))
	}
}

// TestFire_AContextThatEndsWhileTheFireSpawnsActors_RecordsAndLosesTheMessage pins that
// Fire doesn't check its context after it hands the event's effects to the actor
// system, as Tick doesn't: only a parent Fire refused by a context fails it. The event
// enters pair, which spawns target and then sender, whose behavior runs as the deadline
// passes and which pings target as it starts. Target refuses the ping, its context
// having ended, and the ping is lost live, as actor deliveries are across Recover. The
// Fire records, so a retry doesn't run the behaviors again. Recover starts both actors
// fresh, and sender's new start pings the new target.
func TestFire_AContextThatEndsWhileTheFireSpawnsActors_RecordsAndLosesTheMessage(t *testing.T) {
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
	if turn.Err() == nil || len(targets) != 1 {
		t.Fatalf("setup: context ended %v, live spawned %d targets; want it ended, with one target", turn.Err() != nil, len(targets))
	}
	if fireErr != nil {
		t.Fatalf("Fire = %v, want the step recorded: only a parent Fire refused by a context fails it", fireErr)
	}
	wantTail(t, store, id, before+1)
	if targets[0].pinged {
		t.Fatal("live target was pinged, want the ping it refused under the ended context lost")
	}

	cancel = func() {}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "pair" || len(targets) != 2 || !targets[1].pinged {
		t.Fatalf("recovered in %q, %d targets spawned; want pair, with target respawned and pinged by sender's new start", got, len(targets))
	}
}

// TestFire_ContextEndedWithNoActorPalette_Records pins that a Fire whose context ends
// once the kernel applied its event, here while the step armed its timer, and that drove
// no parent Fire, records. Without WithActorPalette, a state that declares an actor
// spawns none.
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

// endingActor is an actor whose delivery runs as the deadline passes: it ends the
// step's context, and keeps running.
type endingActor struct{ end func() }

func (e endingActor) DeliverFire(context.Context, any) (bool, any) {
	e.end()
	return false, nil
}
func (endingActor) ChildEffects() []state.Effect { return nil }
func (endingActor) Output() any                  { return nil }

// noteTarget is an actor that counts the messages delivered to it under a live
// context, and refuses one whose context ended, as an actor backed by an Instance does.
type noteTarget struct{ got int }

func (n *noteTarget) DeliverFire(ctx context.Context, _ any) (bool, any) {
	if ctx.Err() != nil {
		return false, nil
	}
	n.got++
	return false, nil
}
func (*noteTarget) ChildEffects() []state.Effect { return nil }
func (*noteTarget) Output() any                  { return nil }

// TestStep_AContextThatEndsWhileTheStepMessagesActors_RecordsAndLosesTheMessage pins
// that Fire and Tick don't fail when an actor refuses a message they sent it because
// their context ended. Pair runs ender and target; poke, and the timer due, each stay in
// pair and send ender a message, then target one, by SendTo or, for a Fire, by
// ForwardTo alone. Ender's delivery runs as the deadline passes, so target refuses its
// message, its context having ended. The message is lost, as actor deliveries are
// across Recover, and the step records: failing it would have the retry run the
// actors' behaviors again. A recovered Handle starts target fresh, without the message.
func TestStep_AContextThatEndsWhileTheStepMessagesActors_RecordsAndLosesTheMessage(t *testing.T) {
	for _, step := range []string{"Fire", "Fire forward", "Tick"} {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			machine := "messageCtx" + strings.ReplaceAll(step, " ", "")
			enderID := state.ActorID(machine, "pair", 0)
			targetID := state.ActorID(machine, "pair", 1)
			b := state.ForgeFor[stepCtx](machine).
				Actor("ender").
				Actor("target").
				State("idle").
				State("pair").InvokeActor("ender").InvokeActor("target").
				Initial("idle").
				Transition("idle").On("go").GoTo("pair")
			switch step {
			case "Fire":
				b = b.Transition("pair").On("poke").GoTo("pair").SendTo(enderID, "x").SendTo(targetID, "ping")
			case "Fire forward":
				b = b.Transition("pair").On("poke").GoTo("pair").ForwardTo(enderID).ForwardTo(targetID)
			default:
				b = b.Transition("pair").After(5*time.Second).On("due").GoTo("pair").SendTo(enderID, "x").SendTo(targetID, "ping")
			}
			m := b.Quench()
			clk := state.NewFakeClock(epoch)
			cancel := func() {}
			var targets []*noteTarget
			opts := []stepOption{
				durable.WithRunnerClock[string, string, stepCtx](clk),
				durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
					"ender": func(map[string]any) (state.ActorInstance, error) {
						return endingActor{end: func() { cancel() }}, nil
					},
					"target": func(map[string]any) (state.ActorInstance, error) {
						targets = append(targets, &noteTarget{})
						return targets[len(targets)-1], nil
					},
				}),
			}
			store, id := durable.NewMemStore(), durable.InstanceID("message-ctx")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "go"); err != nil {
				t.Fatalf("Fire(go): %v", err)
			}
			before := tailLen(t, store, id)
			run := func(h *durable.Handle[string, string, stepCtx], ctx context.Context) error {
				if step != "Tick" {
					_, fireErr := h.Fire(ctx, "poke")
					return fireErr
				}
				_, tickErr := h.Tick(ctx)
				return tickErr
			}
			clk.Advance(6 * time.Second)
			turn, c := context.WithCancel(ctx)
			cancel = c
			stepErr := run(h, turn)
			if turn.Err() == nil || len(targets) != 1 || targets[0].got != 0 {
				t.Fatalf("setup: context ended %v, live spawned %d targets, the first with %d messages; want it ended, with one target and none",
					turn.Err() != nil, len(targets), targets[0].got)
			}

			if stepErr != nil {
				t.Fatalf("%s = %v, want the step recorded: only a parent Fire refused by a context fails it", step, stepErr)
			}
			wantTail(t, store, id, before+1)

			cancel = func() {}
			recovered, err := durable.Recover(ctx, m, store, id, opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if got := recovered.Instance().Current(); got != "pair" || len(targets) != 2 || targets[1].got != 0 {
				t.Fatalf("recovered in %q, %d targets spawned; want pair, with target respawned once, without the lost message", got, len(targets))
			}
		})
	}
}

// liveOnceCtx is a context that ends partway through a step with no host code to
// end it, as a deadline does: once armed, it answers Err as live live more times,
// then as context.Canceled.
type liveOnceCtx struct {
	context.Context
	armed bool
	live  int
}

func (c *liveOnceCtx) Err() error {
	if !c.armed {
		return nil
	}
	if c.live > 0 {
		c.live--
		return nil
	}
	return context.Canceled
}

// TestStep_AContextThatEndsWithNoActorWork_Records pins that a step whose context ends
// after the kernel applied it, and that drove no parent Fire, records. A machine with timers and a
// service but no actor palette enters loading, which starts fetch and arms expire, by a
// Fire and by a timer. The context ends after the kernel applied the step: for the
// Fire, as it arms expire; for the Tick, after the check that follows the timers' Fires,
// where no host code runs before the absorb, so the context reports it ended from its
// next check on. Handing a service to the runner runs nothing, so the ended context
// cost neither step anything: both record, with fetch in flight, live and recovered.
func TestStep_AContextThatEndsWithNoActorWork_Records(t *testing.T) {
	for _, step := range []string{"Fire", "Tick"} {
		t.Run(step, func(t *testing.T) {
			ctx := context.Background()
			fetch := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { return "ok", nil }
			machine := "noActorWork" + step
			m := state.ForgeFor[stepCtx](machine).
				Service("fetch", fetch).
				State("idle").
				State("waiting").
				State("loading").Invoke("fetch", state.WithInvokeOnDone("fetched")).
				State("ready").
				Initial("idle").
				Transition("idle").On("load").GoTo("loading").
				Transition("idle").On("wait").GoTo("waiting").
				Transition("waiting").After(5 * time.Second).On("due").GoTo("loading").
				Transition("loading").After(time.Minute).On("expire").GoTo("idle").
				Transition("loading").On("fetched").GoTo("ready").
				Quench()
			fc := state.NewFakeClock(epoch)
			clk := &countingClock{Clock: fc}
			opts := []stepOption{
				durable.WithRunnerClock[string, string, stepCtx](clk),
				durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fetch)),
			}
			store, id := durable.NewMemStore(), durable.InstanceID("no-actor-work")
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			turn := &liveOnceCtx{Context: ctx}
			if step == "Fire" {
				before := tailLen(t, store, id)
				clk.onRead = func(int) { turn.armed = true } // arming expire, after the kernel returned
				_, err = h.Fire(turn, "load")
				clk.onRead = nil
				if !turn.armed {
					t.Fatal("the test didn't end the context")
				}
				if err != nil {
					t.Fatalf("Fire = %v, want the step recorded: it handed the actor system nothing", err)
				}
				wantTail(t, store, id, before+1)
			} else {
				if _, err = h.Fire(ctx, "wait"); err != nil {
					t.Fatalf("Fire(wait): %v", err)
				}
				before := tailLen(t, store, id)
				fc.Advance(6 * time.Second)
				base := clk.n
				clk.onRead = func(n int) {
					if n == base+3 { // the scheduler's last read, once due applied and expire armed
						turn.armed, turn.live = true, 1 // live for the check after the timers' Fires
					}
				}
				_, err = h.Tick(turn)
				clk.onRead = nil
				if !turn.armed || turn.live != 0 {
					t.Fatalf("setup: armed %v, %d live answers left; want the context to end after the check that follows the timers' Fires", turn.armed, turn.live)
				}
				if err != nil {
					t.Fatalf("Tick = %v, want the step recorded, as the Fire is: it handed the actor system nothing", err)
				}
				wantTail(t, store, id, before+1)
			}
			if got := h.Instance().Current(); got != "loading" {
				t.Fatalf("state = %q, want loading", got)
			}
			recovered, err := durable.Recover(ctx, m, store, id, opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			svcID := state.InvokeID(machine, "loading", 0)
			for name, h := range map[string]*durable.Handle[string, string, stepCtx]{"live": h, "recovered": recovered} {
				if _, ran, err := h.RunService(ctx, svcID); !ran || err != nil {
					t.Fatalf("%s RunService = %v, %v; want fetch in flight and settled", name, ran, err)
				}
			}
		})
	}
}

// TestFire_AContextThatEndsWithNoActorToReach_Records pins that a Fire whose context
// ends after the kernel applied it, and that drove no parent Fire, records. Leave exits pair, which stops idler, and responds to the event's
// sender, which a host Fire doesn't have, then arms a timer, as the context ends.
// Stopping an actor runs nothing an ended context can refuse, and a response with no
// sender reaches no actor, so the Fire cost nothing and records, with idler stopped
// live and recovered.
func TestFire_AContextThatEndsWithNoActorToReach_Records(t *testing.T) {
	ctx := context.Background()
	const machine = "noActorToReach"
	m := state.ForgeFor[stepCtx](machine).
		Actor("idler").
		State("pair").InvokeActor("idler").
		State("away").
		Initial("pair").
		Transition("pair").On("leave").GoTo("away").Respond("bye").
		Transition("away").After(time.Second).On("back").GoTo("pair").
		Quench()
	clk := &countingClock{Clock: state.NewFakeClock(epoch)}
	idlers := 0
	opts := []stepOption{
		durable.WithRunnerClock[string, string, stepCtx](clk),
		durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
			"idler": func(map[string]any) (state.ActorInstance, error) { idlers++; return &noteTarget{}, nil },
		}),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("no-actor-to-reach")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("pair"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	before := tailLen(t, store, id)
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	clk.onRead = func(int) { cancel() } // arming back, after the kernel returned
	_, err = h.Fire(turn, "leave")
	clk.onRead = nil
	if turn.Err() == nil {
		t.Fatal("the test didn't end the context")
	}
	if err != nil {
		t.Fatalf("Fire = %v, want the step recorded: it stopped an actor and responded to no one", err)
	}
	wantTail(t, store, id, before+1)
	idlerID := state.ActorID(machine, "pair", 0)
	if _, ok := h.ActorRef(idlerID); ok {
		t.Fatal("idler still runs live, want it stopped")
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != "away" || idlers != 1 {
		t.Fatalf("recovered in %q with idler spawned %d times; want away, spawned once", got, idlers)
	}
}
