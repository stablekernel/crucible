package durable_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// stepCtx is the JSON-marshalable context for the step-rule fixtures.
type stepCtx struct {
	Notes []string `json:"notes"`
}

type stepMachine = *state.Machine[string, string, stepCtx]

type stepOption = durable.Option[string, string, stepCtx]

// cancelOn is a context middleware that fires event under a context derived from the
// caller's and already ended, while the caller's stays live.
func cancelOn(event string) state.Middleware[string, string, stepCtx] {
	return func(next state.FireFunc[string, string, stepCtx]) state.FireFunc[string, string, stepCtx] {
		return func(ctx context.Context, ev string) state.FireResult[string] {
			if ev == event {
				derived, cancel := context.WithCancel(ctx)
				cancel()
				return next(derived, ev)
			}
			return next(ctx, ev)
		}
	}
}

// failingSpawn is an actor behavior that never starts: it runs hook, if any, then
// fails, so the parent's onError routes.
func failingSpawn(hook func()) state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		if hook != nil {
			hook()
		}
		return nil, errors.New("the child can't start")
	}
}

// finishingChild is an actor that completes when delivered "finish".
func finishingChild() state.ActorBehavior {
	var runs int64
	return counterChild(&runs)
}

func wantFailedWithCancel(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("%s = %v, want ErrHandleFailed wrapping context.Canceled", what, err)
	}
}

func wantTail(t *testing.T, store durable.Store, id durable.InstanceID, want int) {
	t.Helper()
	if got := tailLen(t, store, id); got != want {
		t.Fatalf("journal has %d records, want %d", got, want)
	}
}

// spawnOnStartMachine is idle -start-> spawning, a state whose actor can't start, so
// the absorb of Fire's own effects routes spawnFailed through the parent.
func spawnOnStartMachine(mw state.Middleware[string, string, stepCtx]) stepMachine {
	b := state.ForgeFor[stepCtx]("spawner").
		Actor("spawnFail").
		State("idle").
		State("spawning").InvokeActor("spawnFail", state.WithInvokeOnError("spawnFailed")).
		State("failed").Final().
		Initial("idle")
	if mw != nil {
		b = b.Use(mw)
	}
	return b.Transition("idle").On("start").GoTo("spawning").
		Transition("spawning").On("spawnFailed").GoTo("failed").
		Quench()
}

// TestFire_SpawnRefusedByAnEndedContext_RecordsNothing proves Fire follows the one
// rule: when the onError of an actor its absorb spawned is refused because a context
// ended, whether the caller's or one derived from it, Fire fails the Handle and
// records nothing, so Recover can't apply the onError live never applied.
func TestFire_SpawnRefusedByAnEndedContext_RecordsNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		derived bool
	}{{"the caller's context", false}, {"a derived context", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			turn, cancel := context.WithCancel(ctx)
			defer cancel()
			var mw state.Middleware[string, string, stepCtx]
			hook := cancel
			if tc.derived {
				mw, hook = cancelOn("spawnFailed"), nil
			}
			m := spawnOnStartMachine(mw)
			store := durable.NewMemStore()
			id := durable.InstanceID("spawn-on-start")
			opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"spawnFail": failingSpawn(hook)})}
			h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			before := tailLen(t, store, id)
			_, err = h.Fire(turn, "start")
			wantFailedWithCancel(t, "Fire", err)
			wantTail(t, store, id, before)
			recovered, err := durable.Recover(ctx, m, store, id, opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if got := recovered.Instance().Current(); got != "idle" {
				t.Fatalf("recovered state = %q, want idle, since nothing was recorded", got)
			}
		})
	}
}

// messengerUsing is messengerMachine with mw installed.
func messengerUsing(mw state.Middleware[string, string, stepCtx]) stepMachine {
	return state.ForgeFor[stepCtx]("messenger").
		Actor("spawnPinger").
		State("listening").InvokeActor("spawnPinger", state.WithInvokeOnDone("pingerDone")).
		State("heard").
		Initial("listening").
		Use(mw).
		Transition("listening").On("pong").GoTo("heard").
		Transition("heard").On("pingerDone").GoTo("heard").
		Quench()
}

// TestDeliverToActor_MessageRefusedByADerivedContext_RecordsNothing proves a delivery
// whose child's message to the parent (SendParent) is refused by a context derived
// from the caller's, while the caller's is live, fails the Handle and records nothing.
func TestDeliverToActor_MessageRefusedByADerivedContext_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	m := messengerUsing(cancelOn("pong"))
	store := durable.NewMemStore()
	id := durable.InstanceID("msg-derived")
	var msgs int64
	palette := map[string]state.ActorBehavior{"spawnPinger": pingerChild(&msgs)}
	h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, stepCtx](palette)).
		Start(ctx, id, stepCtx{}, state.WithInitialState("listening"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("messenger", "listening", 0))
	if !ok {
		t.Fatal("no actor ref for the pinger")
	}
	before := tailLen(t, store, id)
	_, err = h.DeliverToActor(ctx, ref, "ping")
	wantFailedWithCancel(t, "DeliverToActor", err)
	wantTail(t, store, id, before)
}

// TestDeliverToActor_SpawnRefusedByADerivedContext_RecordsNothing proves a delivery
// whose parent transition enters a state with an actor that can't start, and whose
// onError a derived context refuses, fails the Handle and records nothing.
func TestDeliverToActor_SpawnRefusedByADerivedContext_RecordsNothing(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[stepCtx]("relay").
		Actor("spawnChild").
		Actor("spawnFail").
		State("supervising").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone")).
		State("respawning").InvokeActor("spawnFail", state.WithInvokeOnError("spawnFailed")).
		State("failed").Final().
		Initial("supervising").
		Use(cancelOn("spawnFailed")).
		Transition("supervising").On("childDone").GoTo("respawning").
		Transition("respawning").On("spawnFailed").GoTo("failed").
		Quench()
	store := durable.NewMemStore()
	id := durable.InstanceID("respawn-derived")
	palette := map[string]state.ActorBehavior{"spawnChild": finishingChild(), "spawnFail": failingSpawn(nil)}
	h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, stepCtx](palette)).
		Start(ctx, id, stepCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("relay", "supervising", 0))
	if !ok {
		t.Fatal("no actor ref for the child")
	}
	before := tailLen(t, store, id)
	_, err = h.DeliverToActor(ctx, ref, "finish")
	wantFailedWithCancel(t, "DeliverToActor", err)
	wantTail(t, store, id, before)
}

// expiringMachine is idle -arm-> waiting -(after 1s: expire)-> next, with mw
// installed and next's declarations added by decl.
func expiringMachine(mw state.Middleware[string, string, stepCtx], decl func(*state.Builder[string, string, stepCtx]) *state.Builder[string, string, stepCtx]) stepMachine {
	b := state.ForgeFor[stepCtx]("expiring").
		State("idle").
		State("waiting")
	b = decl(b)
	b = b.Initial("idle")
	if mw != nil {
		b = b.Use(mw)
	}
	return b.Transition("idle").On("arm").GoTo("waiting").
		Transition("waiting").After(time.Second).On("expire").GoTo("next").
		Quench()
}

func doneNext(b *state.Builder[string, string, stepCtx]) *state.Builder[string, string, stepCtx] {
	return b.State("next").Final()
}

// startArmed starts m with opts, a fake clock, and fires arm, so a 1s timer is armed.
func startArmed(t *testing.T, m stepMachine, store durable.Store, id durable.InstanceID, opts ...stepOption) (*durable.Handle[string, string, stepCtx], *state.FakeClock, []stepOption) {
	t.Helper()
	fc := state.NewFakeClock(epoch)
	opts = append(opts, durable.WithRunnerClock[string, string, stepCtx](fc))
	h, err := durable.NewRunner(m, store, opts...).Start(context.Background(), id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(context.Background(), "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	return h, fc, opts
}

// TestTick_RefusedByADerivedContext_RecordsNothing proves a Tick whose timer's Fire a
// derived context refuses, while the caller's is live, fails the Handle and records
// nothing.
func TestTick_RefusedByADerivedContext_RecordsNothing(t *testing.T) {
	m := expiringMachine(cancelOn("expire"), doneNext)
	store := durable.NewMemStore()
	id := durable.InstanceID("tick-derived")
	h, fc, _ := startArmed(t, m, store, id)
	before := tailLen(t, store, id)
	fc.Advance(time.Second)
	_, err := h.Tick(context.Background())
	wantFailedWithCancel(t, "Tick", err)
	wantTail(t, store, id, before)
}

// spawnAtStartMachine starts in spawning, whose actor can't start and whose onError a
// derived context refuses, so Start's and Recover's own spawn drive a refused parent
// Fire that belongs to no step.
func spawnAtStartMachine() stepMachine {
	return state.ForgeFor[stepCtx]("spawn-at-start").
		Actor("spawnFail").
		State("spawning").InvokeActor("spawnFail", state.WithInvokeOnError("spawnFailed")).
		State("poked").
		State("failed").Final().
		Initial("spawning").
		Use(cancelOn("spawnFailed")).
		Transition("spawning").On("spawnFailed").GoTo("failed").
		Transition("spawning").On("poke").GoTo("poked").
		Quench()
}

// TestStartAndRecover_SpawnRefusal_Fails proves a parent Fire a context refused while
// Start or Recover spawned the resume point's actors fails the call: it never applied,
// so the Handle would hold state the journal can't reach.
func TestStartAndRecover_SpawnRefusal_Fails(t *testing.T) {
	ctx := context.Background()
	m := spawnAtStartMachine()
	opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"spawnFail": failingSpawn(nil)})}
	store := durable.NewMemStore()
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, "refused", stepCtx{}, state.WithInitialState("spawning"))
	if h != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = Handle %v, %v, want no Handle and context.Canceled", h != nil, err)
	}
	h, err = durable.Recover(ctx, m, store, "refused", opts...)
	if h != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover = Handle %v, %v, want no Handle and context.Canceled", h != nil, err)
	}
}

// TestFire_ContextEndedAfterACleanApply_Records proves a Fire that drove nothing
// refused applied whole, so it records and returns no error even when its context
// ended after the kernel applied the event, here while the step armed its timer.
func TestFire_ContextEndedAfterACleanApply_Records(t *testing.T) {
	ctx := context.Background()
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	m := expiringMachine(nil, doneNext)
	store := durable.NewMemStore()
	id := durable.InstanceID("clean-apply")
	clk := &countingClock{Clock: state.NewFakeClock(epoch)}
	h, err := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, stepCtx](clk)).
		Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	before := tailLen(t, store, id)
	clk.onRead = func(int) { cancel() } // arming the timer, after the kernel returned
	_, err = h.Fire(turn, "arm")
	clk.onRead = nil
	if turn.Err() == nil {
		t.Fatal("the test didn't end the context")
	}
	if err != nil {
		t.Fatalf("Fire = %v, want the clean step recorded", err)
	}
	wantTail(t, store, id, before+1)
	if got := h.Instance().Current(); got != "waiting" {
		t.Fatalf("state = %q, want waiting", got)
	}
}

// noteMachine records each event its transitions take, with the data a settle
// carried, so a recovered context shows which Fire got the actor's output.
func noteMachine() stepMachine {
	note := func(prefix string) func(state.AssignCtx[stepCtx]) stepCtx {
		return func(in state.AssignCtx[stepCtx]) stepCtx {
			c := in.Entity
			c.Notes = append(c.Notes, fmt.Sprintf("%s:%v", prefix, in.Event))
			return c
		}
	}
	return state.ForgeFor[stepCtx]("noter").
		Reducer("noteDone", note("done")).
		Reducer("noteFail", note("fail")).
		Actor("spawnChild").
		Actor("spawnFail").
		State("supervising").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone")).
		State("respawning").InvokeActor("spawnFail", state.WithInvokeOnError("spawnFailed")).
		State("failed").Final().
		Initial("supervising").
		Transition("supervising").On("childDone").GoTo("respawning").Assign("noteDone").
		Transition("respawning").On("spawnFailed").GoTo("failed").Assign("noteFail").
		Quench()
}

// TestDeliverToActor_OutcomeGoesToTheSettlingFire proves that when one delivery drives
// several parent Fires, the actor's output is recorded with the Fire that settled it,
// not the last one, so Recover reaches the live context.
func TestDeliverToActor_OutcomeGoesToTheSettlingFire(t *testing.T) {
	ctx := context.Background()
	m := noteMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("outcome")
	var runs int64
	opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
		"spawnChild": counterChild(&runs),
		"spawnFail":  failingSpawn(nil),
	})}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("noter", "supervising", 0))
	if !ok {
		t.Fatal("no actor ref for the child")
	}
	if _, err = h.DeliverToActor(ctx, ref, "finish"); err != nil {
		t.Fatalf("DeliverToActor: %v", err)
	}
	live := h.Instance().Entity().Notes
	if len(live) != 2 || live[0] != "done:out-1" || live[1] != "fail:spawnFailed" {
		t.Fatalf("live notes = %v, want the output on childDone, then the plain spawnFailed", live)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Entity().Notes; fmt.Sprint(got) != fmt.Sprint(live) {
		t.Fatalf("recovered notes = %v, want the live %v", got, live)
	}
}

// TestFire_PanicInHostCode_FailsTheHandle proves a panic in host code during a step,
// here an actor behavior the step's absorb spawned after the kernel applied the event,
// fails the Handle, since the instance is ahead of the journal, and a later step can't
// record on top of it.
func TestFire_PanicInHostCode_FailsTheHandle(t *testing.T) {
	ctx := context.Background()
	m := spawnOnStartMachine(nil)
	store := durable.NewMemStore()
	id := durable.InstanceID("panics")
	panicking := func(map[string]any) (state.ActorInstance, error) { panic("the child exploded") }
	h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"spawnFail": panicking})).
		Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	before := tailLen(t, store, id)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Fire didn't panic")
			}
		}()
		_, _ = h.Fire(ctx, "start")
	}()
	wantTail(t, store, id, before)
	if _, err = h.Fire(ctx, "spawnFailed"); !errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("Fire after the panic = %v, want ErrHandleFailed", err)
	}
	wantTail(t, store, id, before)
}

// watcherMachine invokes the counter child on supervising; childDone and the other
// events stay in supervising, so the parent keeps declaring the child after it settles.
// how decides what childDone does: "refuse" (a guard fails), "stay" (applies without
// leaving), or "stop" (applies and stops the child explicitly). svcDone, from a service
// the state also invokes, re-enters supervising.
func watcherMachine(how string, withService bool) (*state.Machine[string, string, actorCtx], []durable.Option[string, string, actorCtx]) {
	var runs int64
	svc := func(context.Context, state.ServiceCtx[actorCtx]) (any, error) { return "ok", nil }
	b := state.ForgeFor[actorCtx]("watcher").
		Guard("never", func(state.GuardCtx[actorCtx]) bool { return false }).
		Service("svc", svc).
		Actor("spawnChild").
		State("supervising").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone"))
	if withService {
		b = b.Invoke("svc", state.WithInvokeOnDone("svcDone"))
	}
	b = b.State("complete").Final().Initial("supervising")
	switch how {
	case "refuse":
		b = b.Transition("supervising").On("childDone").GoTo("complete").When("never")
	case "stay":
		b = b.Transition("supervising").On("childDone").GoTo("supervising")
	case "stop":
		b = b.Transition("supervising").On("childDone").GoTo("supervising").StopActor(state.ActorID("watcher", "supervising", 0))
	}
	m := b.Transition("supervising").On("svcDone").GoTo("supervising").Reenter().
		Transition("supervising").On("poke").GoTo("supervising").
		Quench()
	opts := []durable.Option[string, string, actorCtx]{durable.WithActorPalette[string, string, actorCtx](counterPalette(&runs))}
	if withService {
		opts = append(opts, durable.WithServiceRegistry[string, string, actorCtx](state.NewRegistry[actorCtx]().Service("svc", svc)))
	}
	return m, opts
}

// TestRecover_SpawnsTheActorsLiveHadRunning proves Recover spawns only the declared
// actors the live run still had running, as the journal records them: an actor that
// settled while its parent stayed in the declaring state, whether its onDone was
// refused, applied without leaving, or stopped it, isn't spawned again, with or
// without a checkpoint compacting the step; one a later step spawned again is.
func TestRecover_SpawnsTheActorsLiveHadRunning(t *testing.T) {
	ctx := context.Background()
	childID := state.ActorID("watcher", "supervising", 0)
	for _, how := range []string{"refuse", "stay", "stop"} {
		for _, every := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("%s/checkpoint every %d", how, every), func(t *testing.T) {
				m, opts := watcherMachine(how, false)
				if every > 0 {
					opts = append(opts, durable.WithCheckpointEvery[string, string, actorCtx](every))
				}
				store := durable.NewMemStore()
				id := durable.InstanceID("watch")
				h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("supervising"))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				ref, ok := h.ActorRef(childID)
				if !ok {
					t.Fatal("no actor ref for the child")
				}
				if _, err = h.DeliverToActor(ctx, ref, "finish"); err != nil {
					t.Fatalf("DeliverToActor: %v", err)
				}
				recovered, err := durable.Recover(ctx, m, store, id, opts...)
				if err != nil {
					t.Fatalf("Recover: %v", err)
				}
				if found, _ := recovered.DeliverToActor(ctx, ref, "finish"); found {
					t.Fatal("recovered: the actor that settled live is running again")
				}
				// A step on the recovered Handle, then a second Recover, keeps it ended.
				if _, err = recovered.Fire(ctx, "poke"); err != nil {
					t.Fatalf("Fire(poke): %v", err)
				}
				again, err := durable.Recover(ctx, m, store, id, opts...)
				if err != nil {
					t.Fatalf("second Recover: %v", err)
				}
				if found, _ := again.DeliverToActor(ctx, ref, "finish"); found {
					t.Fatal("second Recover: the actor that settled live is running again")
				}
			})
		}
	}
	t.Run("a service re-entering spawns it again", func(t *testing.T) {
		m, opts := watcherMachine("refuse", true)
		store := durable.NewMemStore()
		id := durable.InstanceID("watch-svc")
		h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("supervising"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		ref, _ := h.ActorRef(childID)
		if _, err = h.DeliverToActor(ctx, ref, "finish"); err != nil {
			t.Fatalf("DeliverToActor: %v", err)
		}
		mustRunService(t, h, state.InvokeID("watcher", "supervising", 1))
		recovered, err := durable.Recover(ctx, m, store, id, opts...)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if found, _ := recovered.DeliverToActor(ctx, ref, "finish"); !found {
			t.Fatal("recovered: the actor the service's re-entry spawned again isn't running")
		}
	})
}

// TestDeliverToActor_MiddlewareDeniedParentFire_Recovers proves a parent Fire that
// middleware denied before the kernel saw it is journaled as refused, and Recover
// meets the same refusal instead of failing on the event it has none of.
func TestDeliverToActor_MiddlewareDeniedParentFire_Recovers(t *testing.T) {
	ctx := context.Background()
	deny := func(next state.FireFunc[string, string, actorCtx]) state.FireFunc[string, string, actorCtx] {
		return func(ctx context.Context, event string) state.FireResult[string] {
			if event == "childDone" {
				return state.FireResult[string]{Err: errors.New("denied by policy")}
			}
			return next(ctx, event)
		}
	}
	m := supervisorUsing(deny, false)
	store := durable.NewMemStore()
	id := durable.InstanceID("denied")
	var runs int64
	opts := []durable.Option[string, string, actorCtx]{durable.WithActorPalette[string, string, actorCtx](counterPalette(&runs))}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, actorCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("supervisor", "supervising", 0))
	if !ok {
		t.Fatal("no actor ref for the child")
	}
	if _, err = h.DeliverToActor(ctx, ref, "finish"); err != nil {
		t.Fatalf("DeliverToActor: %v", err)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if got := recovered.Instance().Current(); got != h.Instance().Current() {
		t.Fatalf("recovered state = %q, want the live %q", got, h.Instance().Current())
	}
}

// TestTick_FiresNothing_ContextEnded_StaysHealthy proves a Tick that fired no timer
// keeps the Handle healthy even when its context ends during the call, since it
// consumed nothing an ended context could have cost it.
func TestTick_FiresNothing_ContextEnded_StaysHealthy(t *testing.T) {
	ctx := context.Background()
	m := expiringMachine(nil, doneNext)
	store := durable.NewMemStore()
	id := durable.InstanceID("idle-tick")
	clk := &countingClock{Clock: state.NewFakeClock(epoch)}
	h, err := durable.NewRunner(m, store, durable.WithRunnerClock[string, string, stepCtx](clk)).
		Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil {
		t.Fatalf("Fire(arm): %v", err)
	}
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	clk.onRead = func(int) { cancel() } // the timer isn't due yet
	results, err := h.Tick(turn)
	clk.onRead = nil
	if turn.Err() == nil {
		t.Fatal("the test didn't end the context")
	}
	if err != nil || len(results) != 0 {
		t.Fatalf("Tick = %d results, %v; want none and no error", len(results), err)
	}
	if _, err = h.Fire(ctx, "arm"); errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("the Handle failed: %v", err)
	}
}

// TestRecover_RetriesAFailedSpawn proves an actor whose spawn failed live, which never
// ran, isn't counted as ended: Recover spawns it again, as it did before the journal
// recorded ended actors.
func TestRecover_RetriesAFailedSpawn(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[actorCtx]("retry").
		Actor("spawnChild").
		State("working").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone")).
		State("done").Final().
		Initial("working").
		Transition("working").On("next").GoTo("working").
		Transition("working").On("childDone").GoTo("done").
		Quench()
	store := durable.NewMemStore()
	id := durable.InstanceID("retry")
	failing := map[string]state.ActorBehavior{"spawnChild": failingSpawn(nil)}
	h, err := durable.NewRunner(m, store, durable.WithActorPalette[string, string, actorCtx](failing)).
		Start(ctx, id, actorCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "next"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	var runs int64
	recovered, err := durable.Recover(ctx, m, store, id, durable.WithActorPalette[string, string, actorCtx](counterPalette(&runs)))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, ok := recovered.ActorRef(state.ActorID("retry", "working", 0)); !ok {
		t.Fatal("Recover didn't spawn the actor whose live spawn failed")
	}
}

// failNoteMachine has a state whose actor can't start; its onError applies, staying in
// the state (stay) or leaving it (leave), and notes the failure, so a recovered
// context shows how many times it applied.
func failNoteMachine(initial string, leave bool) stepMachine {
	fail := func(in state.AssignCtx[stepCtx]) stepCtx {
		c := in.Entity
		c.Notes = append(c.Notes, "fail")
		return c
	}
	target := "spawning"
	if leave {
		target = "failed"
	}
	poke := func(in state.AssignCtx[stepCtx]) stepCtx {
		c := in.Entity
		c.Notes = append(c.Notes, "poke")
		return c
	}
	return state.ForgeFor[stepCtx]("noting").
		Reducer("noteFail", fail).
		Reducer("notePoke", poke).
		Actor("spawnChild").
		State("idle").
		State("spawning").InvokeActor("spawnChild", state.WithInvokeOnError("spawnFailed")).
		State("failed").
		Initial(initial).
		Transition("idle").On("start").GoTo("spawning").
		Transition("spawning").On("spawnFailed").GoTo(target).Assign("noteFail").
		Transition("spawning").On("poke").GoTo("spawning").Assign("notePoke").
		Transition("failed").On("poke").GoTo("failed").Assign("notePoke").
		Quench()
}

// TestRecover_ReplaysASpawnsOnErrorOnce proves a failed spawn's onError that applied is
// journaled with the step that drove it, or with the step after Start or Recover drove
// it, so Recover reaches the live state and context, applying it once, and doesn't
// retry the spawn.
func TestRecover_ReplaysASpawnsOnErrorOnce(t *testing.T) {
	ctx := context.Background()
	failing := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"spawnChild": failingSpawn(nil)})}
	check := func(t *testing.T, h *durable.Handle[string, string, stepCtx], m stepMachine, store durable.Store, id durable.InstanceID, opts []stepOption) {
		t.Helper()
		liveState, liveNotes := h.Instance().Current(), fmt.Sprint(h.Instance().Entity().Notes)
		for round := 1; round <= 2; round++ {
			recovered, err := durable.Recover(ctx, m, store, id, opts...)
			if err != nil {
				t.Fatalf("Recover %d: %v", round, err)
			}
			if got, notes := recovered.Instance().Current(), fmt.Sprint(recovered.Instance().Entity().Notes); got != liveState || notes != liveNotes {
				t.Fatalf("Recover %d = %s %s, want the live %s %s", round, got, notes, liveState, liveNotes)
			}
			if _, err = recovered.Fire(ctx, "poke"); err != nil {
				t.Fatalf("Fire(poke) after Recover %d: %v", round, err)
			}
			// The next Recover rebuilds the Handle that just continued.
			liveState, liveNotes = recovered.Instance().Current(), fmt.Sprint(recovered.Instance().Entity().Notes)
		}
	}
	for _, leave := range []bool{false, true} {
		for _, every := range []int{0, 1} {
			opts := append([]stepOption{}, failing...)
			if every > 0 {
				opts = append(opts, durable.WithCheckpointEvery[string, string, stepCtx](every))
			}
			t.Run(fmt.Sprintf("Fire/leave=%v/every=%d", leave, every), func(t *testing.T) {
				m, store, id := failNoteMachine("idle", leave), durable.NewMemStore(), durable.InstanceID("fire")
				h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				if _, err = h.Fire(ctx, "start"); err != nil {
					t.Fatalf("Fire(start): %v", err)
				}
				check(t, h, m, store, id, opts)
			})
			t.Run(fmt.Sprintf("Start/leave=%v/every=%d", leave, every), func(t *testing.T) {
				m, store, id := failNoteMachine("spawning", leave), durable.NewMemStore(), durable.InstanceID("start")
				h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("spawning"))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				if _, err = h.Fire(ctx, "poke"); err != nil {
					t.Fatalf("Fire(poke): %v", err)
				}
				check(t, h, m, store, id, opts)
			})
		}
	}
	t.Run("RunService", func(t *testing.T) {
		fetch := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { return "ok", nil }
		fail := func(in state.AssignCtx[stepCtx]) stepCtx {
			c := in.Entity
			c.Notes = append(c.Notes, "fail")
			return c
		}
		m := state.ForgeFor[stepCtx]("noting").
			Service("fetch", fetch).
			Reducer("noteFail", fail).
			Actor("spawnChild").
			State("loading").Invoke("fetch", state.WithInvokeOnDone("ok")).
			State("spawning").InvokeActor("spawnChild", state.WithInvokeOnError("spawnFailed")).
			Initial("loading").
			Transition("loading").On("ok").GoTo("spawning").
			Transition("spawning").On("spawnFailed").GoTo("spawning").Assign("noteFail").
			Transition("spawning").On("poke").GoTo("spawning").
			Quench()
		opts := append([]stepOption{durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("fetch", fetch))}, failing...)
		store, id := durable.NewMemStore(), durable.InstanceID("settle")
		h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("loading"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, ran, err := h.RunService(ctx, state.InvokeID("noting", "loading", 0)); err != nil || !ran {
			t.Fatalf("RunService = %v, %v", ran, err)
		}
		check(t, h, m, store, id, opts)
	})
	t.Run("a spawn that fails during Recover", func(t *testing.T) {
		m, store, id := failNoteMachine("spawning", false), durable.NewMemStore(), durable.InstanceID("at-recover")
		good := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"spawnChild": finishingChild()})}
		h, err := durable.NewRunner(m, store, good...).Start(ctx, id, stepCtx{}, state.WithInitialState("spawning"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "poke"); err != nil {
			t.Fatalf("Fire(poke): %v", err)
		}
		// Recover while the actor can't start: its onError applies on this Handle.
		r1, err := durable.Recover(ctx, m, store, id, failing...)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if _, err = r1.Fire(ctx, "poke"); err != nil {
			t.Fatalf("Fire(poke): %v", err)
		}
		// A later Recover, with the actor able to start, reaches what r1 had.
		r2, err := durable.Recover(ctx, m, store, id, good...)
		if err != nil {
			t.Fatalf("second Recover: %v", err)
		}
		if got, want := fmt.Sprint(r2.Instance().Entity().Notes), fmt.Sprint(r1.Instance().Entity().Notes); got != want {
			t.Fatalf("second Recover notes = %s, want r1's %s", got, want)
		}
		if found, _ := r2.DeliverToActor(ctx, state.ActorRef{ID: state.ActorID("noting", "spawning", 0)}, "finish"); found {
			t.Fatal("second Recover spawned the actor r1 had handled as failed")
		}
	})
}

// TestRecover_RetriesASpawnWhoseLatestOnErrorWasRefused proves the latest spawn attempt
// decides: a failed spawn's onError applies and re-enters the state, the spawn there
// fails too, and the parent refuses that onError, so the actor isn't handled and
// Recover tries it again.
func TestRecover_RetriesASpawnWhoseLatestOnErrorWasRefused(t *testing.T) {
	ctx := context.Background()
	fail := func(in state.AssignCtx[stepCtx]) stepCtx {
		c := in.Entity
		c.Notes = append(c.Notes, "fail")
		return c
	}
	m := state.ForgeFor[stepCtx]("again").
		Guard("firstFailure", func(in state.GuardCtx[stepCtx]) bool { return len(in.Entity.Notes) == 0 }).
		Reducer("noteFail", fail).
		Actor("spawnChild").
		State("idle").
		State("spawning").InvokeActor("spawnChild", state.WithInvokeOnError("spawnFailed")).
		Initial("idle").
		Transition("idle").On("start").GoTo("spawning").
		Transition("spawning").On("spawnFailed").GoTo("spawning").Reenter().When("firstFailure").Assign("noteFail").
		Quench()
	store, id := durable.NewMemStore(), durable.InstanceID("again")
	failing := durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"spawnChild": failingSpawn(nil)})
	h, err := durable.NewRunner(m, store, failing).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	recovered, err := durable.Recover(ctx, m, store, id, durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"spawnChild": finishingChild()}))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, ok := recovered.ActorRef(state.ActorID("again", "spawning", 0)); !ok {
		t.Fatal("Recover didn't retry the spawn whose latest onError the parent refused")
	}
}

// TestStart_FailsWhenItsContextEndsDuringASpawn proves Start returns no Handle when its
// context ends while an initial actor's spawn fails, so the parent refuses its onError:
// live never applied it. Recover starts from the baseline, retries the spawn, and with
// a live context the onError applies.
func TestStart_FailsWhenItsContextEndsDuringASpawn(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[stepCtx]("held").
		Actor("spawnFail").
		State("spawning").InvokeActor("spawnFail", state.WithInvokeOnError("spawnFailed")).
		State("failed").
		Initial("spawning").
		Transition("spawning").On("spawnFailed").GoTo("failed").
		Transition("spawning").On("poke").GoTo("spawning").
		Quench()
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	// The spawn ends Start's own context, so the parent refuses its onError.
	ending := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"spawnFail": failingSpawn(cancel)})}
	opts := []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"spawnFail": failingSpawn(nil)})}
	store, id := durable.NewMemStore(), durable.InstanceID("held")
	h, err := durable.NewRunner(m, store, ending...).Start(turn, id, stepCtx{}, state.WithInitialState("spawning"))
	if h != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = Handle %v, %v, want no Handle and context.Canceled", h != nil, err)
	}
	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	// Recover retried the spawn, and with a live context its onError applied.
	if got := recovered.Instance().Current(); got != "failed" {
		t.Fatalf("recovered state = %q, want failed", got)
	}
}

// TestRecover_FailsWhenItsContextEndsAfterReplay proves Recover returns no Handle when
// its context ends after the journal is replayed, here while it spawns the resume
// point's actor, though no parent Fire was refused.
func TestRecover_FailsWhenItsContextEndsAfterReplay(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[stepCtx]("late").
		Actor("quiet").
		State("working").InvokeActor("quiet").
		Initial("working").
		Transition("working").On("poke").GoTo("working").
		Quench()
	quiet := func(hook func()) []stepOption {
		return []stepOption{durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{
			"quiet": func(map[string]any) (state.ActorInstance, error) {
				if hook != nil {
					hook()
				}
				return &sender{sent: true}, nil
			},
		})}
	}
	store, id := durable.NewMemStore(), durable.InstanceID("late")
	h, err := durable.NewRunner(m, store, quiet(nil)...).Start(ctx, id, stepCtx{}, state.WithInitialState("working"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	recovered, err := durable.Recover(turn, m, store, id, quiet(cancel)...)
	if recovered != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Recover = Handle %v, %v, want no Handle and context.Canceled", recovered != nil, err)
	}
}
