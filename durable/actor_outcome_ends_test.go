package durable_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests hold the symmetry table true across the ends that journal an actor's
// settle: whichever step drove it — a DeliverToActor delivery (covered by
// TestActor_DoneData_SameTypeLiveAndAfterRecover), a Fire, a Tick, or a RunService —
// the parent's Assign must see the same done-data live and after Recover, and a
// StateAt at the settle's step must show it. The Fire, Tick and RunService ends drive
// the settle from the step's own absorb: the step enters a state that spawns a "poker"
// actor, which sends the running "kid" actor its finishing event as it starts, so kid
// settles while the step records. Each row is a fresh instance driven by one step
// kind, all through one machine whose "work" region offers the three drivings.

// endsMachine is one machine for the three settle ends. The keeper region holds kid
// from Start; the work region starts in W, which invokes fetch and arms an after
// timer, and each driving leaves W for X, whose entry spawns the poker that finishes
// kid: a Fire of "go", the timer's "elapsed", or fetch's "served" onDone.
func endsMachine(fn state.ServiceFn[outcomeCtx]) *state.Machine[string, string, outcomeCtx] {
	return state.ForgeFor[outcomeCtx]("ends").
		Service("fetch", fn).
		Reducer("recordDone", func(in state.AssignCtx[outcomeCtx]) outcomeCtx {
			c := in.Entity
			c.Notes = append(c.Notes, fmt.Sprintf("%T:%v", in.Event, in.Event))
			return c
		}).
		Actor("kid").Actor("poker").
		SuperState("both").
		Region("keeper").Initial("K").
		SubState("K").InvokeActor("kid", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childFail")).
		SubState("Kdone").Final().
		EndRegion().
		Region("work").Initial("W").
		SubState("W").Invoke("fetch", state.WithInvokeOnDone("served"), state.WithInvokeOnError("servfail")).
		SubState("X").InvokeActor("poker").
		EndRegion().
		EndSuperState().
		Initial("both").
		Transition("W").On("go").GoTo("X").
		Transition("W").After(10 * time.Second).On("elapsed").GoTo("X").
		Transition("W").On("served").GoTo("X").
		Transition("K").On("childDone").GoTo("Kdone").Assign("recordDone").
		Quench()
}

// endsIDs are the stable ids the ends machine's kid invocation and fetch invocation
// take, which the poker addresses kid by and RunService names fetch by.
var (
	endsKid  = state.ActorID("ends", "K", 0)
	endsEcho = state.InvokeID("ends", "W", 0)
)

// poker is an actor that sends the running kid its finishing event as it starts, so
// kid settles inside the step whose entry spawned the poker.
type poker struct {
	target string
	sent   bool
}

func (*poker) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (p *poker) ChildEffects() []state.Effect {
	if p.sent {
		return nil
	}
	p.sent = true
	return []state.Effect{state.SendTo{TargetID: p.target, Event: "finish"}}
}
func (*poker) Output() any { return nil }

// endsOpts builds the options every ends run takes: the palette holding kid and the
// poker, the registry serving fetch, and the decoder that hands kid's childDone
// done-data back as the order the actor produced.
func endsOpts(runs *int64) []durable.Option[string, string, outcomeCtx] {
	fn := func(context.Context, state.ServiceCtx[outcomeCtx]) (any, error) { return "ok", nil }
	return []durable.Option[string, string, outcomeCtx]{
		durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{
			"kid":   outputChild(runs, order{ID: 7}),
			"poker": func(map[string]any) (state.ActorInstance, error) { return &poker{target: endsKid}, nil },
		}),
		durable.WithServiceRegistry[string, string, outcomeCtx](state.NewRegistry[outcomeCtx]().Service("fetch", fn)),
		endsDecoder(),
	}
}

// endsStart starts a fresh ends instance on its own history Store and returns the
// Handle, the store, the id and the count of kid's instantiations. extra opts come
// after the base ones, so one can override the decoder.
func endsStart(t *testing.T, name string, clock state.Clock, extra ...durable.Option[string, string, outcomeCtx]) (*durable.Handle[string, string, outcomeCtx], durable.Store, durable.InstanceID, *int64) {
	t.Helper()
	ctx := context.Background()
	var runs int64
	opts := append(endsOpts(&runs), extra...)
	if clock != nil {
		opts = append(opts, durable.WithRunnerClock[string, string, outcomeCtx](clock))
	}
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID(name)
	fn := func(context.Context, state.ServiceCtx[outcomeCtx]) (any, error) { return "ok", nil }
	h, err := durable.NewRunner(endsMachine(fn), store, opts...).Start(ctx, id, outcomeCtx{}, state.WithInitialState("both"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return h, store, id, &runs
}

// endsDecoder decodes kid's childDone done-data as the order the actor produced, and
// every other event's payload in JSON's generic form.
func endsDecoder() durable.Option[string, string, outcomeCtx] {
	return durable.WithEventDataDecoder[string, string, outcomeCtx](func(e string, data []byte) (any, error) {
		if e == "childDone" {
			return durable.DecodeAs[order](data)
		}
		return durable.DecodeAs[any](data)
	})
}

// wantEndsNote is what kid's onDone Assign records, live and on every replay.
const wantEndsNote = "durable_test.order:{7}"

// checkEndsRecovered recovers the instance and asserts the replay delivered kid's
// done-data exactly as the live run did, without running the actor again, and that a
// StateAt at the settle's step shows the same.
func checkEndsRecovered(t *testing.T, h *durable.Handle[string, string, outcomeCtx], store durable.Store, id durable.InstanceID, runs *int64) {
	t.Helper()
	ctx := context.Background()
	fn := func(context.Context, state.ServiceCtx[outcomeCtx]) (any, error) { return "ok", nil }
	live := h.Instance().Entity().Notes
	if len(live) != 1 || live[0] != wantEndsNote {
		t.Fatalf("live notes = %q, want kid's %q", live, wantEndsNote)
	}
	rh, err := durable.Recover(ctx, endsMachine(fn), store, id, endsOpts(runs)...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	rec := rh.Instance().Entity().Notes
	if len(rec) != 1 || rec[0] != live[0] {
		t.Fatalf("after Recover the notes = %q, want the live run's %q", rec, live)
	}
	if got := atomic.LoadInt64(runs); got != 1 {
		t.Fatalf("kid instantiated %d times across the live run and Recover, want 1", got)
	}
	v, err := durable.StateAt(ctx, endsMachine(fn), store, id, 0, endsOpts(runs)...)
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if got := v.Instance().Entity().Notes; len(got) != 1 || got[0] != live[0] {
		t.Fatalf("StateAt(0) notes = %q, want the live run's %q", got, live)
	}
}

// TestActorOutcome_AFireSpawnedSettle_ReplaysAsItDelivered pins the Fire end: a Fire
// whose transition's entry spawns an actor that finishes a running one journals the
// settle with the step, and Recover and StateAt re-deliver its done-data decoded, as
// the live Fire's absorb delivered it.
func TestActorOutcome_AFireSpawnedSettle_ReplaysAsItDelivered(t *testing.T) {
	ctx := context.Background()
	h, store, id, runs := endsStart(t, "ends-fire", nil)
	if _, err := h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire go: %v", err)
	}
	checkEndsRecovered(t, h, store, id, runs)
}

// TestActorOutcome_ATickSpawnedSettle_ReplaysAsItDelivered pins the Tick end: a timer
// whose transition's entry spawns an actor that finishes a running one journals the
// settle with the tick's Record, and Recover and StateAt re-deliver its done-data
// decoded, as the live tick's absorb delivered it.
func TestActorOutcome_ATickSpawnedSettle_ReplaysAsItDelivered(t *testing.T) {
	ctx := context.Background()
	clock := state.NewFakeClock(time.Unix(0, 0).UTC())
	h, store, id, runs := endsStart(t, "ends-tick", clock)
	clock.Advance(11 * time.Second)
	if _, err := h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	checkEndsRecovered(t, h, store, id, runs)
}

// TestActorOutcome_ARunServiceSpawnedSettle_ReplaysAsItDelivered pins the RunService
// end: a service settle whose transition's entry spawns an actor that finishes a
// running one journals the settle with the service step, and Recover and StateAt
// re-deliver its done-data decoded, as the live settle's absorb delivered it.
func TestActorOutcome_ARunServiceSpawnedSettle_ReplaysAsItDelivered(t *testing.T) {
	ctx := context.Background()
	h, store, id, runs := endsStart(t, "ends-svc", nil)
	if _, ran, err := h.RunService(ctx, endsEcho); err != nil || !ran {
		t.Fatalf("RunService = %v, %v; want the settle ran", err, ran)
	}
	checkEndsRecovered(t, h, store, id, runs)
}

// TestActorOutcome_AnUndecodableSettle_FailsEachDrivingStep pins the no-parking rule
// on the Fire, Tick and RunService ends: when the settle their absorb drives can't
// decode its done-data, the settle aborts, the driving step fails the Handle with
// ErrHandleFailed wrapping ErrEventDataEncoding, and nothing is recorded, so a
// Recover runs the actor again. The DeliverToActor end's is
// TestActor_DoneDataThatWontDecode_IsNeverParked.
func TestActorOutcome_AnUndecodableSettle_FailsEachDrivingStep(t *testing.T) {
	drive := []struct {
		name  string
		clock bool
		step  func(*testing.T, context.Context, *durable.Handle[string, string, outcomeCtx])
	}{
		{"Fire", false, func(t *testing.T, ctx context.Context, h *durable.Handle[string, string, outcomeCtx]) {
			if _, err := h.Fire(ctx, "go"); !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, durable.ErrEventDataEncoding) {
				t.Fatalf("Fire go = %v, want ErrHandleFailed wrapping ErrEventDataEncoding", err)
			}
		}},
		{"Tick", true, func(t *testing.T, ctx context.Context, h *durable.Handle[string, string, outcomeCtx]) {
			if _, err := h.Tick(ctx); !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, durable.ErrEventDataEncoding) {
				t.Fatalf("Tick = %v, want ErrHandleFailed wrapping ErrEventDataEncoding", err)
			}
		}},
		{"RunService", false, func(t *testing.T, ctx context.Context, h *durable.Handle[string, string, outcomeCtx]) {
			_, ran, err := h.RunService(ctx, endsEcho)
			if !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, durable.ErrEventDataEncoding) {
				t.Fatalf("RunService = %v, %v; want ErrHandleFailed wrapping ErrEventDataEncoding", err, ran)
			}
		}},
	}
	for _, d := range drive {
		t.Run(d.name, func(t *testing.T) {
			ctx := context.Background()
			var fake *state.FakeClock
			if d.clock {
				fake = state.NewFakeClock(time.Unix(0, 0).UTC())
			}
			clock := state.Clock(nil)
			if fake != nil {
				clock = fake
			}
			// A decoder that refuses kid's done-data, in place of the one endsOpts wires.
			refusing := childDoneDecoder(func([]byte) (any, error) { return nil, errors.New("decoder refused") })
			h, store, id, runs := endsStart(t, "ends-abort-"+d.name, clock, refusing)
			if fake != nil {
				fake.Advance(11 * time.Second)
			}
			d.step(t, ctx, h)
			if notes := h.Instance().Entity().Notes; len(notes) != 0 {
				t.Fatalf("the aborted settle ran Assigns: %q", notes)
			}
			if _, recs, err := store.Load(ctx, id); err != nil || len(recs) != 0 {
				t.Fatalf("the journal holds %d Records (err %v), want none: nothing was recorded", len(recs), err)
			}
			// The actor runs again on Recover, from its initial state.
			fn := func(context.Context, state.ServiceCtx[outcomeCtx]) (any, error) { return "ok", nil }
			rh, err := durable.Recover(ctx, endsMachine(fn), store, id, endsOpts(runs)...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if got := atomic.LoadInt64(runs); got != 2 {
				t.Fatalf("kid instantiated %d times, want 2: Recover ran it again", got)
			}
			if _, ok := rh.ActorRef(endsKid); !ok {
				t.Fatal("the recovered Handle doesn't run kid, want it re-run")
			}
		})
	}
}

// startSettleMachine is a machine whose Start settles an actor: the kid region's actor
// spawns first, then the finisher region's, whose start sends kid its finishing event,
// so kid settles while Start spawns. The settle's Fire runs before any step, so the
// first step journals it.
func startSettleMachine() *state.Machine[string, string, outcomeCtx] {
	return state.ForgeFor[outcomeCtx]("startsettle").
		Reducer("recordDone", func(in state.AssignCtx[outcomeCtx]) outcomeCtx {
			c := in.Entity
			c.Notes = append(c.Notes, fmt.Sprintf("%T:%v", in.Event, in.Event))
			return c
		}).
		Actor("kid").Actor("finisher").
		SuperState("both").
		Region("kidreg").Initial("K").
		SubState("K").InvokeActor("kid", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childFail")).
		SubState("Kdone").Final().
		EndRegion().
		Region("finreg").Initial("F").
		SubState("F").InvokeActor("finisher").
		SubState("F2").
		EndRegion().
		EndSuperState().
		Initial("both").
		Transition("K").On("childDone").GoTo("Kdone").Assign("recordDone").
		Transition("F").On("settle").GoTo("F2").
		Quench()
}

// TestActorOutcome_ASettleDrivenAtStart_ReplaysAsItDelivered pins the Start end: a
// settle an initial spawn drives runs before any step, the first step journals it
// ahead of its own work, and Recover and StateAt re-deliver its done-data decoded, as
// the live start delivered it. With a decoder that can't decode the done-data, Start
// fails with the abort, and so does a Recover from the baseline, whose respawns drive
// the same settle; neither records anything.
func TestActorOutcome_ASettleDrivenAtStart_ReplaysAsItDelivered(t *testing.T) {
	ctx := context.Background()
	kid := state.ActorID("startsettle", "K", 0)
	palette := func(runs *int64) durable.Option[string, string, outcomeCtx] {
		return durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{
			"kid": outputChild(runs, order{ID: 7}),
			"finisher": func(map[string]any) (state.ActorInstance, error) {
				return &poker{target: kid}, nil
			},
		})
	}
	var runs int64
	store := durable.NewMemStore(durable.WithHistory())
	id := durable.InstanceID("startsettle-1")
	h, err := durable.NewRunner(startSettleMachine(), store, palette(&runs), endsDecoder()).
		Start(ctx, id, outcomeCtx{}, state.WithInitialState("both"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	live := h.Instance().Entity().Notes
	if len(live) != 1 || live[0] != wantEndsNote {
		t.Fatalf("live notes = %q, want kid's %q", live, wantEndsNote)
	}
	// A step journals the start's settle, ahead of its own work.
	if _, err = h.Fire(ctx, "settle"); err != nil {
		t.Fatalf("Fire settle: %v", err)
	}
	rh, err := durable.Recover(ctx, startSettleMachine(), store, id, palette(&runs), endsDecoder())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec := rh.Instance().Entity().Notes; len(rec) != 1 || rec[0] != live[0] {
		t.Fatalf("after Recover the notes = %q, want the live run's %q", rec, live)
	}
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("kid instantiated %d times, want 1", got)
	}
	if _, err = durable.StateAt(ctx, startSettleMachine(), store, id, 0, palette(&runs), endsDecoder()); err != nil {
		t.Fatalf("StateAt: %v", err)
	}

	// A settle Start drove runs before any step, so a journal with no Record yet —
	// the baseline — holds none of it: a Recover from there re-runs the spawns, whose
	// settle a decoder that can't decode the done-data fails, before it returns a
	// Handle.
	base := durable.NewMemStore()
	var baseRuns int64
	if _, err = durable.NewRunner(startSettleMachine(), base, palette(&baseRuns), endsDecoder()).
		Start(ctx, "startsettle-3", outcomeCtx{}, state.WithInitialState("both")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// A decoder that can't decode kid's done-data fails Start, whose spawns drive the
	// settle, recording nothing. No Handle exists yet, so the error wraps
	// ErrEventDataEncoding directly, as an undecodable WithEventData payload's does.
	failing := childDoneDecoder(func([]byte) (any, error) { return nil, errors.New("decoder refused") })
	var fresh int64
	fstore := durable.NewMemStore()
	if _, err = durable.NewRunner(startSettleMachine(), fstore, palette(&fresh), failing).
		Start(ctx, "startsettle-2", outcomeCtx{}, state.WithInitialState("both")); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Start with a failing decoder = %v, want the undecodable done-data's error", err)
	}
	if _, recs, lerr := fstore.Load(ctx, "startsettle-2"); lerr != nil || len(recs) != 0 {
		t.Fatalf("the journal holds %d Records (err %v), want none", len(recs), lerr)
	}
	// ... and a Recover from the baseline, whose respawns drive the same settle.
	if _, err = durable.Recover(ctx, startSettleMachine(), base, "startsettle-3", palette(&baseRuns), failing); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover from the baseline with a failing decoder = %v, want the undecodable done-data's error", err)
	}
	// The journaled settle fails a replay whose decoder can't decode it, as the live
	// settle refused it, before any Assign runs again.
	if _, err = durable.Recover(ctx, startSettleMachine(), store, id, palette(&runs), failing); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover with a failing decoder = %v, want the undecodable done-data's error", err)
	}
}
