package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests pin the routing half of the settle transform's contract, which the
// review of #268 surfaced: the transform runs only when the settle routes a
// completion, so an actor spawned with neither an onDone nor an onError — a dynamic
// spawn routes nothing, where an invoked actor always routes, firing the zero event
// its unset outcome leaves — settles as the no-op it was before the transform
// existed. No decoder is consulted, so an output that won't encode can't fail the
// step; nothing is recorded for the settle, so Recover and StateAt replay the run
// with no done-data to hold a decoder to.

// quietMachine spawns its child through the spawn action with no onDone and no
// onError, so the child's settle routes nothing; "poke" stays in supervising, so a
// test can prove the Handle is healthy after the unrouted settle.
func quietMachine() *state.Machine[string, string, outcomeCtx] {
	return state.ForgeFor[outcomeCtx]("quiet").
		State("idle").
		State("supervising").
		Initial("idle").
		Transition("idle").On("start").GoTo("supervising").Spawn("kid", "quiet-kid").
		Transition("supervising").On("poke").GoTo("supervising").
		Quench()
}

// quietStart starts a quietMachine with the given child behavior and extra options,
// fires "start" to spawn the routing-less child, and returns the Handle and its
// Store and instance id.
func quietStart(t *testing.T, behavior state.ActorBehavior, opts ...durable.Option[string, string, outcomeCtx]) (
	*durable.Handle[string, string, outcomeCtx], *durable.MemStore, durable.InstanceID,
) {
	t.Helper()
	ctx := context.Background()
	m := quietMachine()
	all := append([]durable.Option[string, string, outcomeCtx]{
		durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"kid": behavior}),
	}, opts...)
	store, id := durable.NewMemStore(), durable.InstanceID("quiet-settle")
	h, err := durable.NewRunner(m, store, all...).Start(ctx, id, outcomeCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, ferr := h.Fire(ctx, "start"); ferr != nil {
		t.Fatalf("Fire(start): %v", ferr)
	}
	return h, store, id
}

// quietDeliver delivers "finish" to the routing-less child and asserts the delivery
// drove an unrouted settle that neither failed the Handle nor recorded an actor
// entry, and that the Handle stays healthy, Recover works, and StateAt works.
func quietDeliver(t *testing.T, h *durable.Handle[string, string, outcomeCtx], store *durable.MemStore, id durable.InstanceID) {
	t.Helper()
	ctx := context.Background()
	if delivered, derr := h.DeliverToActorByID(ctx, "quiet-kid", "finish"); derr != nil || !delivered {
		t.Fatalf("DeliverToActorByID = %v, %v; want the unrouted settle to drive a healthy step", delivered, derr)
	}
	if _, ferr := h.Fire(ctx, "poke"); ferr != nil {
		t.Fatalf("Fire after the unrouted settle: %v, want the Handle healthy", ferr)
	}
	// Nothing was recorded for the settle: no actor entry anywhere in the journal.
	_, recs, lerr := store.Load(ctx, id)
	if lerr != nil {
		t.Fatalf("Load: %v", lerr)
	}
	entries := 0
	for _, rec := range recs {
		for _, e := range rec.Entries {
			if e.Kind == state.JournalActorMessage {
				entries++
			}
		}
	}
	if entries != 0 {
		t.Fatalf("the journal holds %d actor entries, want none: the unrouted settle recorded nothing", entries)
	}
	// Recover and StateAt replay the run without the done-data.
	if _, rerr := durable.Recover(ctx, quietMachine(), store, id,
		durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"kid": outputChild(new(int64), nil)})); rerr != nil {
		t.Fatalf("Recover: %v", rerr)
	}
	steps, serr := durable.Steps(ctx, store, id)
	if serr != nil || len(steps) == 0 {
		t.Fatalf("Steps = %v, %v; want the recorded steps", steps, serr)
	}
	if _, aerr := durable.StateAt(ctx, quietMachine(), store, id, steps[len(steps)-1],
		durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"kid": outputChild(new(int64), nil)})); aerr != nil {
		t.Fatalf("StateAt: %v", aerr)
	}
}

// TestDeliverToActor_AnUnroutedSettleWithUnencodableOutputIsANoOp proves an actor
// whose completion routes nothing settles as a no-op even when its Output() won't
// encode — a channel — where the same output routed to an onDone would abort the
// settle and fail the Handle: the unrouted settle consults no transform, the
// delivery records nothing for it, the Handle stays healthy, and Recover and StateAt
// replay the run.
func TestDeliverToActor_AnUnroutedSettleWithUnencodableOutputIsANoOp(t *testing.T) {
	var runs int64
	h, store, id := quietStart(t, outputChild(&runs, make(chan int)))
	quietDeliver(t, h, store, id)
	// The child's behavior ran once, live; neither replay re-runs it, and a spawn
	// doesn't return on Recover.
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("the child's behavior ran %d times, want the live run's once", got)
	}
}

// TestDeliverToActor_AnUnroutedSettleConsultsNoDecoder proves the unrouted settle
// never reaches the decoder: an actor whose completion routes nothing, with an output
// that encodes cleanly, settles as a no-op under a decoder that refuses the zero
// event the settle would have handed it — the event an onDone-less spawn leaves.
func TestDeliverToActor_AnUnroutedSettleConsultsNoDecoder(t *testing.T) {
	refuses := errors.New("refuses the zero event")
	decoder := durable.WithEventDataDecoder[string, string, outcomeCtx](func(e string, data []byte) (any, error) {
		if e == "" {
			return nil, refuses
		}
		return durable.DecodeAs[any](data)
	})
	var runs int64
	h, store, id := quietStart(t, outputChild(&runs, "payload"), decoder)
	quietDeliver(t, h, store, id)
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("the child's behavior ran %d times, want the live run's once", got)
	}
}

// twinMachine's "waiting" state invokes two children, left and right, each routing
// its own onDone, and "kick" stays in waiting (an internal self-transition, so
// neither child is stopped or respawned) while sending each child "finish": one
// Fire's absorb finishes both, each settling with its own done-data.
func twinMachine(a *actorAssigns) *state.Machine[string, string, outcomeCtx] {
	return state.ForgeFor[outcomeCtx]("twins").
		Reducer("recordDone", func(in state.AssignCtx[outcomeCtx]) outcomeCtx {
			a.done++
			a.seen = append(a.seen, fmt.Sprintf("%T:%v", in.Event, in.Event))
			return in.Entity
		}).
		Actor("leftKid").
		Actor("rightKid").
		State("waiting").
		InvokeActor("leftKid", state.WithInvokeOnDone("leftDone")).
		InvokeActor("rightKid", state.WithInvokeOnDone("rightDone")).
		Initial("waiting").
		Transition("waiting").On("kick").GoTo("waiting").
		SendTo(state.ActorID("twins", "waiting", 0), "finish").
		SendTo(state.ActorID("twins", "waiting", 1), "finish").
		Transition("waiting").On("leftDone").GoTo("waiting").Assign("recordDone").
		Transition("waiting").On("rightDone").GoTo("waiting").Assign("recordDone").
		Quench()
}

// twinDecoder decodes the left child's done-data as an order and the right child's
// as a string, so each onDone's Assign sees the type its actor produced.
func twinDecoder() durable.Option[string, string, outcomeCtx] {
	return durable.WithEventDataDecoder[string, string, outcomeCtx](func(e string, data []byte) (any, error) {
		switch e {
		case "leftDone":
			return durable.DecodeAs[order](data)
		case "rightDone":
			return durable.DecodeAs[string](data)
		}
		return durable.DecodeAs[any](data)
	})
}

// twinSettles is one journal entry a settle drove, as the test reads it: the actor
// it settled, and the done-data and Go type it recorded.
type twinSettles struct {
	actor string
	data  string
	typ   string
}

// TestFire_TwoSettlesInOneStepEachDoneDataItsOwn proves one driving step that
// finishes two actors journals each settle's own done-data under its own actor, and
// delivers each onDone's Assign its own value — live, after Recover, and through
// StateAt — so a relay that swapped one settle's JSON into the other's slot, or
// dropped it, fails: each entry's Data and Type are asserted per actor, as each
// Assign's value is per event.
func TestFire_TwoSettlesInOneStepEachDoneDataItsOwn(t *testing.T) {
	ctx := context.Background()
	a := &actorAssigns{}
	var runs int64
	m := twinMachine(a)
	palette := durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{
		"leftKid":  outputChild(&runs, order{ID: 41}),
		"rightKid": outputChild(&runs, "right is done"),
	})
	store, id := durable.NewMemStore(), durable.InstanceID("twins-one-step")
	h, err := durable.NewRunner(m, store, palette, twinDecoder()).Start(ctx, id, outcomeCtx{}, state.WithInitialState("waiting"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, ferr := h.Fire(ctx, "kick"); ferr != nil {
		t.Fatalf("Fire(kick): %v", ferr)
	}

	// The live run: both settles ran in the one driving step, each Assign seeing its
	// own actor's done-data, in send order.
	wantLeft, wantRight := a.seen[0], a.seen[1]
	if a.done != 2 {
		t.Fatalf("the Fire settled %d actors, want 2", a.done)
	}
	if wantLeft != fmt.Sprintf("%T:%v", order{ID: 41}, order{ID: 41}) {
		t.Fatalf("the left Assign saw %q, want the left child's order", wantLeft)
	}
	if wantRight != "string:right is done" {
		t.Fatalf("the right Assign saw %q, want the right child's string", wantRight)
	}

	// The journal: one actor entry per settle, each under its own actor, carrying
	// that actor's done-data and its Go type.
	left, right := state.ActorID("twins", "waiting", 0), state.ActorID("twins", "waiting", 1)
	ord := reflect.TypeOf(order{})
	wantLeftType, wantRightType := ord.PkgPath()+"."+ord.Name(), "string"
	_, recs, lerr := store.Load(ctx, id)
	if lerr != nil {
		t.Fatalf("Load: %v", lerr)
	}
	var settles []twinSettles
	for _, rec := range recs {
		for _, e := range rec.Entries {
			if e.Kind != state.JournalActorMessage {
				continue
			}
			var mp struct {
				Data    json.RawMessage `json:"data"`
				Type    string          `json:"type"`
				HasData bool            `json:"hasData"`
			}
			if uerr := json.Unmarshal(e.Payload, &mp); uerr != nil {
				t.Fatalf("decoding the actor entry for %q: %v", e.CorrelationID, uerr)
			}
			if !mp.HasData {
				t.Fatalf("the settle entry for %q carries no done-data", e.CorrelationID)
			}
			settles = append(settles, twinSettles{actor: e.CorrelationID, data: string(mp.Data), typ: mp.Type})
		}
	}
	if len(settles) != 2 {
		t.Fatalf("the journal holds %d settle entries, want the two settles of the one step", len(settles))
	}
	for i, want := range []twinSettles{
		{left, `{"ID":41}`, wantLeftType},
		{right, `"right is done"`, wantRightType},
	} {
		if settles[i] != want {
			t.Fatalf("settle entry %d = %+v, want %+v: each settle journals its own done-data and type under its own actor", i, settles[i], want)
		}
	}

	// Recover: the replay re-fires each settle's onDone with the recorded done-data,
	// so each Assign sees the value the live run's did.
	h2, rerr := durable.Recover(ctx, m, store, id, palette, twinDecoder())
	if rerr != nil {
		t.Fatalf("Recover: %v", rerr)
	}
	if got := h2.Instance().Current(); got != "waiting" {
		t.Fatalf("after Recover in %q, want waiting", got)
	}
	if a.done != 4 || a.seen[2] != wantLeft || a.seen[3] != wantRight {
		t.Fatalf("replay settled %d times seeing %v, want each Assign its own live value [%q %q]", a.done, a.seen, wantLeft, wantRight)
	}

	// StateAt, at the step that settled them, replays the same values.
	steps, serr := durable.Steps(ctx, store, id)
	if serr != nil || len(steps) == 0 {
		t.Fatalf("Steps = %v, %v; want the recorded steps", steps, serr)
	}
	view, aerr := durable.StateAt(ctx, m, store, id, steps[len(steps)-1], palette, twinDecoder())
	if aerr != nil {
		t.Fatalf("StateAt: %v", aerr)
	}
	if got := view.Instance().Current(); got != "waiting" {
		t.Fatalf("StateAt in %q, want waiting", got)
	}
	if a.done != 6 || a.seen[4] != wantLeft || a.seen[5] != wantRight {
		t.Fatalf("StateAt settled %d times seeing %v, want each Assign its own live value [%q %q]", a.done, a.seen, wantLeft, wantRight)
	}

	// No replay re-ran the children: only the live run's behaviors ran.
	if got := atomic.LoadInt64(&runs); got != 2 {
		t.Fatalf("the children's behaviors ran %d times, want the live run's 2", got)
	}
}
