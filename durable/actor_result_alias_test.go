package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	alpha "github.com/stablekernel/crucible/durable/internal/typefixture/alpha/model"
	"github.com/stablekernel/crucible/state"
)

// These tests answer the actor-side form of the rename concern WithResultTypeAlias
// answers for service results: an actor's done-data is held to the Go type the live
// settle recorded, so a pure refactor that moves or renames the type fails Recover
// and StateAt for every journal already settled under the old name — and the alias
// admits the new type on the actor paths too, with no new API.

// actorAlias is the option mapping a done-data type recorded before a rename, as
// rewriteActorTypes stands in for one, to alpha.Result.
func actorAlias() durable.Option[string, string, outcomeCtx] {
	return durable.WithResultTypeAlias[string, string, outcomeCtx](movedFromType, alpha.Result{})
}

// actorAssigns counts the parent's onDone Assigns and what each saw, so a test can
// tell whether a replay ran one and with which value.
type actorAssigns struct {
	done int
	seen []string
}

// aliasCollectorMachine is collectorMachine whose Assigns report to a, so a test
// reads them even when Recover fails.
func aliasCollectorMachine(a *actorAssigns) *state.Machine[string, string, outcomeCtx] {
	return state.ForgeFor[outcomeCtx]("collector").
		Reducer("recordDone", func(in state.AssignCtx[outcomeCtx]) outcomeCtx {
			a.done++
			a.seen = append(a.seen, fmt.Sprintf("%T:%v", in.Event, in.Event))
			return in.Entity
		}).
		Actor("kid").
		State("supervising").InvokeActor("kid", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childFail")).
		State("complete").Final().
		State("failed").Final().
		Initial("supervising").
		Transition("supervising").On("childDone").GoTo("complete").Assign("recordDone").
		Transition("supervising").On("childFail").GoTo("failed").
		Quench()
}

// aliasDecoder decodes the parent's childDone done-data as alpha.Result and every
// other event's payload as JSON's own form.
func aliasDecoder() durable.Option[string, string, outcomeCtx] {
	return childDoneDecoder(durable.DecodeAs[alpha.Result])
}

// settledActorDone settles an alpha.Result done-data for alias-1 live, under a
// decoder for it, and returns what Recover and StateAt need.
func settledActorDone(t *testing.T, a *actorAssigns) (*state.Machine[string, string, outcomeCtx], *int64, *durable.MemStore, durable.InstanceID) {
	t.Helper()
	var runs int64
	behavior := outputChild(&runs, alpha.Result{ID: 7, Name: "ann"})
	m := aliasCollectorMachine(a)
	store := durable.NewMemStore()
	id := durable.InstanceID("alias-1")
	h, err := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"kid": behavior}),
		aliasDecoder()).Start(context.Background(), id, outcomeCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("collector", "supervising", 0))
	if !ok {
		t.Fatalf("no actor ref for the spawned child")
	}
	if _, derr := h.DeliverToActor(context.Background(), ref, "finish"); derr != nil {
		t.Fatalf("DeliverToActor: %v", derr)
	}
	return m, &runs, store, id
}

// rewriteActorTypes copies id's journal into a fresh MemStore with every recorded
// actor entry's done-data Type from replaced by to, as if the type had been moved
// after the settle that recorded it.
func rewriteActorTypes(t *testing.T, src *durable.MemStore, id durable.InstanceID, from, to string) *durable.MemStore {
	t.Helper()
	ctx := context.Background()
	snap, recs, err := src.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	dst := durable.NewMemStore()
	if err = dst.Checkpoint(ctx, id, snap, -1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	rewrote := 0
	for _, rec := range recs {
		for i, e := range rec.Entries {
			if e.Kind != state.JournalActorMessage {
				continue
			}
			var p map[string]any
			if err = json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatalf("decoding an actor entry: %v", err)
			}
			if p["type"] == from {
				p["type"] = to
				rewrote++
			}
			if rec.Entries[i].Payload, err = json.Marshal(p); err != nil {
				t.Fatalf("encoding an actor entry: %v", err)
			}
		}
		if _, err = dst.Append(ctx, id, rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if rewrote == 0 {
		t.Fatalf("no actor entry recorded the type %s", from)
	}
	return dst
}

// TestActorResultTypeAlias_RenamedDoneData: an actor's done-data recorded under the
// type's old name fails Recover and StateAt with ErrEventDataEncoding under a decoder
// for the type's new name, naming the recorded type and the done-data; the
// WithResultTypeAlias that admits a renamed service result admits the new type on
// both paths, and the value onDone's Assign sees is the one the actor produced.
func TestActorResultTypeAlias_RenamedDoneData(t *testing.T) {
	ctx := context.Background()
	a := &actorAssigns{}
	m, runs, store, id := settledActorDone(t, a)
	renamed := rewriteActorTypes(t, store, id, alphaResultType, movedFromType)
	steps, stepsErr := durable.Steps(ctx, renamed, id)
	if stepsErr != nil || len(steps) != 1 {
		t.Fatalf("Steps = %v, %v; want the settle", steps, stepsErr)
	}
	palette := func() durable.Option[string, string, outcomeCtx] {
		var r int64
		return durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"kid": outputChild(&r, alpha.Result{ID: 7, Name: "ann"})})
	}
	for _, what := range []struct {
		name string
		call func(opts ...durable.Option[string, string, outcomeCtx]) error
	}{
		{"Recover", func(opts ...durable.Option[string, string, outcomeCtx]) error {
			_, err := durable.Recover(ctx, aliasCollectorMachine(a), renamed, id, opts...)
			return err
		}},
		{"StateAt", func(opts ...durable.Option[string, string, outcomeCtx]) error {
			_, err := durable.StateAt(ctx, aliasCollectorMachine(a), renamed, id, steps[0], opts...)
			return err
		}},
	} {
		err := what.call(palette(), aliasDecoder())
		if !errors.Is(err, durable.ErrEventDataEncoding) {
			t.Fatalf("%s without the alias: %v, want ErrEventDataEncoding", what.name, err)
		}
		if !strings.Contains(err.Error(), movedFromType) {
			t.Fatalf("%s without the alias: %v, want it to name the recorded type %s", what.name, err, movedFromType)
		}
		if !strings.Contains(err.Error(), "actor done-data") {
			t.Fatalf("%s without the alias: %v, want it to name the actor's done-data", what.name, err)
		}
	}
	if a.done != 1 {
		t.Fatalf("a failed replay ran an Assign: onDone %d times", a.done)
	}

	h, err := durable.Recover(ctx, m, renamed, id, palette(), aliasDecoder(), actorAlias())
	if err != nil {
		t.Fatalf("Recover with the alias: %v", err)
	}
	if got := h.Instance().Current(); got != "complete" {
		t.Fatalf("Recover with the alias: in %q, want complete", got)
	}
	if a.done != 2 || a.seen[1] != a.seen[0] {
		t.Fatalf("replay with the alias ran onDone %d times and saw %v, want the live run's %v", a.done, a.seen, a.seen[:1])
	}
	if got := atomic.LoadInt64(runs); got != 1 {
		t.Fatalf("the actor's behavior ran %d times, want once", got)
	}
	if _, err := durable.StateAt(ctx, m, renamed, id, steps[0], palette(), aliasDecoder(), actorAlias()); err != nil {
		t.Fatalf("StateAt with the alias: %v", err)
	}
}

// threeSettlesActorMachine settles its child three times: each state invokes the
// child, and each onDone enters the next state, which invokes it again, so a
// checkpoint after the second settle leaves the third one in the tail a Recover from
// that checkpoint replays.
func threeSettlesActorMachine(a *actorAssigns) *state.Machine[string, string, outcomeCtx] {
	return state.ForgeFor[outcomeCtx]("collector").
		Reducer("recordDone", func(in state.AssignCtx[outcomeCtx]) outcomeCtx {
			a.done++
			a.seen = append(a.seen, fmt.Sprintf("%T:%v", in.Event, in.Event))
			return in.Entity
		}).
		Actor("kid").
		State("first").InvokeActor("kid", state.WithInvokeOnDone("childDone1"), state.WithInvokeOnError("childFail")).
		State("second").InvokeActor("kid", state.WithInvokeOnDone("childDone2"), state.WithInvokeOnError("childFail")).
		State("third").InvokeActor("kid", state.WithInvokeOnDone("childDone3"), state.WithInvokeOnError("childFail")).
		State("ready").Final().
		State("failed").Final().
		Initial("first").
		Transition("first").On("childDone1").GoTo("second").Assign("recordDone").
		Transition("first").On("childFail").GoTo("failed").
		Transition("second").On("childDone2").GoTo("third").Assign("recordDone").
		Transition("second").On("childFail").GoTo("failed").
		Transition("third").On("childDone3").GoTo("ready").Assign("recordDone").
		Transition("third").On("childFail").GoTo("failed").
		Quench()
}

// threeSettlesDecoder decodes each childDone done-data as alpha.Result and every
// other event's payload as JSON's own form.
func threeSettlesDecoder() durable.Option[string, string, outcomeCtx] {
	return durable.WithEventDataDecoder[string, string, outcomeCtx](func(e string, data []byte) (any, error) {
		switch e {
		case "childDone1", "childDone2", "childDone3":
			return durable.DecodeAs[alpha.Result](data)
		}
		return durable.DecodeAs[any](data)
	})
}

// TestActorResultTypeAlias_RenamedDoneDataThroughCheckpoint: a checkpoint that
// compacts the earlier settles leaves the last one in the tail, and a Recover from it
// replays that settle's recorded done-data type: the old name fails it, and the
// WithResultTypeAlias settles it, with the value the actor produced.
func TestActorResultTypeAlias_RenamedDoneDataThroughCheckpoint(t *testing.T) {
	ctx := context.Background()
	var runs int64
	a := &actorAssigns{}
	m := threeSettlesActorMachine(a)
	behavior := outputChild(&runs, alpha.Result{ID: 7, Name: "ann"})
	store := durable.NewMemStore()
	id := durable.InstanceID("alias-1")
	// A checkpoint lands after the second settle (steps 0 and 2 of three cross no
	// boundary; step 1 crosses 2), compacting the first two and leaving the third.
	every := durable.WithCheckpointEvery[string, string, outcomeCtx](2)
	h, err := durable.NewRunner(m, store,
		durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"kid": behavior}),
		threeSettlesDecoder(), every).Start(ctx, id, outcomeCtx{}, state.WithInitialState("first"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, st := range []string{"first", "second", "third"} {
		ref, ok := h.ActorRef(state.ActorID("collector", st, 0))
		if !ok {
			t.Fatalf("no actor ref for the child in %q", st)
		}
		if _, derr := h.DeliverToActor(ctx, ref, "finish"); derr != nil {
			t.Fatalf("DeliverToActor in %q: %v", st, derr)
		}
	}
	// The first two settles checkpointed; the tail holds the third one.
	_, recs, lerr := store.Load(ctx, id)
	if lerr != nil || len(recs) != 1 || recs[0].Step != 2 {
		t.Fatalf("the journal holds %v (err %v), want only the third settle at step 2", recs, lerr)
	}
	if a.done != 3 {
		t.Fatalf("the live run settled %d times, want 3", a.done)
	}
	renamed := rewriteActorTypes(t, store, id, alphaResultType, movedFromType)
	palette := durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"kid": behavior})
	if _, rerr := durable.Recover(ctx, m, renamed, id, palette, threeSettlesDecoder(), every); !errors.Is(rerr, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover from the checkpoint without the alias: %v, want ErrEventDataEncoding", rerr)
	} else if !strings.Contains(rerr.Error(), movedFromType) || !strings.Contains(rerr.Error(), "actor done-data") {
		t.Fatalf("Recover from the checkpoint without the alias: %v, want it to name the recorded type %s and the actor's done-data", rerr, movedFromType)
	}
	if a.done != 3 {
		t.Fatalf("a failed replay ran an Assign: onDone %d times", a.done)
	}
	if got := atomic.LoadInt64(&runs); got != 3 {
		t.Fatalf("the actor's behavior ran %d times, want 3", got)
	}
	h, err = durable.Recover(ctx, m, renamed, id, palette, threeSettlesDecoder(), actorAlias(), every)
	if err != nil {
		t.Fatalf("Recover from the checkpoint with the alias: %v", err)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("in %q, want ready", got)
	}
	if a.done != 4 || a.seen[3] != a.seen[2] {
		t.Fatalf("replay with the alias ran onDone %d times and saw %v, want the third settle's %v", a.done, a.seen, a.seen[2:3])
	}
}
