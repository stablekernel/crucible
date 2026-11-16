package durable_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// olderCheckpoint returns the store's latest checkpoint rewritten in the shape an
// older version wrote, with no journal format and no actor or service sets, in a new
// store, at the same step.
func olderCheckpoint(t *testing.T, store *durable.MemStore, id durable.InstanceID) *durable.MemStore {
	t.Helper()
	ctx := context.Background()
	snap, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 0 {
		t.Fatalf("tail holds %d Records, want the checkpoint to cover every step", len(tail))
	}
	step, ok, err := store.LastStep(ctx, id)
	if err != nil || !ok {
		t.Fatalf("LastStep = %d, %v, %v", step, ok, err)
	}
	var env map[string]json.RawMessage
	if err = json.Unmarshal(snap, &env); err != nil {
		t.Fatalf("decoding the checkpoint: %v", err)
	}
	for _, k := range []string{"format", "endedServices", "endedActors", "unstartedActors"} {
		delete(env, k)
	}
	if snap, err = json.Marshal(env); err != nil {
		t.Fatalf("encoding the checkpoint: %v", err)
	}
	older := durable.NewMemStore()
	if err = older.Checkpoint(ctx, id, snap, step); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	return older
}

// TestRecover_FromAnOlderCheckpointDoesNotRerunACompoundStatesSettledService pins that
// Recover from a checkpoint an older version wrote starts what that version would have:
// the active leaves' services, not a compound state's, which the older version never
// started and which, here, already settled into a child. The service doesn't run again.
func TestRecover_FromAnOlderCheckpointDoesNotRerunACompoundStatesSettledService(t *testing.T) {
	ctx := context.Background()
	runs := 0
	load := func(context.Context, state.ServiceCtx[stepCtx]) (any, error) { runs++; return "ok", nil }
	m := state.ForgeFor[stepCtx]("olderCompound").
		Service("load", load).
		State("idle").
		SuperState("outer").Invoke("load", state.WithInvokeOnDone("loaded")).
		SubState("loading").
		SubState("ready").
		Initial("loading").
		EndSuperState().
		Initial("idle").
		Transition("idle").On("go").GoTo("outer").
		Transition("loading").On("loaded").GoTo("ready").
		Transition("ready").On("poke").GoTo("ready").
		Quench()
	opts := []stepOption{
		durable.WithServiceRegistry[string, string, stepCtx](state.NewRegistry[stepCtx]().Service("load", load)),
		durable.WithCheckpointEvery[string, string, stepCtx](1),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("older-compound")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	svcID := state.InvokeID("olderCompound", "outer", 0)
	if _, ran, runErr := h.RunService(ctx, svcID); !ran || runErr != nil {
		t.Fatalf("RunService = %v, %v", ran, runErr)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	older := olderCheckpoint(t, store, id)
	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, ran, runErr := recovered.RunService(ctx, svcID); ran || runErr != nil {
		t.Fatalf("RunService after Recover from an older checkpoint = ran %v, %v (runs %d), want the settled compound service left alone", ran, runErr, runs)
	}
	// A step this version records on top, and the checkpoint it lands, keep it settled.
	if _, err = recovered.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke) after Recover: %v", err)
	}
	again, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	if _, ran, runErr := again.RunService(ctx, svcID); ran || runErr != nil {
		t.Fatalf("RunService after a second Recover = ran %v, %v (runs %d), want the service still settled", ran, runErr, runs)
	}
}

// TestRecover_FromAnOlderCheckpointDoesNotRespawnACompoundStatesSettledActor pins the
// same for a compound state's actor that finished into a child: Recover from an older
// checkpoint, which records no ended actors, doesn't spawn it again.
func TestRecover_FromAnOlderCheckpointDoesNotRespawnACompoundStatesSettledActor(t *testing.T) {
	ctx := context.Background()
	m := state.ForgeFor[stepCtx]("olderCompoundActor").
		Actor("kid").
		State("idle").
		SuperState("outer").InvokeActor("kid", state.WithInvokeOnDone("kidDone"), state.WithInvokeID("kid")).
		SubState("loading").
		SubState("ready").
		Initial("loading").
		EndSuperState().
		Initial("idle").
		Transition("idle").On("go").GoTo("outer").
		Transition("loading").On("kidDone").GoTo("ready").
		Transition("ready").On("poke").GoTo("ready").
		Quench()
	opts := []stepOption{
		durable.WithActorPalette[string, string, stepCtx](map[string]state.ActorBehavior{"kid": finishingChild()}),
		durable.WithCheckpointEvery[string, string, stepCtx](1),
	}
	store, id := durable.NewMemStore(), durable.InstanceID("older-compound-actor")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, stepCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	if ok, deliverErr := h.DeliverToActorByID(ctx, "kid", "finish"); !ok || deliverErr != nil {
		t.Fatalf("DeliverToActorByID = %v, %v", ok, deliverErr)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("state = %q, want ready by the kid's onDone", got)
	}
	if _, err = h.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke): %v", err)
	}
	older := olderCheckpoint(t, store, id)
	recovered, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, running := recovered.ActorRef("kid"); running {
		t.Fatal("Recover from an older checkpoint spawned the compound state's kid again, which had finished")
	}
	// A step this version records on top, and the checkpoint it lands, keep it ended.
	if _, err = recovered.Fire(ctx, "poke"); err != nil {
		t.Fatalf("Fire(poke) after Recover: %v", err)
	}
	again, err := durable.Recover(ctx, m, older, id, opts...)
	if err != nil {
		t.Fatalf("second Recover: %v", err)
	}
	if _, running := again.ActorRef("kid"); running {
		t.Fatal("a second Recover spawned the finished kid again")
	}
}
