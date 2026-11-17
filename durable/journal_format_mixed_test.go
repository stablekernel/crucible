package durable_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests answer a review finding on a rolling deploy that runs two versions on one
// instance: an older replica appends a Record of its own, lower journal format after
// this version's checkpoint. Recover must refuse the journal, and say that mixed
// versions wrote it rather than blame the Store.

// TestRecover_OlderVersionAppendedAfterThisVersionsCheckpoint: this version's Start
// writes a baseline in its own format, and an older replica, which stamps format 1, appends the
// next step. Recover refuses, naming the older version, not a Store that lost
// Record.Format.
func TestRecover_OlderVersionAppendedAfterThisVersionsCheckpoint(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	if _, err := durable.NewRunner(effectMachine(), st).Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// The older replica: a Runner of this version, whose Store stamps format 1 on every
	// Record it appends, as that version's Runner did.
	if _, err := durable.NewRunner(effectMachine(), &olderWriter{MemStore: st, format: 1}).Fire(ctx, "i1", "go"); err != nil {
		t.Fatalf("the older replica's Fire: %v", err)
	}
	_, err := durable.Recover(ctx, effectMachine(), st, "i1")
	if err == nil {
		t.Fatal("Recover replayed a format-1 Record after a checkpoint of this version's format")
	}
	if msg := err.Error(); !strings.Contains(msg, "older version") || strings.Contains(msg, "didn't keep Record.Format") {
		t.Fatalf("Recover: %v, want it to say an older version appended to this version's journal, not blame the Store", err)
	}
}

// TestRecover_FormatlessRecordAfterThisVersionsCheckpoint keeps the diagnosis for a
// Record with no format at all, which a Store that drops the field gives, as does a
// version from before the field: Recover refuses and names both.
func TestRecover_FormatlessRecordAfterThisVersionsCheckpoint(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	if _, err := durable.NewRunner(effectMachine(), st).Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := durable.NewRunner(effectMachine(), formatDroppingStore{st}).Fire(ctx, "i1", "go"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	_, err := durable.Recover(ctx, effectMachine(), st, "i1")
	if err == nil || !strings.Contains(err.Error(), "didn't keep Record.Format") {
		t.Fatalf("Recover: %v, want it to say the Store may not have kept Record.Format", err)
	}
}

// forkMachine invokes fetch in loading. "next" is declared in loading and in ready,
// with different targets, so a journal replayed from the wrong state still replays,
// to the wrong place.
func forkMachine(fn state.ServiceFn[svcCtx]) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("fork").
		Service("fetch", fn).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").
		State("canceled").Final().
		State("shipped").Final().
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").
		Transition("loading").On("fail").GoTo("failed").
		Transition("loading").On("next").GoTo("canceled").
		Transition("ready").On("next").GoTo("shipped").
		Quench()
}

// TestRecover_OlderVersionAfterAParkIsRefused shows why Recover can't replay an older
// replica's Record by its own format's rules. This version parks a result; the older
// replica, which has no parking, reads the parked entry as a settle, so it moves to
// ready, and its "next" ships the order. Replayed here, the park leaves the instance in
// loading, where "next" cancels it: the two versions disagree about what the journal
// means, so Recover must refuse rather than recover canceled.
func TestRecover_OlderVersionAfterAParkIsRefused(t *testing.T) {
	ctx := context.Background()
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return scnUser{ID: 7, Name: "ann"}, nil }
	m := forkMachine(fn)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	st := durable.NewMemStore()
	h, err := durable.NewRunner(m, st, reg, failingDecoder()).Start(ctx, "o1", svcCtx{}, state.WithInitialState("loading"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, ran, err := h.RunService(ctx, state.InvokeID("fork", "loading", 0))
	mustPark(t, "RunService", ran, err)
	// The older replica's step: the "next" it fired from ready, in its format.
	if _, err = st.Append(ctx, "o1", durable.Record{Step: 1, Event: []byte(`"next"`), Format: 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	rh, err := durable.Recover(ctx, m, st, "o1", reg, failingDecoder())
	if err == nil {
		t.Fatalf("Recover replayed the older replica's step into %q; the older replica left the order shipped", rh.Instance().Current())
	}
}
