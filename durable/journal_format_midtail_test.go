package durable_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests answer a review finding on the journal format check: it compared each
// Record's format only with the checkpoint's, so an older version's Record appended
// after this version's Records, past an older checkpoint, replayed by its own format's
// rules. A journal's format never goes down, so a Record below any format before it,
// the checkpoint's or an earlier Record's, means mixed versions wrote it, and Recover
// and StateAt refuse it before replaying anything.

// TestRecover_OlderRecordMidTailIsRefused: a format-1 checkpoint, then this version's
// format-5 Record that parks a result, then an older replica's format-1 Fire. The older
// replica reads the park as a settle (see TestRecover_OlderVersionAfterAParkIsRefused),
// so the two versions disagree about the journal, and Recover must refuse it, naming
// the older version, as it does when the checkpoint itself is newer.
func TestRecover_OlderRecordMidTailIsRefused(t *testing.T) {
	ctx := context.Background()
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return scnUser{ID: 7, Name: "ann"}, nil }
	m := forkMachine(fn)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	st := durable.NewMemStore()
	// An older version, of format 1, started the instance and wrote its checkpoint.
	older := &olderWriter{MemStore: st, format: 1}
	if _, err := durable.NewRunner(m, older, reg, failingDecoder()).Start(ctx, "o1", svcCtx{}, state.WithInitialState("loading")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// This version recovers it and parks the result, in a format-4 Record.
	h, err := durable.Recover(ctx, m, st, "o1", reg, failingDecoder())
	if err != nil {
		t.Fatalf("Recover the older checkpoint: %v", err)
	}
	_, ran, err := h.RunService(ctx, state.InvokeID("fork", "loading", 0))
	mustPark(t, "RunService", ran, err)
	_, recs, err := st.Load(ctx, "o1")
	if err != nil || len(recs) != 1 || recs[0].Format != 5 {
		t.Fatalf("the journal holds %+v (err %v), want the park's one format-5 Record", recs, err)
	}
	// The older replica fires "next" after it, in its format.
	if _, err = st.Append(ctx, "o1", durable.Record{Step: recs[0].Step + 1, Event: []byte(`"next"`), Format: 1}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	rh, err := durable.Recover(ctx, m, st, "o1", reg, failingDecoder())
	if err == nil {
		t.Fatalf("Recover replayed a format-1 Record after a format-4 one, into %q", rh.Instance().Current())
	}
	if msg := err.Error(); !strings.Contains(msg, "older version") || !strings.Contains(msg, "don't run mixed versions") {
		t.Fatalf("Recover: %v, want the mixed-version error", err)
	}
	if _, err = durable.StateAt(ctx, m, st, "o1", recs[0].Step+1, reg, failingDecoder()); err == nil || !strings.Contains(err.Error(), "older version") {
		t.Fatalf("StateAt: %v, want the mixed-version error", err)
	}
}

// TestRecover_RefusesAMixedJournalBeforeReplaying: Recover checks every Record's format
// before it replays any, so a journal it refuses sends no effect. This version's Fire
// records an effect no handler sent, and an older replica appends after it; the effect
// would be sent if Recover only refused on reaching the older Record.
func TestRecover_RefusesAMixedJournalBeforeReplaying(t *testing.T) {
	ctx := context.Background()
	st := durable.NewMemStore()
	h, err := durable.NewRunner(effectMachine(), st).Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	// No handler: step 0's effect is recorded and not sent.
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire go: %v", err)
	}
	if _, err = durable.NewRunner(effectMachine(), &olderWriter{MemStore: st, format: 1}).Fire(ctx, "i1", "bill"); err != nil {
		t.Fatalf("the older replica's Fire: %v", err)
	}
	rec := newEffectRecorder()
	_, err = durable.Recover(ctx, effectMachine(), st, "i1", durable.WithEffectHandler[string, string, *runCtx](rec.handler()))
	if err == nil || !strings.Contains(err.Error(), "older version") {
		t.Fatalf("Recover: %v, want the mixed-version error", err)
	}
	if n := rec.totalApplies(); n != 0 {
		t.Fatalf("Recover sent %v before refusing the journal, want nothing sent", rec.ids())
	}
}

// TestRecover_NonDecreasingFormatsLoad: a journal whose formats never go down loads,
// whatever they are: an older version's checkpoint and Records, then this version's.
func TestRecover_NonDecreasingFormatsLoad(t *testing.T) {
	ctx := context.Background()
	for _, formats := range [][]int{{0, 0, 2}, {0, 1, 2}, {1, 1, 1}, {1, 2, 2}, {0, 2, 2}} {
		st := durable.NewMemStore()
		r := durable.NewRunner(effectMachine(), &olderWriter{MemStore: st, format: formats[0]})
		if _, err := r.Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle")); err != nil {
			t.Fatalf("%v: Start: %v", formats, err)
		}
		if _, err := durable.NewRunner(effectMachine(), &olderWriter{MemStore: st, format: formats[1]}).Fire(ctx, "i1", "go"); err != nil {
			t.Fatalf("%v: Fire go: %v", formats, err)
		}
		if _, err := durable.NewRunner(effectMachine(), &olderWriter{MemStore: st, format: formats[2]}).Fire(ctx, "i1", "bill"); err != nil {
			t.Fatalf("%v: Fire bill: %v", formats, err)
		}
		rh, err := durable.Recover(ctx, effectMachine(), st, "i1")
		if err != nil {
			t.Fatalf("%v: Recover: %v", formats, err)
		}
		if got := rh.Instance().Current(); got != "billed" {
			t.Fatalf("%v: recovered in %q, want billed", formats, got)
		}
		if _, err = durable.StateAt(ctx, effectMachine(), st, "i1", 1); err != nil {
			t.Fatalf("%v: StateAt: %v", formats, err)
		}
	}
}
