package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// effectsDroppingStore is a Store that keeps every Record field but Effects, as a
// custom backend with a schema that has no column for the envelopes would.
type effectsDroppingStore struct{ *durable.MemStore }

func (s effectsDroppingStore) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	rec.Effects = nil
	return s.MemStore.Append(ctx, id, rec, opts...)
}

// TestRecover_StoreThatDropsEffects_SendsACrashedStepsEffects answers whether
// exactly-once depends on the Store keeping Record.Effects. A step whose dispatch
// failed live, as a crash between the append and the dispatch leaves it, holds its
// effect unmarked. Recover must send it once, under the id the live run stamped, even
// from a Store that didn't keep the envelopes: replay re-derives the effect, and the
// Record's format says this version dispatched it. Before this change a Fire's effects
// were sent this way whatever the Record held.
func TestRecover_StoreThatDropsEffects_SendsACrashedStepsEffects(t *testing.T) {
	ctx := context.Background()
	t.Run("fire", func(t *testing.T) {
		st := effectsDroppingStore{durable.NewMemStore()}
		live := &stepIDHandler{failStep: "0"}
		h, err := durable.NewRunner(effectMachine(), st, durable.WithEffectHandler[string, string, *runCtx](live.handle)).
			Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "go"); !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("Fire go: %v, want ErrEffectDispatch", err)
		}
		healthy := newEffectRecorder()
		if _, err = durable.Recover(ctx, effectMachine(), st, "i1", durable.WithEffectHandler[string, string, *runCtx](healthy.handler())); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		assertAppliedOnceAsLive(t, live, healthy, 1)
	})
	t.Run("tick", func(t *testing.T) {
		st := effectsDroppingStore{durable.NewMemStore()}
		clk := state.NewFakeClock(epoch)
		live := &stepIDHandler{failStep: "1"}
		h, err := durable.NewRunner(timerNotifyMachine(), st,
			durable.WithEffectHandler[string, string, *runCtx](live.handle),
			durable.WithRunnerClock[string, string, *runCtx](clk)).
			Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "go"); err != nil {
			t.Fatalf("Fire go: %v", err)
		}
		clk.Advance(2 * time.Second)
		if _, err = h.Tick(ctx); !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("Tick: %v, want ErrEffectDispatch", err)
		}
		healthy := newEffectRecorder()
		if _, err = durable.Recover(ctx, timerNotifyMachine(), st, "i1",
			durable.WithEffectHandler[string, string, *runCtx](healthy.handler()),
			durable.WithRunnerClock[string, string, *runCtx](clk)); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		assertAppliedOnceAsLive(t, live, healthy, 1)
	})
	t.Run("service", func(t *testing.T) {
		st := effectsDroppingStore{durable.NewMemStore()}
		var calls int64
		fn := counterService(&calls)
		reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
		live := &stepIDHandler{failStep: "1"}
		h, err := durable.NewRunner(fetchThenWaitMachine(fn), st, reg, durable.WithEffectHandler[string, string, svcCtx](live.handle)).
			Start(ctx, "i1", svcCtx{}, state.WithInitialState("idle"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if _, err = h.Fire(ctx, "start"); err != nil {
			t.Fatalf("Fire start: %v", err)
		}
		if _, _, err = h.RunService(ctx, state.InvokeID("fetchThenWait", "loading", 0)); !errors.Is(err, durable.ErrEffectDispatch) {
			t.Fatalf("RunService: %v, want ErrEffectDispatch", err)
		}
		healthy := newEffectRecorder()
		if _, err = durable.Recover(ctx, fetchThenWaitMachine(fn), st, "i1", reg,
			durable.WithEffectHandler[string, string, svcCtx](healthy.handler())); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		assertAppliedOnceAsLive(t, live, healthy, 1)
		if calls != 1 {
			t.Fatalf("the service ran %d times, want 1", calls)
		}
	})
}

// withCheckpointFormat returns the checkpoint envelope cp with its journal format set to
// format, as the version that wrote format would have left it.
func withCheckpointFormat(t *testing.T, cp []byte, format int) []byte {
	t.Helper()
	var env map[string]json.RawMessage
	if err := json.Unmarshal(cp, &env); err != nil {
		t.Fatalf("decoding the checkpoint envelope: %v", err)
	}
	env["format"] = json.RawMessage(itoa(format))
	out, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("encoding the checkpoint envelope: %v", err)
	}
	return out
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// TestRecover_OlderVersionsFireEffectIsSentWithoutEnvelopes keeps what every version
// did for a Fire: Recover sends its unmarked effects whatever the Record holds. The
// journal here is of format 1, from a version before tick and service effects were
// dispatched, and its Fire Record names no effects.
func TestRecover_OlderVersionsFireEffectIsSentWithoutEnvelopes(t *testing.T) {
	ctx := context.Background()
	src := durable.NewMemStore()
	live := &stepIDHandler{failStep: "0"}
	h, err := durable.NewRunner(effectMachine(), src, durable.WithEffectHandler[string, string, *runCtx](live.handle)).
		Start(ctx, "i1", &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); !errors.Is(err, durable.ErrEffectDispatch) {
		t.Fatalf("Fire go: %v, want ErrEffectDispatch", err)
	}
	snap, recs, err := src.Load(ctx, "i1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	old := durable.NewMemStore()
	if err = old.Checkpoint(ctx, "i1", withCheckpointFormat(t, snap, 1), -1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	for _, rec := range recs {
		rec.Format, rec.Effects = 1, nil
		if _, err = old.Append(ctx, "i1", rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	healthy := newEffectRecorder()
	if _, err = durable.Recover(ctx, effectMachine(), old, "i1", durable.WithEffectHandler[string, string, *runCtx](healthy.handler())); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertAppliedOnceAsLive(t, live, healthy, 1)
}

// asFormatOne copies src's journal for id into a fresh MemStore with the checkpoint and
// every Record of journal format 1, older than tickDriversFormat: a version that still
// recorded idle ticks and left a settle's clock read for the next Record, so a test can
// hand-build the Records such a version left.
func asFormatOne(t *testing.T, src *durable.MemStore, id durable.InstanceID) *durable.MemStore {
	t.Helper()
	return asFormat(t, src, id, 1)
}

// asFormat copies src's journal for id into a fresh MemStore with the checkpoint and
// every Record of journal format, as a version that wrote that format left them.
func asFormat(t *testing.T, src *durable.MemStore, id durable.InstanceID, format int) *durable.MemStore {
	t.Helper()
	ctx := context.Background()
	snap, recs, err := src.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	dst := durable.NewMemStore()
	if err = dst.Checkpoint(ctx, id, withCheckpointFormat(t, snap, format), -1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	for _, rec := range recs {
		rec.Format = format
		if _, err = dst.Append(ctx, id, rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	return dst
}
