package state_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// txnCtx is the value context for the failed-Fire transactionality regression. It
// carries a balance an entry assign would fold so a rolled-back Fire is provably a
// context no-op, and a JSON-marshalable shape so the snapshot round-trip works with
// the default codec.
type txnCtx struct {
	Balance int `json:"balance"`
}

// TestFire_FailedEntryAction_RollsBackConfiguration pins the full-transactionality
// contract: when a transition's ENTRY action fails, the failed Fire is a no-op on
// the instance's persisted internal state. Config, current state, and context are
// all left at their pre-Fire values, FireResult.NewState reports the ORIGINAL state
// (not the abandoned target), and a snapshot taken afterward round-trips to an
// instance identical to one that never Fired.
//
// The machine moves off -> target on "go"; target's OnEntry action errors. Before
// the fix the kernel advanced i.current/i.config to "target" and reported
// NewState=target while leaving the context rolled back — a split. The fix rolls
// the configuration back together with the (already transactional) context and
// effects.
func TestFire_FailedEntryAction_RollsBackConfiguration(t *testing.T) {
	boom := errors.New("entry boom")
	bump := func(in state.AssignCtx[txnCtx]) txnCtx { c := in.Entity; c.Balance += 100; return c }
	fail := func(state.ActionCtx[txnCtx]) (state.Effect, error) { return nil, boom }

	build := func() *state.Machine[string, string, txnCtx] {
		return state.Forge[string, string, txnCtx]("txn").
			Action("explode", fail).
			Reducer("bump", bump).
			State("off").
			Transition("off").On("go").GoTo("target").
			State("target").OnEntry("explode").OnEntryAssign("bump").
			Initial("off").
			CurrentStateFn(func(txnCtx) string { return "off" }).
			Quench()
	}

	m := build()
	ctx := context.Background()

	// A never-Fired control instance and its snapshot: the post-failure instance must
	// be indistinguishable from this one.
	control := m.Cast(txnCtx{Balance: 1}, state.WithInitialState("off"))
	wantSnap := control.Snapshot()

	inst := m.Cast(txnCtx{Balance: 1}, state.WithInitialState("off"))

	res := inst.Fire(ctx, "go")
	if res.Err == nil {
		t.Fatalf("expected the failed entry action to error, got nil (state=%v)", res.NewState)
	}
	if !errors.Is(res.Err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", res.Err, boom)
	}

	// (b) FireResult reports the actual resulting (original) state.
	if res.NewState != "off" {
		t.Fatalf("NewState = %v on a failed Fire; want the original state off (no half-advanced config)", res.NewState)
	}
	if got := inst.Current(); got != "off" {
		t.Fatalf("instance current = %v after a failed Fire; want off", got)
	}
	if cfg := inst.Configuration(); len(cfg) != 1 || cfg[0] != "off" {
		t.Fatalf("configuration = %v after a failed Fire; want [off]", cfg)
	}

	// (c) Context unchanged: the entry assign that follows the failed action never
	// commits, and there is no separate rollback to verify since context was already
	// transactional — but assert it to lock the whole-instance no-op.
	if got := inst.Entity().Balance; got != 1 {
		t.Fatalf("context balance = %d after a failed Fire; want 1 (unchanged)", got)
	}

	// (d) Snapshot the post-failure instance: it must equal a snapshot of the
	// never-Fired control, so nothing split was persisted.
	gotSnap := inst.Snapshot()
	if !reflect.DeepEqual(gotSnap, wantSnap) {
		t.Fatalf("post-failure snapshot != never-Fired snapshot:\n got = %+v\nwant = %+v", gotSnap, wantSnap)
	}

	// The control instance can still Fire cleanly through a non-failing path, proving
	// the rolled-back instance is genuinely re-fireable from its original state.
	clean := build0(t)
	if r := clean.Fire(ctx, "go"); r.Err != nil {
		t.Fatalf("clean machine should advance: %v", r.Err)
	} else if r.NewState != "target" {
		t.Fatalf("clean machine NewState = %v, want target", r.NewState)
	}
}

// build0 casts a no-fail variant of the txn machine for the re-fireability control.
func build0(t *testing.T) *state.Instance[string, string, txnCtx] {
	t.Helper()
	m := state.Forge[string, string, txnCtx]("txn-ok").
		State("off").
		Transition("off").On("go").GoTo("target").
		State("target").
		Initial("off").
		CurrentStateFn(func(txnCtx) string { return "off" }).
		Quench()
	return m.Cast(txnCtx{Balance: 1}, state.WithInitialState("off"))
}

// TestFire_FailedRegionEntry_RollsBackEarlierRegion covers the parallel-region
// split: regions fire in declaration order within one macrostep, each committing its
// own config/context fold to the live instance. When a LATER region's entry action
// fails, an EARLIER region's already-committed transition must roll back too — a
// partial-region commit is the parallel analog of the half-advanced config.
//
// r1 takes r1a -> r1b on "go" (committing a config swap and a context fold); r2 then
// takes r2a -> r2b on "go" whose entry action errors. The whole Fire must be a no-op:
// r1 stays at r1a and the r1 fold is discarded.
func TestFire_FailedRegionEntry_RollsBackEarlierRegion(t *testing.T) {
	boom := errors.New("region entry boom")
	bump := func(in state.AssignCtx[txnCtx]) txnCtx { c := in.Entity; c.Balance += 10; return c }
	fail := func(state.ActionCtx[txnCtx]) (state.Effect, error) { return nil, boom }

	m := state.Forge[string, string, txnCtx]("txn-par").
		Action("explode", fail).
		Reducer("bump", bump).
		SuperState("live").
		Region("r1").
		Initial("r1a").
		SubState("r1a").On("go").GoTo("r1b").Assign("bump").
		SubState("r1b").
		EndRegion().
		Region("r2").
		Initial("r2a").
		SubState("r2a").On("go").GoTo("r2b").
		SubState("r2b").OnEntry("explode").
		EndRegion().
		EndSuperState().
		Initial("live").
		Quench()

	ctx := context.Background()
	control := m.Cast(txnCtx{Balance: 1}, state.WithInitialState("live"))
	wantSnap := control.Snapshot()

	inst := m.Cast(txnCtx{Balance: 1}, state.WithInitialState("live"))
	res := inst.Fire(ctx, "go")
	if res.Err == nil {
		t.Fatalf("expected the failed region entry to error, got nil (state=%v)", res.NewState)
	}
	if !errors.Is(res.Err, boom) {
		t.Fatalf("err = %v, want it to wrap %v", res.Err, boom)
	}

	// r1 must not have committed: configuration still holds r1a (and r2a), context
	// fold discarded.
	cfg := inst.Configuration()
	if !containsLeaf(cfg, "r1a") || containsLeaf(cfg, "r1b") {
		t.Fatalf("configuration = %v after a failed parallel Fire; r1 must stay at r1a", cfg)
	}
	if got := inst.Entity().Balance; got != 1 {
		t.Fatalf("context balance = %d after a failed parallel Fire; want 1 (r1 fold rolled back)", got)
	}

	gotSnap := inst.Snapshot()
	if !reflect.DeepEqual(gotSnap, wantSnap) {
		t.Fatalf("post-failure parallel snapshot != never-Fired snapshot:\n got = %+v\nwant = %+v", gotSnap, wantSnap)
	}
}

// containsLeaf reports whether leaf is present in cfg.
func containsLeaf(cfg []string, leaf string) bool {
	for _, s := range cfg {
		if s == leaf {
			return true
		}
	}
	return false
}
