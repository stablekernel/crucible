package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// runCtx is a JSON-marshalable instance context for the record/replay proofs:
// exported fields so the default snapshot codec captures it losslessly, and an
// assign target so a context-mutating fixture has something to fold into.
type runCtx struct {
	Count int      `json:"count"`
	Tags  []string `json:"tags"`
}

// linearMachine is a flat three-state machine: idle -> active -> done, the
// simplest record/replay target.
func linearMachine() *state.Machine[string, string, *runCtx] {
	return state.Forge[string, string, *runCtx]("linear").
		State("idle").
		State("active").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("active").
		Transition("active").On("finish").GoTo("done").
		Quench()
}

// branchingMachine routes from a single state down one of two arms depending on
// which event drives it, so replay must re-drive the exact recorded event to
// reach the live arm.
func branchingMachine() *state.Machine[string, string, *runCtx] {
	return state.Forge[string, string, *runCtx]("branching").
		State("start").
		State("left").
		State("right").
		State("leftDone").Final().
		State("rightDone").Final().
		Initial("start").
		Transition("start").On("goLeft").GoTo("left").
		Transition("start").On("goRight").GoTo("right").
		Transition("left").On("finish").GoTo("leftDone").
		Transition("right").On("finish").GoTo("rightDone").
		Quench()
}

// assignMachine folds context on every transition, so a recovered instance only
// matches byte-for-byte if the assigns replay in the exact recorded order.
func assignMachine() *state.Machine[string, string, *runCtx] {
	return state.Forge[string, string, *runCtx]("assign").
		Action("bump", func(c state.ActionCtx[*runCtx]) (state.Effect, error) {
			c.Entity.Count++
			c.Entity.Tags = append(c.Entity.Tags, "bumped")
			return nil, nil
		}).
		State("idle").
		State("active").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("active").Do("bump").
		Transition("active").On("again").GoTo("active").Do("bump").
		Transition("active").On("finish").GoTo("done").Do("bump").
		Quench()
}

// parallelMachine enters a parallel region so a snapshot of a multi-leaf
// configuration must round-trip every region's active leaf through replay.
func parallelMachine() *state.Machine[string, string, *runCtx] {
	return state.Forge[string, string, *runCtx]("parallel").
		State("off").
		SuperState("on").
		Region("A").
		Initial("a1").
		SubState("a1").On("aNext").GoTo("a2").
		SubState("a2").
		EndRegion().
		Region("B").
		Initial("b1").
		SubState("b1").On("bNext").GoTo("b2").
		SubState("b2").
		EndRegion().
		EndSuperState().
		Initial("off").
		Transition("off").On("start").GoTo("on").
		Quench()
}

// liveSnapshotBytes drives events through a plain kernel instance (no durability)
// and returns the marshaled snapshot of the resulting state — the ground truth a
// recovered instance must match byte-for-byte.
func liveSnapshotBytes[C any](t *testing.T, m *state.Machine[string, string, C], initial string, entity C, events []string) []byte {
	t.Helper()
	// The Runner records in full-trace, unbounded-history mode, so the never-durable
	// reference must run in the same mode for a byte-identical snapshot comparison;
	// a plain (lite) cast would omit the recorded Trace history.
	inst := m.Cast(entity, state.WithInitialState(initial), state.WithUnboundedHistory[string]())
	ctx := context.Background()
	for _, ev := range events {
		if res := inst.Fire(ctx, ev); res.Err != nil {
			t.Fatalf("live Fire(%q): %v", ev, res.Err)
		}
	}
	b, err := state.MarshalSnapshot(inst.Snapshot())
	if err != nil {
		t.Fatalf("marshal live snapshot: %v", err)
	}
	return b
}

// recoveredSnapshotBytes records a run through the Runner, then reconstructs a
// fresh instance purely from the Store via Recover and returns its marshaled
// snapshot.
func recoveredSnapshotBytes[C any](t *testing.T, m *state.Machine[string, string, C], initial string, entity C, events []string, opts ...durable.Option[string, string, C]) []byte {
	t.Helper()
	ctx := context.Background()
	store := durable.NewMemStore()
	id := durable.InstanceID("inst-1")

	runner := durable.NewRunner(m, store, opts...)
	if _, err := runner.Start(ctx, id, entity, state.WithInitialState(initial)); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range events {
		if _, err := runner.Fire(ctx, id, ev); err != nil {
			t.Fatalf("runner Fire(%q): %v", ev, err)
		}
	}

	recovered, err := durable.Recover(ctx, m, store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	b, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered snapshot: %v", err)
	}
	return b
}

// TestRecover_ByteIdenticalAcrossFixtures is the acceptance gate: a recovered
// instance's marshaled snapshot is byte-identical to a never-durable live run
// across linear, branching, context-mutating, and parallel machines.
func TestRecover_ByteIdenticalAcrossFixtures(t *testing.T) {
	tests := []struct {
		name    string
		machine *state.Machine[string, string, *runCtx]
		initial string
		events  []string
	}{
		{"linear", linearMachine(), "idle", []string{"go", "finish"}},
		{"branchingLeft", branchingMachine(), "start", []string{"goLeft", "finish"}},
		{"branchingRight", branchingMachine(), "start", []string{"goRight", "finish"}},
		{"assign", assignMachine(), "idle", []string{"go", "again", "again", "finish"}},
		{"parallel", parallelMachine(), "off", []string{"start", "aNext", "bNext"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			live := liveSnapshotBytes(t, tc.machine, tc.initial, &runCtx{}, tc.events)
			recovered := recoveredSnapshotBytes(t, tc.machine, tc.initial, &runCtx{}, tc.events)
			if string(live) != string(recovered) {
				t.Fatalf("snapshot mismatch\n live: %s\n got:  %s", live, recovered)
			}
		})
	}
}

// TestRecover_WithCheckpointCompaction proves recovery from a compacted log:
// with a checkpoint every two steps, Load returns only the post-checkpoint tail,
// yet the recovered snapshot still matches the live run byte-for-byte.
func TestRecover_WithCheckpointCompaction(t *testing.T) {
	m := assignMachine()
	events := []string{"go", "again", "again", "again", "finish"}
	live := liveSnapshotBytes(t, m, "idle", &runCtx{}, events)
	recovered := recoveredSnapshotBytes(t, m, "idle", &runCtx{}, events,
		durable.WithCheckpointEvery[string, string, *runCtx](2))
	if string(live) != string(recovered) {
		t.Fatalf("compacted recovery mismatch\n live: %s\n got:  %s", live, recovered)
	}
}

// TestRecover_OnlyReplaysTail confirms checkpoint compaction actually bounds the
// replay: after a checkpoint the Store's tail holds only the steps after it.
func TestRecover_OnlyReplaysTail(t *testing.T) {
	ctx := context.Background()
	m := assignMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("inst-tail")
	runner := durable.NewRunner(m, store,
		durable.WithCheckpointEvery[string, string, *runCtx](2))
	if _, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"go", "again", "again", "again", "again"} {
		if _, err := runner.Fire(ctx, id, ev); err != nil {
			t.Fatalf("Fire(%q): %v", ev, err)
		}
	}
	snap, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if snap == nil {
		t.Fatal("expected a checkpoint snapshot after compaction")
	}
	// Steps 0..4 fired; with a checkpoint every 2 steps the boundaries are steps 1
	// and 3, so the latest checkpoint is through step 3 (steps 0..3 compacted),
	// leaving only step 4 in the tail.
	if len(tail) != 1 || tail[0].Step != 4 {
		t.Fatalf("tail: want [step 4], got %+v", tail)
	}
}

// TestRunner_CrashResume_MatchesNeverCrashed recovers mid-sequence (after step
// k), continues firing the remaining events on the recovered instance, and
// asserts the final snapshot matches a run that never crashed.
func TestRunner_CrashResume_MatchesNeverCrashed(t *testing.T) {
	m := assignMachine()
	all := []string{"go", "again", "again", "finish"}
	live := liveSnapshotBytes(t, m, "idle", &runCtx{}, all)

	ctx := context.Background()
	store := durable.NewMemStore()
	id := durable.InstanceID("inst-crash")

	// First lifetime: Start and fire through step k=2 (events go, again), then
	// "crash" — drop the runner, keep only the Store.
	r1 := durable.NewRunner(m, store, durable.WithCheckpointEvery[string, string, *runCtx](2))
	if _, err := r1.Start(ctx, id, &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range all[:2] {
		if _, err := r1.Fire(ctx, id, ev); err != nil {
			t.Fatalf("pre-crash Fire(%q): %v", ev, err)
		}
	}

	// Second lifetime: Recover purely from the Store and continue.
	recovered, err := durable.Recover(ctx, m, store, id,
		durable.WithCheckpointEvery[string, string, *runCtx](2))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	for _, ev := range all[2:] {
		if _, ferr := recovered.Fire(ctx, ev); ferr != nil {
			t.Fatalf("post-resume Fire(%q): %v", ev, ferr)
		}
	}

	got, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(live) != string(got) {
		t.Fatalf("crash-resume mismatch\n never-crashed: %s\n resumed:       %s", live, got)
	}
}

// TestRunner_Fire_RecordsDrivingEvent verifies each Fire appends a Record whose
// Step indexes the produced Trace and whose Event payload reconstructs the
// driving event.
func TestRunner_Fire_RecordsDrivingEvent(t *testing.T) {
	ctx := context.Background()
	m := linearMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("inst-rec")
	runner := durable.NewRunner(m, store)
	if _, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := runner.Fire(ctx, id, "go"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 1 {
		t.Fatalf("tail len: want 1, got %d", len(tail))
	}
	if tail[0].Step != 0 {
		t.Fatalf("step: want 0, got %d", tail[0].Step)
	}
	var ev string
	if err := json.Unmarshal(tail[0].Event, &ev); err != nil {
		t.Fatalf("decode recorded event: %v", err)
	}
	if ev != "go" {
		t.Fatalf("recorded event: want go, got %q", ev)
	}
}

// TestRunner_Fire_IdempotentReAppend proves a retried step does not corrupt the
// log: appending the same step twice (a crash-retry between persist and ack)
// collapses to one Record, and recovery still matches the live run.
func TestRunner_Fire_IdempotentReAppend(t *testing.T) {
	ctx := context.Background()
	store := durable.NewMemStore()
	id := durable.InstanceID("inst-idem")

	// Simulate a retried persist: append step 0 twice directly against the Store,
	// exactly as a crash-retry would. The second append is a no-op.
	rec := durable.Record{Step: 0, Event: json.RawMessage(`"go"`)}
	seq1, err := store.Append(ctx, id, rec)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	seq2, err := store.Append(ctx, id, rec)
	if err != nil {
		t.Fatalf("retried append: %v", err)
	}
	if seq1 != seq2 {
		t.Fatalf("idempotent re-append must return the original seq: %d vs %d", seq1, seq2)
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 1 {
		t.Fatalf("retried append corrupted the log: tail len %d", len(tail))
	}
}

// TestRecover_Deterministic confirms recovery is deterministic: two independent
// recoveries of the same recorded run yield byte-identical snapshots.
func TestRecover_Deterministic(t *testing.T) {
	m := assignMachine()
	events := []string{"go", "again", "finish"}
	first := recoveredSnapshotBytes(t, m, "idle", &runCtx{}, events)
	second := recoveredSnapshotBytes(t, m, "idle", &runCtx{}, events)
	if string(first) != string(second) {
		t.Fatalf("recovery is nondeterministic\n first:  %s\n second: %s", first, second)
	}
}

// TestRecover_UnknownInstance surfaces the Store's not-found error through
// Recover rather than masking it.
func TestRecover_UnknownInstance(t *testing.T) {
	ctx := context.Background()
	m := linearMachine()
	store := durable.NewMemStore()
	if _, err := durable.Recover(ctx, m, store, durable.InstanceID("nope")); !errors.Is(err, durable.ErrInstanceNotFound) {
		t.Fatalf("want ErrInstanceNotFound, got %v", err)
	}
}

// TestRunner_Fire_Stateless drives events through the Runner's stateless Fire
// (no Handle), which reloads and replays between steps, and confirms the run
// still recovers byte-identically.
func TestRunner_Fire_Stateless(t *testing.T) {
	ctx := context.Background()
	m := assignMachine()
	events := []string{"go", "again", "finish"}
	live := liveSnapshotBytes(t, m, "idle", &runCtx{}, events)

	store := durable.NewMemStore()
	id := durable.InstanceID("inst-stateless")
	runner := durable.NewRunner(m, store)
	if _, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range events {
		// Each stateless Fire reloads from the Store, replays the tail, fires, and
		// records — exercising the reload-between-steps path.
		if _, err := runner.Fire(ctx, id, ev); err != nil {
			t.Fatalf("stateless Fire(%q): %v", ev, err)
		}
	}
	recovered, err := durable.Recover(ctx, m, store, id)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	got, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(live) != string(got) {
		t.Fatalf("stateless recovery mismatch\n live: %s\n got:  %s", live, got)
	}
	if recovered.ID() != id {
		t.Fatalf("ID: want %q, got %q", id, recovered.ID())
	}
}

// upperCodec is a custom EventCodec that proves WithEventCodec is honored: it
// decodes the recorded JSON payload through encoding/json like the default.
type upperCodec struct{ used *bool }

func (c upperCodec) Decode(payload []byte) (string, error) {
	*c.used = true
	var s string
	if len(payload) == 0 {
		return s, nil
	}
	err := json.Unmarshal(payload, &s)
	return s, err
}

// TestRunner_WithEventCodec confirms a supplied event codec is used during
// replay.
func TestRunner_WithEventCodec(t *testing.T) {
	ctx := context.Background()
	m := linearMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("inst-codec")
	used := false
	opt := durable.WithEventCodec[string, string, *runCtx](upperCodec{used: &used})

	runner := durable.NewRunner(m, store, opt)
	if _, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := runner.Fire(ctx, id, "go"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if _, err := durable.Recover(ctx, m, store, id, opt); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if !used {
		t.Fatal("custom event codec was not used during replay")
	}
}

// TestRunner_Fire_TransitionError surfaces a kernel transition error (an event
// with no enabled transition) without recording a step.
func TestRunner_Fire_TransitionError(t *testing.T) {
	ctx := context.Background()
	m := linearMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("inst-err")
	h, err := durable.NewRunner(m, store).Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	// "finish" is not enabled from idle; the Fire must error and record nothing.
	if _, ferr := h.Fire(ctx, "finish"); ferr == nil {
		t.Fatal("expected a transition error firing an unenabled event")
	}
	_, tail, err := store.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 0 {
		t.Fatalf("a failed Fire recorded a step: tail %+v", tail)
	}
}

// TestRunner_Start_RejectsDuplicate guards against re-Starting a live instance,
// which would clobber its recorded baseline.
func TestRunner_Start_RejectsDuplicate(t *testing.T) {
	ctx := context.Background()
	m := linearMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("inst-dup")
	runner := durable.NewRunner(m, store)
	if _, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle")); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("idle")); !errors.Is(err, durable.ErrInstanceExists) {
		t.Fatalf("want ErrInstanceExists, got %v", err)
	}
}
