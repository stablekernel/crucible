package durable_test

// Regression tests for #241: a step whose Append lands but whose Checkpoint then
// fails once must cost the caller only the checkpoint error. The Record is
// durable, so the Handle carries on — the next step succeeds instead of the
// Handle refusing every later one — and a Recover rebuilds the same state and
// step number as the live Handle. Every seam that settles a step — Tick, Fire,
// RunService and DeliverToActor — is pinned: the Tick and Fire ones below, and
// the RunService settle and DeliverToActor ones in the table-driven test after
// them.

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// errCheckpointUnavailable is the one-shot Store failure these tests inject into
// Checkpoint after a step's Append has landed.
var errCheckpointUnavailable = errors.New("checkpoint unavailable")

// failingCheckpointStore fails the next Checkpoint after armed is set, once, with
// errCheckpointUnavailable, so a test can land a step's Append and fail only its
// checkpoint: the failure is a sentinel the tests can errors.Is, and it costs just
// one checkpoint — the retried one succeeds.
type failingCheckpointStore struct {
	durable.Store
	armed bool
}

func newFailingCheckpointStore() *failingCheckpointStore {
	return &failingCheckpointStore{Store: durable.NewMemStore()}
}

func (s *failingCheckpointStore) Checkpoint(ctx context.Context, id durable.InstanceID, snapshot []byte, throughStep int, opts ...durable.CheckpointOption) error {
	if s.armed {
		s.armed = false
		return errCheckpointUnavailable
	}
	return s.Store.Checkpoint(ctx, id, snapshot, throughStep, opts...)
}

// ckptFailTickMachine chains three after timers, so two Ticks each fire one and
// record a tick barrier, the third state can still leave on "finish", and "done"
// can leave on "close". With "arm" fired first and every second step
// checkpointed, the first Tick's Record lands on a due checkpoint and so does the
// next Tick's, so the retried checkpoint succeeds once the Store stops failing.
func ckptFailTickMachine() *state.Machine[string, string, *runCtx] {
	return state.ForgeFor[*runCtx]("ckptFailTick").
		State("idle").
		State("first").
		State("second").
		State("third").
		State("done").
		State("closed").Final().
		Initial("idle").
		Transition("idle").On("arm").GoTo("first").
		Transition("first").After(5 * time.Second).On("t1").GoTo("second").
		Transition("second").After(5 * time.Second).On("t2").GoTo("third").
		Transition("third").After(5 * time.Second).On("t3").GoTo("done").
		Transition("third").On("finish").GoTo("done").
		Transition("done").On("close").GoTo("closed").
		Quench()
}

// ckptFailFireMachine walks one event per step, so with "go" and "again" fired
// first and every third step checkpointed, "stay" lands on a due checkpoint and
// "finish" and "close" step off it.
func ckptFailFireMachine() *state.Machine[string, string, *runCtx] {
	return state.ForgeFor[*runCtx]("ckptFailFire").
		State("idle").
		State("active").
		State("done").
		State("closed").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("active").
		Transition("active").On("again").GoTo("active").
		Transition("active").On("stay").GoTo("active").
		Transition("active").On("finish").GoTo("done").
		Transition("done").On("close").GoTo("closed").
		Quench()
}

// ckptFailServiceMachine walks one step per call: Fires "boot" and "start" reach
// "loading", whose invoked service the seam's RunService settles, and with every
// third step checkpointed the settle lands on a due checkpoint (step 2). The
// onDone Assign folds the result into Notes, so a Recover's snapshot must match
// the live one's byte for byte.
func ckptFailServiceMachine(fn state.ServiceFn[svcCtx]) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("ckptFailSvc").
		Service("fetch", fn).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			c := in.Entity
			if r, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "result:"+r)
			}
			return c
		}).
		State("idle").
		State("warm").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok")).
		State("settled").
		State("closed").Final().
		Initial("idle").
		Transition("idle").On("boot").GoTo("warm").
		Transition("warm").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("settled").Assign("noteDone").
		Transition("settled").On("poke").GoTo("settled").
		Transition("settled").On("close").GoTo("closed").
		Quench()
}

// ckptFailActorMachine walks one step per call: Fires "boot" and "start" reach
// "working", whose spawned child the seam's DeliverToActor settles, and with every
// third step checkpointed the delivery lands on a due checkpoint (step 2). The
// onDone Assign folds the child's output into Notes, so a Recover's snapshot must
// match the live one's byte for byte.
func ckptFailActorMachine() *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("ckptFailActor").
		Actor("finisher").
		Reducer("noteChild", func(in state.AssignCtx[svcCtx]) svcCtx {
			c := in.Entity
			if o, ok := in.Event.(string); ok {
				c.Notes = append(c.Notes, "child:"+o)
			}
			return c
		}).
		State("idle").
		State("warm").
		State("working").InvokeActor("finisher", state.WithInvokeOnDone("childDone")).
		State("settled").
		State("closed").Final().
		Initial("idle").
		Transition("idle").On("boot").GoTo("warm").
		Transition("warm").On("start").GoTo("working").
		Transition("working").On("childDone").GoTo("settled").Assign("noteChild").
		Transition("settled").On("poke").GoTo("settled").
		Transition("settled").On("close").GoTo("closed").
		Quench()
}

// ckptFailFinisher is the child a delivery settles: a machine that completes when
// delivered "finish", reporting "child-done" as its output.
func ckptFailFinisher() state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		child := state.ForgeFor[svcCtx]("ckptFailChild").
			State("working").
			State("done").Final().
			Initial("working").
			Transition("working").On("finish").GoTo("done").
			Quench()
		return state.NewActor(child.Cast(svcCtx{}, state.WithInitialState[string]("working")),
			func(*state.Instance[string, string, svcCtx]) any { return "child-done" }), nil
	}
}

// mustMatchLive fails unless the recovered Handle holds the live Handle's state:
// the same current state and a byte-identical snapshot.
func mustMatchLive[C any](t *testing.T, live, recovered *durable.Handle[string, string, C]) {
	t.Helper()
	if got, want := recovered.Instance().Current(), live.Instance().Current(); got != want {
		t.Fatalf("recovered in %q, want %q, as live", got, want)
	}
	liveSnap, err := state.MarshalSnapshot(live.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal live snapshot: %v", err)
	}
	recSnap, err := state.MarshalSnapshot(recovered.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered snapshot: %v", err)
	}
	if !bytes.Equal(liveSnap, recSnap) {
		t.Fatalf("recovered snapshot differs from live:\nlive: %s\ngot:  %s", liveSnap, recSnap)
	}
}

// TestTickCheckpointFailsOnceAfterLandedAppend_HandleCarriesOn is #241's
// reproduction: a Tick whose Append lands but whose Checkpoint then fails once
// returns the checkpoint error with its results, the next Tick succeeds instead
// of the Handle refusing every later step, and Recovers rebuild the live
// Handle's state and step number from the durable Record.
func TestTickCheckpointFailsOnceAfterLandedAppend_HandleCarriesOn(t *testing.T) {
	ctx := context.Background()
	id := durable.InstanceID("tick-ckpt-fail")
	m := ckptFailTickMachine()
	clk := state.NewFakeClock(epoch)
	st := newFailingCheckpointStore()
	opts := []durable.Option[string, string, *runCtx]{
		durable.WithRunnerClock[string, string, *runCtx](clk),
		durable.WithCheckpointEvery[string, string, *runCtx](2),
	}
	h, err := durable.NewRunner(m, st, opts...).Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "arm"); err != nil { // step 0
		t.Fatalf("Fire arm: %v", err)
	}
	st.armed = true // the Tick's checkpoint at step 1 fails; its Append lands

	// The failed Tick returns its results with the checkpoint error, and the
	// Handle hasn't failed: the Record is durable, only the checkpoint isn't.
	clk.Advance(6 * time.Second)
	results, err := h.Tick(ctx)
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("the failed-checkpoint Tick = %d results (first err %v), want the timer fired", len(results), resultsErr(results))
	}
	if !errors.Is(err, errCheckpointUnavailable) {
		t.Fatalf("the Tick's error = %v, want the checkpoint failure", err)
	}
	if errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("the failed checkpoint failed the Handle: %v", err)
	}
	// The Append landed: the journal holds the tick's Record past the baseline.
	_, tail, lerr := st.Load(ctx, id)
	if lerr != nil || len(tail) != 2 || tail[1].Step != 1 || !tail[1].Tick || tail[1].TickSteps != 1 {
		t.Fatalf("after the failed checkpoint, Load = %d Records (%v); want the tick barrier of step 1 durable past the baseline", len(tail), lerr)
	}

	// A Recover right after the failure replays that Record and rebuilds the
	// live Handle's state.
	rh, err := durable.Recover(ctx, m, st, id, opts...)
	if err != nil {
		t.Fatalf("Recover after the failed checkpoint: %v", err)
	}
	mustMatchLive(t, h, rh)

	// The Handle carries on: the next Tick records its own step instead of
	// refusing with ErrHandleFailed, and its checkpoint — the retry the failed one
	// waited for — succeeds now that the Store no longer fails.
	clk.Advance(6 * time.Second)
	if results, err = h.Tick(ctx); len(results) != 1 || results[0].Err != nil || err != nil {
		t.Fatalf("the next Tick = %d results, %v; want it to fire and succeed", len(results), err)
	}
	if got := h.Instance().Current(); got != "third" {
		t.Fatalf("live in %q, want third", got)
	}
	if _, err = h.Fire(ctx, "finish"); err != nil { // step 5, also checkpoint-due
		t.Fatalf("the Fire after that = %v, want it to succeed too", err)
	}
	if got := h.Instance().Current(); got != "done" {
		t.Fatalf("live in %q, want done", got)
	}

	// A Recover now rebuilds the same state and the same step number: its next
	// Fire takes step 6, one past the live Handle's last Record, which is exactly
	// where the live Handle's next step goes.
	rh, err = durable.Recover(ctx, m, st, id, opts...)
	if err != nil {
		t.Fatalf("Recover after the next Tick: %v", err)
	}
	mustMatchLive(t, h, rh)
	if _, err = rh.Fire(ctx, "close"); err != nil {
		t.Fatalf("the recovered Handle's Fire: %v", err)
	}
	_, tail, lerr = st.Load(ctx, id)
	if lerr != nil || len(tail) != 1 || tail[0].Step != 6 {
		t.Fatalf("the recovered Handle recorded step %d (Load: %d Records, %v); want 6, the live Handle's next step", tailStep(tail), len(tail), lerr)
	}
}

// TestFireCheckpointFailsOnceAfterLandedAppend_HandleCarriesOn is #241's Fire
// variant: a Fire whose Append lands but whose Checkpoint then fails once
// returns the checkpoint error with its result, the next Fire succeeds instead
// of the Handle refusing every later step, and Recovers rebuild the live
// Handle's state and step number from the durable Record.
func TestFireCheckpointFailsOnceAfterLandedAppend_HandleCarriesOn(t *testing.T) {
	ctx := context.Background()
	id := durable.InstanceID("fire-ckpt-fail")
	m := ckptFailFireMachine()
	st := newFailingCheckpointStore()
	opts := []durable.Option[string, string, *runCtx]{
		durable.WithCheckpointEvery[string, string, *runCtx](3),
	}
	h, err := durable.NewRunner(m, st, opts...).Start(ctx, id, &runCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, ev := range []string{"go", "again"} { // steps 0 and 1
		if _, err = h.Fire(ctx, ev); err != nil {
			t.Fatalf("Fire %s: %v", ev, err)
		}
	}
	st.armed = true // "stay"'s checkpoint at step 2 fails; its Append lands

	// The failed Fire returns its result with the checkpoint error, and the
	// Handle hasn't failed: the Record is durable, only the checkpoint isn't.
	res, err := h.Fire(ctx, "stay")
	if res.Err != nil {
		t.Fatalf("the failed-checkpoint Fire's transition: %v", res.Err)
	}
	if !errors.Is(err, errCheckpointUnavailable) {
		t.Fatalf("the Fire's error = %v, want the checkpoint failure", err)
	}
	if errors.Is(err, durable.ErrHandleFailed) {
		t.Fatalf("the failed checkpoint failed the Handle: %v", err)
	}
	if got := h.Instance().Current(); got != "active" {
		t.Fatalf("live in %q, want active", got)
	}
	// The Append landed: the journal holds step 2's Record past the baseline.
	_, tail, lerr := st.Load(ctx, id)
	if lerr != nil || len(tail) != 3 || tail[2].Step != 2 || tail[2].Tick {
		t.Fatalf("after the failed checkpoint, Load = %d Records (%v); want step 2's Record durable past the baseline", len(tail), lerr)
	}

	// A Recover right after the failure replays that Record and rebuilds the
	// live Handle's state.
	rh, err := durable.Recover(ctx, m, st, id, opts...)
	if err != nil {
		t.Fatalf("Recover after the failed checkpoint: %v", err)
	}
	mustMatchLive(t, h, rh)

	// The Handle carries on: the next Fire records its own step instead of
	// refusing with ErrHandleFailed.
	if _, err = h.Fire(ctx, "finish"); err != nil {
		t.Fatalf("the next Fire = %v, want it to succeed", err)
	}
	if got := h.Instance().Current(); got != "done" {
		t.Fatalf("live in %q, want done", got)
	}

	// A Recover now rebuilds the same state and the same step number: its next
	// Fire takes step 4, one past the live Handle's last Record, which is
	// exactly where the live Handle's next step goes.
	rh, err = durable.Recover(ctx, m, st, id, opts...)
	if err != nil {
		t.Fatalf("Recover after the next Fire: %v", err)
	}
	mustMatchLive(t, h, rh)
	if _, err = rh.Fire(ctx, "close"); err != nil {
		t.Fatalf("the recovered Handle's Fire: %v", err)
	}
	_, tail, lerr = st.Load(ctx, id)
	if lerr != nil || len(tail) != 5 || tail[4].Step != 4 {
		t.Fatalf("the recovered Handle recorded step %d (Load: %d Records, %v); want 4, the live Handle's next step", tailStep(tail), len(tail), lerr)
	}
}

// TestSeamCheckpointFailsOnceAfterLandedAppend_HandleCarriesOn is #241's pin for
// the other two seams that settle a step, the RunService settle and
// DeliverToActor. Each drives one step whose Append lands but whose Checkpoint
// then fails once, and each checks the same contract as the Tick and Fire tests
// above: the seam's call returns the checkpoint error rather than
// ErrHandleFailed, the Record is durable in the journal, the next step succeeds
// instead of the Handle refusing it, and Recovers rebuild the live Handle's
// state and step number, so the recovered Handle's next Fire records at exactly
// the live Handle's next step.
func TestSeamCheckpointFailsOnceAfterLandedAppend_HandleCarriesOn(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		id        durable.InstanceID
		newModel  func() (*state.Machine[string, string, svcCtx], []durable.Option[string, string, svcCtx])
		warmup    []string // the events fired first, so the seam's own step is step 2, a due checkpoint
		seam      func(t *testing.T, h *durable.Handle[string, string, svcCtx], st *failingCheckpointStore)
		seamEntry func(rec durable.Record) bool
	}{
		{
			name: "RunService settle",
			id:   "service-ckpt-fail",
			newModel: func() (*state.Machine[string, string, svcCtx], []durable.Option[string, string, svcCtx]) {
				fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return "service-data", nil }
				return ckptFailServiceMachine(fn), []durable.Option[string, string, svcCtx]{
					durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
					durable.WithCheckpointEvery[string, string, svcCtx](3),
				}
			},
			warmup: []string{"boot", "start"},
			seam: func(t *testing.T, h *durable.Handle[string, string, svcCtx], st *failingCheckpointStore) {
				st.armed = true // the settle's checkpoint at step 2 fails; its Append lands
				res, ran, err := h.RunService(ctx, state.InvokeID("ckptFailSvc", "loading", 0))
				if !ran || res.Err != nil {
					t.Fatalf("the failed-checkpoint RunService = ran %v, transition err %v; want the settle applied", ran, res.Err)
				}
				if !errors.Is(err, errCheckpointUnavailable) {
					t.Fatalf("RunService's error = %v, want the checkpoint failure", err)
				}
				if errors.Is(err, durable.ErrHandleFailed) {
					t.Fatalf("the failed checkpoint failed the Handle: %v", err)
				}
			},
			seamEntry: hasEntry(state.JournalServiceResult, state.InvokeID("ckptFailSvc", "loading", 0)),
		},
		{
			name: "DeliverToActor",
			id:   "actor-ckpt-fail",
			newModel: func() (*state.Machine[string, string, svcCtx], []durable.Option[string, string, svcCtx]) {
				return ckptFailActorMachine(), []durable.Option[string, string, svcCtx]{
					durable.WithActorPalette[string, string, svcCtx](map[string]state.ActorBehavior{"finisher": ckptFailFinisher()}),
					durable.WithCheckpointEvery[string, string, svcCtx](3),
				}
			},
			warmup: []string{"boot", "start"},
			seam: func(t *testing.T, h *durable.Handle[string, string, svcCtx], st *failingCheckpointStore) {
				st.armed = true // the delivery's checkpoint at step 2 fails; its Append lands
				found, err := h.DeliverToActor(ctx, state.ActorRef{ID: state.ActorID("ckptFailActor", "working", 0)}, "finish")
				if !found {
					t.Fatal("DeliverToActor reported the actor was not running")
				}
				if !errors.Is(err, errCheckpointUnavailable) {
					t.Fatalf("DeliverToActor's error = %v, want the checkpoint failure", err)
				}
				if errors.Is(err, durable.ErrHandleFailed) {
					t.Fatalf("the failed checkpoint failed the Handle: %v", err)
				}
			},
			seamEntry: hasEntry(state.JournalActorMessage, state.ActorID("ckptFailActor", "working", 0)),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, opts := tc.newModel()
			st := newFailingCheckpointStore()
			h, err := durable.NewRunner(m, st, opts...).Start(ctx, tc.id, svcCtx{}, state.WithInitialState("idle"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			for _, ev := range tc.warmup { // steps 0 and 1
				if _, err = h.Fire(ctx, ev); err != nil {
					t.Fatalf("Fire %s: %v", ev, err)
				}
			}
			tc.seam(t, h, st) // step 2: the Append lands, the Checkpoint fails once

			if got := h.Instance().Current(); got != "settled" {
				t.Fatalf("live in %q, want settled", got)
			}
			// The Append landed: the journal holds the seam's Record at step 2 past
			// the baseline.
			_, tail, lerr := st.Load(ctx, tc.id)
			if lerr != nil || len(tail) != 3 || tail[2].Step != 2 || !tc.seamEntry(tail[2]) {
				t.Fatalf("after the failed checkpoint, Load = %d Records (%v); want the seam's Record of step 2 durable past the baseline", len(tail), lerr)
			}

			// A Recover right after the failure replays that Record and rebuilds
			// the live Handle's state.
			rh, err := durable.Recover(ctx, m, st, tc.id, opts...)
			if err != nil {
				t.Fatalf("Recover after the failed checkpoint: %v", err)
			}
			mustMatchLive(t, h, rh)

			// The Handle carries on: the next step records its own instead of
			// refusing with ErrHandleFailed.
			if _, err = h.Fire(ctx, "poke"); err != nil { // step 3
				t.Fatalf("the next Fire = %v, want it to succeed", err)
			}

			// A Recover now rebuilds the same state and the same step number: its
			// next Fire takes step 4, one past the live Handle's last Record, which
			// is exactly where the live Handle's next step goes.
			rh, err = durable.Recover(ctx, m, st, tc.id, opts...)
			if err != nil {
				t.Fatalf("Recover after the next Fire: %v", err)
			}
			mustMatchLive(t, h, rh)
			if _, err = rh.Fire(ctx, "close"); err != nil { // step 4
				t.Fatalf("the recovered Handle's Fire: %v", err)
			}
			_, tail, lerr = st.Load(ctx, tc.id)
			if lerr != nil || len(tail) != 5 || tail[4].Step != 4 {
				t.Fatalf("the recovered Handle recorded step %d (Load: %d Records, %v); want 4, the live Handle's next step", tailStep(tail), len(tail), lerr)
			}
		})
	}
}

// hasEntry reports whether rec carries a journal entry of kind correlated by id,
// identifying a seam's Record in the tail.
func hasEntry(kind state.JournalKind, id string) func(durable.Record) bool {
	return func(rec durable.Record) bool {
		for _, e := range rec.Entries {
			if e.Kind == kind && e.CorrelationID == id {
				return true
			}
		}
		return false
	}
}

// resultsErr returns the first result's error, or nil when there are none, for
// failure messages.
func resultsErr[S comparable](results []state.FireResult[S]) error {
	if len(results) == 0 {
		return nil
	}
	return results[0].Err
}

// tailStep returns the tail's last step, or -1 when the tail is empty, for
// failure messages.
func tailStep(tail []durable.Record) int {
	if len(tail) == 0 {
		return -1
	}
	return tail[len(tail)-1].Step
}
