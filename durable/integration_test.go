package durable_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// errInjectedEffectCrash simulates a crash mid effect-dispatch on the live path, so
// the effect's write-ahead Record survives but its dispatched mark does not.
var errInjectedEffectCrash = errors.New("injected: crashed mid effect dispatch")

// This file is the durable capstone: one machine that exercises EVERY durable seam
// together — an invoked service, a spawned child-machine actor, a domain effect, and
// a delayed (`after`) timer — driven through the Runner over both the in-memory and
// the on-disk Store, with crashes injected at each seam boundary. It proves the
// seams compose: a recovery at any point reaches a snapshot byte-identical to a
// never-crashed reference, each external call (service, actor) happens exactly once,
// each domain effect is applied exactly once, and the timer fires from its recorded
// deadline regardless of the wall clock at recovery.

// pipelineCtx folds each seam's routed result so a recovered instance's context can
// be compared byte-for-byte against a never-crashed run. Exported fields so the
// default snapshot codec captures it losslessly.
type pipelineCtx struct {
	Fetched string   `json:"fetched"`
	Worked  string   `json:"worked"`
	Notes   []string `json:"notes"`
}

// pipelineSeams records the per-id domain-effect applications the capstone asserts
// exactly-once over. The service-invocation and actor-instantiation counters are
// plain int64s the test threads through the registry and palette.
type pipelineSeams struct {
	mu         sync.Mutex
	effectHits map[string]int
}

func newPipelineSeams() *pipelineSeams {
	return &pipelineSeams{effectHits: map[string]int{}}
}

func (s *pipelineSeams) effectHandler() durable.EffectHandler {
	return func(_ context.Context, effectID string, _ state.Effect) error {
		s.mu.Lock()
		s.effectHits[effectID]++
		s.mu.Unlock()
		return nil
	}
}

// failingEffectHandler returns a handler that fails the FIRST application of a domain
// effect without recording a hit, simulating a crash AFTER the write-ahead Record was
// appended but BEFORE the effect was marked dispatched. A later recovery dispatches
// it exactly once through the healthy handler. Subsequent applications (none expected
// before the crash) record normally.
func failingEffectHandler(s *pipelineSeams) durable.EffectHandler {
	var failed bool
	return func(_ context.Context, effectID string, _ state.Effect) error {
		if !failed {
			failed = true
			return errInjectedEffectCrash
		}
		s.mu.Lock()
		s.effectHits[effectID]++
		s.mu.Unlock()
		return nil
	}
}

// totalEffectApplies returns how many domain-effect applications landed, across all
// ids.
func (s *pipelineSeams) totalEffectApplies() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, n := range s.effectHits {
		total += n
	}
	return total
}

// maxEffectApplies returns the highest per-id application count, which must be 1 for
// exactly-once.
func (s *pipelineSeams) maxEffectApplies() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	maxN := 0
	for _, n := range s.effectHits {
		if n > maxN {
			maxN = n
		}
	}
	return maxN
}

// pipelineService is the invoked service: it returns a fresh value each call, so a
// byte-identical recovery is only possible if the recorded result was replayed and
// the service was not re-invoked.
func pipelineService(calls *int64) state.ServiceFn[*pipelineCtx] {
	return func(context.Context, state.ServiceCtx[*pipelineCtx]) (any, error) {
		n := atomic.AddInt64(calls, 1)
		return fmt.Sprintf("fetched-%d", n), nil
	}
}

// pipelineChild is the spawned actor behavior: its completion output differs each
// run, so byte-identity proves the recorded output was replayed, not regenerated.
func pipelineChild(runs *int64) state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		child := state.Forge[string, string, *pipelineCtx]("worker").
			State("working").
			State("finished").Final().
			Initial("working").
			Transition("working").On("complete").GoTo("finished").
			Quench()
		inst := child.Cast(&pipelineCtx{}, state.WithInitialState[string]("working"))
		return state.NewActor(inst, func(*state.Instance[string, string, *pipelineCtx]) any {
			n := atomic.AddInt64(runs, 1)
			return fmt.Sprintf("worked-%d", n)
		}), nil
	}
}

// pipelineEffect is the domain effect the notifying step emits — a side effect the
// machine cannot perform itself, dispatched through the EffectHandler exactly once.
type pipelineEffect struct {
	Channel string `json:"channel"`
}

func (pipelineEffect) Kind() string { return "pipeline.notify" }

// pipelineMachine wires all four durable seams into one lifetime:
//
//	idle -(start)-> fetching [invoke load]
//	  -(loaded)-> assign(recordFetch) -> spawning [invokeActor worker]
//	    -(workerDone)-> assign(recordWork) -> ready
//	      -(proceed)-> notifying [emit notify effect; after 10s]
//	        -(elapsed)-> done(final)
//
// So one instance reads the clock (timer arm + tick), runs one service, runs one
// actor, and emits one domain effect — every seam the durable runtime records. The
// timer and the domain effect arm on the plain `proceed` Fire so a settle never has
// to carry a timer effect into the next seam.
func pipelineMachine() *state.Machine[string, string, *pipelineCtx] {
	return state.Forge[string, string, *pipelineCtx]("pipeline").
		Service("load", nil). // bound by the Runner's registry; nil here is a declaration
		Actor("worker").
		Reducer("recordFetch", func(in state.AssignCtx[*pipelineCtx]) *pipelineCtx {
			c := in.Entity
			if r, ok := in.Event.(string); ok {
				c.Fetched = r
				c.Notes = append(c.Notes, "fetched:"+r)
			}
			return c
		}).
		Reducer("recordWork", func(in state.AssignCtx[*pipelineCtx]) *pipelineCtx {
			c := in.Entity
			if o, ok := in.Event.(string); ok {
				c.Worked = o
				c.Notes = append(c.Notes, "worked:"+o)
			}
			return c
		}).
		Action("notify", func(c state.ActionCtx[*pipelineCtx]) (state.Effect, error) {
			c.Entity.Notes = append(c.Entity.Notes, "notified")
			return pipelineEffect{Channel: "ops"}, nil
		}).
		Action("markDone", func(c state.ActionCtx[*pipelineCtx]) (state.Effect, error) {
			c.Entity.Notes = append(c.Entity.Notes, "done")
			return nil, nil
		}).
		State("idle").
		State("fetching").Invoke("load", state.WithInvokeOnDone("loaded"), state.WithInvokeOnError("loadFail")).
		State("spawning").InvokeActor("worker", state.WithInvokeOnDone("workerDone"), state.WithInvokeOnError("workerFail")).
		State("ready").
		State("notifying").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("start").GoTo("fetching").
		Transition("fetching").On("loaded").GoTo("spawning").Assign("recordFetch").
		Transition("spawning").On("workerDone").GoTo("ready").Assign("recordWork").
		Transition("ready").On("proceed").GoTo("notifying").Do("notify").
		Transition("notifying").After(10 * time.Second).On("elapsed").GoTo("done").Do("markDone").
		Quench()
}

// pipelineRegistry binds the invoked service under "load" so the Runner resolves the
// same implementation the machine declares.
func pipelineRegistry(fn state.ServiceFn[*pipelineCtx]) *state.Registry[*pipelineCtx] {
	return state.NewRegistry[*pipelineCtx]().Service("load", fn)
}

// pipelinePalette binds the actor behavior under "worker".
func pipelinePalette(runs *int64) map[string]state.ActorBehavior {
	return map[string]state.ActorBehavior{"worker": pipelineChild(runs)}
}

// loadInvokeID / workerActorID name the in-flight service and spawned actor at their
// declaring states, the ids the Handle settles and delivers by.
func loadInvokeID() string  { return state.InvokeID("pipeline", "fetching", 0) }
func workerActorID() string { return state.ActorID("pipeline", "spawning", 0) }

// pipelineRunner builds a Runner wiring every seam against st on the fixed fake clock
// clk, so a recovered run and a reference run share one deterministic time source.
func pipelineRunner(
	m *state.Machine[string, string, *pipelineCtx],
	st durable.Store,
	clk state.Clock,
	seams *pipelineSeams,
	svcCalls, actorRuns *int64,
) *durable.Runner[string, string, *pipelineCtx] {
	return durable.NewRunner(
		m, st,
		durable.WithRunnerClock[string, string, *pipelineCtx](clk),
		durable.WithServiceRegistry[string, string, *pipelineCtx](pipelineRegistry(pipelineService(svcCalls))),
		durable.WithActorPalette[string, string, *pipelineCtx](pipelinePalette(actorRuns)),
		durable.WithEffectHandler[string, string, *pipelineCtx](seams.effectHandler()),
		durable.WithCheckpointEvery[string, string, *pipelineCtx](2),
	)
}

// pipelineReference drives the whole lifetime through a single never-crashed Handle
// and returns its final marshaled snapshot — the byte-identical ground truth every
// crash-injected recovery must match. It asserts the never-crashed run itself invokes
// each seam exactly once.
func pipelineReference(t *testing.T, newStore func() durable.Store) []byte {
	t.Helper()
	ctx := context.Background()
	m := pipelineMachine()
	clk := state.NewFakeClock(epoch)
	seams := newPipelineSeams()
	var svcCalls, actorRuns int64

	r := pipelineRunner(m, newStore(), clk, seams, &svcCalls, &actorRuns)
	h, err := r.Start(ctx, "ref", &pipelineCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("ref Start: %v", err)
	}
	drivePipeline(t, ctx, h, clk)

	if got := atomic.LoadInt64(&svcCalls); got != 1 {
		t.Fatalf("reference invoked the service %d times, want 1", got)
	}
	if got := atomic.LoadInt64(&actorRuns); got != 1 {
		t.Fatalf("reference ran the actor %d times, want 1", got)
	}
	if seams.totalEffectApplies() != 1 || seams.maxEffectApplies() != 1 {
		t.Fatalf("reference effect applies: total=%d max=%d, want 1/1", seams.totalEffectApplies(), seams.maxEffectApplies())
	}
	b, err := state.MarshalSnapshot(h.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal ref: %v", err)
	}
	if got := h.Instance().Current(); got != "done" {
		t.Fatalf("reference did not reach done: %q", got)
	}
	return b
}

// drivePipeline advances a live Handle through every seam to completion: fire start,
// settle the service, deliver to the actor, then advance the clock past the timer
// deadline and tick it to fire. It is the canonical live driver both the reference
// and each post-recovery continuation reuse.
func drivePipeline(t *testing.T, ctx context.Context, h *durable.Handle[string, string, *pipelineCtx], clk *state.FakeClock) {
	t.Helper()
	if _, err := h.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	if _, ok, err := h.RunService(ctx, loadInvokeID()); err != nil || !ok {
		t.Fatalf("RunService: ok=%v err=%v", ok, err)
	}
	ref, ok := h.ActorRef(workerActorID())
	if !ok {
		t.Fatalf("no actor ref for spawned worker")
	}
	if delivered, err := h.DeliverToActor(ctx, ref, "complete"); err != nil || !delivered {
		t.Fatalf("DeliverToActor: delivered=%v err=%v", delivered, err)
	}
	if _, err := h.Fire(ctx, "proceed"); err != nil { // arms the timer, emits the notify effect
		t.Fatalf("Fire(proceed): %v", err)
	}
	clk.Advance(11 * time.Second)
	if _, err := h.Tick(ctx); err != nil {
		t.Fatalf("Tick: %v", err)
	}
}

// crashPoint names a seam boundary the capstone drops the runner at, forcing a
// recovery to resume from there.
type crashPoint int

const (
	crashAfterStart    crashPoint = iota // service in flight, nothing settled
	crashAfterService                    // service settled, actor in flight
	crashAfterActor                      // actor settled, before the effect/timer step
	crashBetweenEffect                   // effect step appended but dispatch failed mid-flight
	crashWhilePending                    // timer armed, clock advanced, not yet ticked
)

func (c crashPoint) String() string {
	switch c {
	case crashAfterStart:
		return "after-service-armed"
	case crashAfterService:
		return "after-service-settled"
	case crashAfterActor:
		return "after-actor-settled"
	case crashBetweenEffect:
		return "between-effect-and-dispatch"
	case crashWhilePending:
		return "while-timer-pending"
	default:
		return "unknown"
	}
}

// runCrashMatrixCase drives the pipeline up to crash, drops the Handle, recovers from
// the Store, and finishes the lifetime through a fresh Handle on a divergent wall
// clock — proving the timer fires from its recorded deadline. It returns the final
// recovered snapshot plus the seam counters for exactly-once assertions.
func runCrashMatrixCase(t *testing.T, newStore func() durable.Store, crash crashPoint) ([]byte, *pipelineSeams, int64, int64) {
	t.Helper()
	ctx := context.Background()
	m := pipelineMachine()
	store := newStore()
	id := durable.InstanceID("pipeline-" + crash.String())

	seams := newPipelineSeams()
	var svcCalls, actorRuns int64
	clk := state.NewFakeClock(epoch)

	// For the effect-crash case the live run uses a handler that FAILS on the notify
	// effect, so the proceed step's Record (carrying the effect id) is appended but
	// the effect is never marked dispatched — the write-ahead window recovery heals.
	liveHandler := seams.effectHandler()
	if crash == crashBetweenEffect {
		liveHandler = failingEffectHandler(seams)
	}
	r1 := durable.NewRunner(
		m, store,
		durable.WithRunnerClock[string, string, *pipelineCtx](clk),
		durable.WithServiceRegistry[string, string, *pipelineCtx](pipelineRegistry(pipelineService(&svcCalls))),
		durable.WithActorPalette[string, string, *pipelineCtx](pipelinePalette(&actorRuns)),
		durable.WithEffectHandler[string, string, *pipelineCtx](liveHandler),
		durable.WithCheckpointEvery[string, string, *pipelineCtx](2),
	)
	h1, err := r1.Start(ctx, id, &pipelineCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Drive the live Handle up to the crash boundary, then drop it (simulate a crash).
	if _, err = h1.Fire(ctx, "start"); err != nil {
		t.Fatalf("Fire(start): %v", err)
	}
	if crash >= crashAfterService {
		if _, ok, rerr := h1.RunService(ctx, loadInvokeID()); rerr != nil || !ok {
			t.Fatalf("RunService: ok=%v err=%v", ok, rerr)
		}
	}
	if crash >= crashAfterActor {
		ref, ok := h1.ActorRef(workerActorID())
		if !ok {
			t.Fatalf("no actor ref before crash")
		}
		if delivered, derr := h1.DeliverToActor(ctx, ref, "complete"); derr != nil || !delivered {
			t.Fatalf("DeliverToActor: delivered=%v err=%v", delivered, derr)
		}
	}
	if crash >= crashBetweenEffect {
		// Fire proceed: it arms the timer and emits the notify effect. For the
		// effect-crash case the failing handler surfaces an error AFTER the Record was
		// written, leaving the effect un-marked for recovery to dispatch.
		_, ferr := h1.Fire(ctx, "proceed")
		if crash == crashBetweenEffect {
			if ferr == nil {
				t.Fatalf("Fire(proceed): expected effect dispatch failure, got nil")
			}
		} else if ferr != nil {
			t.Fatalf("Fire(proceed): %v", ferr)
		}
	}
	if crash >= crashWhilePending {
		clk.Advance(11 * time.Second) // timer now due but un-ticked at the crash
	}

	// Recover on a wholly different wall-clock baseline: a timer re-armed from the
	// wall clock would diverge; it must re-arm from the recorded deadline.
	recClock := state.NewFakeClock(epoch.Add(720 * time.Hour))
	h2, err := durable.Recover(
		ctx, m, store, id,
		durable.WithRunnerClock[string, string, *pipelineCtx](recClock),
		durable.WithServiceRegistry[string, string, *pipelineCtx](pipelineRegistry(pipelineService(&svcCalls))),
		durable.WithActorPalette[string, string, *pipelineCtx](pipelinePalette(&actorRuns)),
		durable.WithEffectHandler[string, string, *pipelineCtx](seams.effectHandler()),
		durable.WithCheckpointEvery[string, string, *pipelineCtx](2),
	)
	if err != nil {
		t.Fatalf("Recover (%s): %v", crash, err)
	}

	// Finish whatever seams remain after the crash point, through the recovered Handle.
	finishPipeline(t, ctx, h2, recClock, crash)

	if got := h2.Instance().Current(); got != "done" {
		t.Fatalf("recovered (%s) did not reach done: %q", crash, got)
	}
	b, err := state.MarshalSnapshot(h2.Instance().Snapshot())
	if err != nil {
		t.Fatalf("marshal recovered (%s): %v", crash, err)
	}
	return b, seams, atomic.LoadInt64(&svcCalls), atomic.LoadInt64(&actorRuns)
}

// finishPipeline completes the seams that had not yet run at the crash point, through
// the recovered Handle: the service (if it had not settled), the actor (if it had not
// settled), then the timer fire. The recovered run re-arms the timer from its
// recorded deadline, so a single advance past it ticks it to done.
func finishPipeline(t *testing.T, ctx context.Context, h *durable.Handle[string, string, *pipelineCtx], clk *state.FakeClock, crash crashPoint) {
	t.Helper()
	if crash < crashAfterService {
		if _, ok, err := h.RunService(ctx, loadInvokeID()); err != nil || !ok {
			t.Fatalf("recovered RunService: ok=%v err=%v", ok, err)
		}
	}
	if crash < crashAfterActor {
		ref, ok := h.ActorRef(workerActorID())
		if !ok {
			t.Fatalf("no actor ref after recovery")
		}
		if delivered, err := h.DeliverToActor(ctx, ref, "complete"); err != nil || !delivered {
			t.Fatalf("recovered DeliverToActor: delivered=%v err=%v", delivered, err)
		}
	}
	// Fire proceed unless the live run already did (crashBetweenEffect and later both
	// fired it before the crash; recovery re-dispatched the un-marked effect during
	// Recover, so it is already applied exactly once).
	if crash < crashBetweenEffect {
		if _, err := h.Fire(ctx, "proceed"); err != nil {
			t.Fatalf("recovered Fire(proceed): %v", err)
		}
	}
	// The timer was armed on entry to notifying. The recovery clock is on a fresh
	// baseline, so advance it past the recorded deadline and tick to fire the timer.
	clk.Advance(11 * time.Second)
	if _, err := h.Tick(ctx); err != nil {
		t.Fatalf("recovered Tick: %v", err)
	}
}

// TestIntegration_CrashMatrix_MemStore is the capstone over the in-memory Store: the
// pipeline machine — service + actor + effect + timer — recovers byte-identically to
// a never-crashed reference at every crash point, invoking each seam exactly once.
func TestIntegration_CrashMatrix_MemStore(t *testing.T) {
	newStore := func() durable.Store { return durable.NewMemStore() }
	runCrashMatrix(t, "MemStore", newStore)
}

// TestIntegration_CrashMatrix_FileStore is the same capstone over the on-disk Store,
// proving the seams compose identically against durable-across-restart persistence.
func TestIntegration_CrashMatrix_FileStore(t *testing.T) {
	newStore := func() durable.Store {
		st, err := durable.NewFileStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewFileStore: %v", err)
		}
		return st
	}
	runCrashMatrix(t, "FileStore", newStore)
}

// runCrashMatrix runs the never-crashed reference and every crash-point recovery
// against newStore, asserting byte-identity and exactly-once seam execution for each.
func runCrashMatrix(t *testing.T, label string, newStore func() durable.Store) {
	t.Helper()
	ref := pipelineReference(t, newStore)

	for _, crash := range []crashPoint{crashAfterStart, crashAfterService, crashAfterActor, crashBetweenEffect, crashWhilePending} {
		t.Run(label+"/"+crash.String(), func(t *testing.T) {
			got, seams, svcCalls, actorRuns := runCrashMatrixCase(t, newStore, crash)

			if string(got) != string(ref) {
				t.Fatalf("%s %s: recovered snapshot not byte-identical to reference\n ref: %s\n got: %s",
					label, crash, ref, got)
			}
			if svcCalls != 1 {
				t.Fatalf("%s %s: service invoked %d times, want exactly 1", label, crash, svcCalls)
			}
			if actorRuns != 1 {
				t.Fatalf("%s %s: actor ran %d times, want exactly 1", label, crash, actorRuns)
			}
			if total, maxN := seams.totalEffectApplies(), seams.maxEffectApplies(); total != 1 || maxN != 1 {
				t.Fatalf("%s %s: effect applies total=%d max=%d, want exactly 1 each (exactly-once)",
					label, crash, total, maxN)
			}
		})
	}
}

// TestIntegration_Deterministic confirms two independent recoveries of the same
// crash-injected pipeline run yield byte-identical snapshots, over both stores.
func TestIntegration_Deterministic(t *testing.T) {
	stores := map[string]func() durable.Store{
		"MemStore": func() durable.Store { return durable.NewMemStore() },
		"FileStore": func() durable.Store {
			st, err := durable.NewFileStore(t.TempDir())
			if err != nil {
				t.Fatalf("NewFileStore: %v", err)
			}
			return st
		},
	}
	for label, newStore := range stores {
		t.Run(label, func(t *testing.T) {
			first, _, _, _ := runCrashMatrixCase(t, newStore, crashAfterActor)
			second, _, _, _ := runCrashMatrixCase(t, newStore, crashAfterActor)
			if string(first) != string(second) {
				t.Fatalf("%s: recovery nondeterministic\n first:  %s\n second: %s", label, first, second)
			}
		})
	}
}
