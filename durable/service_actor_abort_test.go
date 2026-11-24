package durable_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests pin the review's High on serviceStep: when a service's settle
// drives an actor whose own settle aborts in the same step, the service's settle
// is journaled before the Handle fails, so the ServiceFn never runs again — only
// the actor re-runs. The parking design's "never runs again" promise holds for a
// result that decoded, not just for one that didn't.

// serviceAbortMachine invokes fetch in first; its onDone enters second, which
// invokes kid (routing childDone) and poker. The poker sends the parent "poke" as
// it starts, and the poke transition finishes kid, so kid settles — and its
// done-data hits the decoder — inside the RunService step that settled fetch.
func serviceAbortMachine(fn state.ServiceFn[outcomeCtx]) *state.Machine[string, string, outcomeCtx] {
	return state.ForgeFor[outcomeCtx]("svcabort").
		Service("fetch", fn).
		State("first").Invoke("fetch", state.WithInvokeOnDone("served")).
		State("second").
		InvokeActor("kid", state.WithInvokeOnDone("childDone")).
		InvokeActor("poker").
		Initial("first").
		Transition("first").On("served").GoTo("second").
		Transition("second").On("poke").GoTo("second").SendTo(serviceAbortKid, "finish").
		Quench()
}

// serviceAbortIDs are the stable ids of serviceAbortMachine's fetch invocation
// and kid invocation.
var (
	serviceAbortFetch = state.InvokeID("svcabort", "first", 0)
	serviceAbortKid   = state.ActorID("svcabort", "second", 0)
)

// poking is an actor that sends its parent "poke" from its initial entry, then
// keeps running. The parent's poke transition finishes kid, so kid settles inside
// the step whose entry spawned the poker. The message it sends the parent is what
// AbsorbRespawn drops on Recover, so the respawned poker doesn't finish kid again:
// only the host's redelivery re-runs kid's settle.
type poking struct{ sent bool }

func (*poking) DeliverFire(context.Context, any) (bool, any) { return false, nil }
func (p *poking) ChildEffects() []state.Effect {
	if p.sent {
		return nil
	}
	p.sent = true
	return []state.Effect{state.SendParent{Event: "poke"}}
}
func (*poking) Output() any { return nil }

// serviceAbortOpts wires the machine's palette and registry, counting fetch's
// ServiceFn runs, with a decoder that refuses kid's childDone done-data.
func serviceAbortOpts(fetchCalls, kidRuns *int64) (state.ServiceFn[outcomeCtx], []durable.Option[string, string, outcomeCtx]) {
	fn := state.ServiceFn[outcomeCtx](func(context.Context, state.ServiceCtx[outcomeCtx]) (any, error) {
		atomic.AddInt64(fetchCalls, 1)
		return "ok", nil
	})
	return fn, []durable.Option[string, string, outcomeCtx]{
		durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{
			"kid":   outputChild(kidRuns, order{ID: 7}),
			"poker": func(map[string]any) (state.ActorInstance, error) { return &poking{}, nil },
		}),
		durable.WithServiceRegistry[string, string, outcomeCtx](state.NewRegistry[outcomeCtx]().Service("fetch", fn)),
		durable.WithEventDataDecoder[string, string, outcomeCtx](func(e string, data []byte) (any, error) {
			if e == "childDone" {
				return nil, errors.New("decoder refused")
			}
			return durable.DecodeAs[any](data)
		}),
	}
}

// TestRunService_AnActorAbortInTheStep_StillRecordsTheServiceResult proves the
// service's settle is journaled before the Handle fails on an actor settle the
// same step drove: across three Recover-and-retry rounds the ServiceFn runs
// exactly once — the first RunService settles fetch and fails on kid's aborted
// settle, and every later RunService finds fetch settled in the journal, so only
// kid re-runs, on each Recover's respawn and each redelivery. On the commit
// before the fix, nothing was recorded for the aborted step, so each Recover left
// fetch in flight and each retry ran the ServiceFn again: three runs for three
// attempts, non-idempotent work repeating.
func TestRunService_AnActorAbortInTheStep_StillRecordsTheServiceResult(t *testing.T) {
	ctx := context.Background()
	var fetchCalls, kidRuns int64
	fn, opts := serviceAbortOpts(&fetchCalls, &kidRuns)
	m := serviceAbortMachine(fn)
	store, id := durable.NewMemStore(), durable.InstanceID("svcabort-1")
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, outcomeCtx{}, state.WithInitialState("first"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Round 1: the settle drives fetch's onDone into second, whose poker finishes
	// kid, whose done-data the decoder refuses. The step fails the Handle with the
	// actor's abort, but the service's own settle — which ran and decoded — is
	// recorded first.
	_, ran, err := h.RunService(ctx, serviceAbortFetch)
	if !ran {
		t.Fatal("RunService reported the service not in flight; want it ran and settled")
	}
	if !errors.Is(err, durable.ErrHandleFailed) || !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("RunService = %v; want ErrHandleFailed wrapping the actor settle's ErrEventDataEncoding", err)
	}
	if got := atomic.LoadInt64(&fetchCalls); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want the one live run", got)
	}
	// The journal holds the service's settle (and the poke fire the settle's absorb
	// drove, which applied), even though the Handle failed.
	svcEntries, actorEntries := 0, 0
	_, recs, lerr := store.Load(ctx, id)
	if lerr != nil {
		t.Fatalf("Load: %v", lerr)
	}
	for _, rec := range recs {
		for _, e := range rec.Entries {
			switch e.Kind {
			case state.JournalServiceResult:
				if e.CorrelationID != serviceAbortFetch {
					t.Fatalf("a service entry for %q, want fetch's", e.CorrelationID)
				}
				svcEntries++
			case state.JournalActorMessage:
				actorEntries++
			}
		}
	}
	if svcEntries != 1 {
		t.Fatalf("the journal holds %d service entries, want the one settle the failed step recorded", svcEntries)
	}
	if actorEntries != 1 {
		t.Fatalf("the journal holds %d actor entries, want the poke fire the settle's absorb drove", actorEntries)
	}

	// Rounds 2 and 3: Recover replays the recorded settle — the parent reaches
	// second and fetch stays settled — so RunService finds nothing in flight and
	// runs no ServiceFn again. Only kid re-runs: the recovered Handle respawns it
	// (the poker's start message is quiet), and a redelivery re-drives the aborted
	// settle.
	for round := 2; round <= 3; round++ {
		h, err = durable.Recover(ctx, m, store, id, opts...)
		if err != nil {
			t.Fatalf("Recover round %d: %v", round, err)
		}
		if got := h.Instance().Current(); got != "second" {
			t.Fatalf("after Recover %d the parent is in %q, want second: the recorded settle replayed", round, got)
		}
		if _, ran, err = h.RunService(ctx, serviceAbortFetch); ran || err != nil {
			t.Fatalf("RunService round %d ran=%v err=%v; want nothing in flight: the journal holds the settle, so the ServiceFn never runs again", round, ran, err)
		}
		if _, derr := h.DeliverToActorByID(ctx, serviceAbortKid, "finish"); !errors.Is(derr, durable.ErrHandleFailed) || !errors.Is(derr, durable.ErrEventDataEncoding) {
			t.Fatalf("the redelivery round %d = %v; want ErrHandleFailed wrapping ErrEventDataEncoding: only the actor re-runs", round, derr)
		}
	}

	// Across the three attempts: one ServiceFn run, and only kid re-ran — once at
	// Start, once per Recover's respawn of the still-running kid.
	if got := atomic.LoadInt64(&fetchCalls); got != 1 {
		t.Fatalf("the ServiceFn ran %d times across three attempts, want 1", got)
	}
	if got := atomic.LoadInt64(&kidRuns); got != 3 {
		t.Fatalf("kid instantiated %d times, want 3: the live spawn and each Recover's respawn of the still-running kid", got)
	}
}
