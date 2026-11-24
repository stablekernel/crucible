package durable_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests pin what an aborted settle does to the ends that drive it, as the
// review's runner findings asked: Start's baseline is already written when its
// spawns' settle aborts, so a retry Start can't be a clean start; and a Fire
// whose own transition spawned the aborted actor records nothing, so a Recover
// restores the pre-event state and never respawns that actor.

// TestStart_AnAbortedSettleLeavesTheBaselineForRecover pins Start's real
// behavior: when a settle Start's spawns drive aborts, the baseline checkpoint is
// already written, so a retry Start on the id returns ErrInstanceExists — the
// retry is a Recover from the baseline, which re-runs the spawns and aborts the
// same way, until the decoder is fixed.
func TestStart_AnAbortedSettleLeavesTheBaselineForRecover(t *testing.T) {
	ctx := context.Background()
	kid := state.ActorID("startsettle", "K", 0)
	palette := func(runs *int64) durable.Option[string, string, outcomeCtx] {
		return durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{
			"kid": outputChild(runs, order{ID: 7}),
			"finisher": func(map[string]any) (state.ActorInstance, error) {
				return &poker{target: kid}, nil
			},
		})
	}
	failing := childDoneDecoder(func([]byte) (any, error) { return nil, errors.New("decoder refused") })
	var runs int64
	store := durable.NewMemStore()
	id := durable.InstanceID("start-abort-retry")
	if _, err := durable.NewRunner(startSettleMachine(), store, palette(&runs), failing).
		Start(ctx, id, outcomeCtx{}, state.WithInitialState("both")); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Start with a failing decoder = %v, want the undecodable done-data's error", err)
	}
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("kid instantiated %d times, want the live start's once", got)
	}
	// The baseline checkpoint is written, so a retry Start is refused: the retry is
	// a Recover.
	if _, err := durable.NewRunner(startSettleMachine(), store, palette(&runs), failing).
		Start(ctx, id, outcomeCtx{}, state.WithInitialState("both")); !errors.Is(err, durable.ErrInstanceExists) {
		t.Fatalf("the retry Start = %v, want ErrInstanceExists: the baseline is already written", err)
	}
	// A Recover from the baseline re-runs the spawns, whose settle aborts the same
	// way: the actors ran again, and no Handle comes back until the decoder decodes.
	if _, err := durable.Recover(ctx, startSettleMachine(), store, id, palette(&runs), failing); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover from the baseline with a failing decoder = %v, want the same abort, from the re-run spawns", err)
	}
	if got := atomic.LoadInt64(&runs); got != 2 {
		t.Fatalf("kid instantiated %d times, want 2: the Recover re-ran the spawns", got)
	}
	// With the decoder fixed, that same Recover returns a Handle and settles kid.
	h, err := durable.Recover(ctx, startSettleMachine(), store, id, palette(&runs), endsDecoder())
	if err != nil {
		t.Fatalf("Recover with the fixed decoder: %v", err)
	}
	if notes := h.Instance().Entity().Notes; len(notes) != 1 || notes[0] != wantEndsNote {
		t.Fatalf("the recovered notes = %q, want the settled kid's [%s]", notes, wantEndsNote)
	}
}

// selfFinisher is an actor that finishes itself as it spawns — the SendTo its
// initial entry emits targets the actor's own id, which the spawn passed as its
// input — and completes with a fixed output.
type selfFinisher struct {
	self   string
	output any
	sent   bool
}

func (s *selfFinisher) DeliverFire(_ context.Context, ev any) (bool, any) {
	if ev == "finish" {
		return true, s.output
	}
	return false, nil
}

func (s *selfFinisher) ChildEffects() []state.Effect {
	if s.sent {
		return nil
	}
	s.sent = true
	return []state.Effect{state.SendTo{TargetID: s.self, Event: "finish"}}
}
func (s *selfFinisher) Output() any { return s.output }

// TestFire_AnAbortedSettleOfTheFiresOwnSpawnIsNotRespawned pins the Fire end of
// the abort rule: a Fire whose transition dynamically spawned an actor that
// settles at once with an outcome that won't record fails the Handle and records
// nothing, so a Recover restores the state before the event — which doesn't
// declare the spawn — and the actor isn't respawned: the host re-fires the event,
// and only then does the actor run again. The error carries the actor's own
// wording: its done-data won't encode.
func TestFire_AnAbortedSettleOfTheFiresOwnSpawnIsNotRespawned(t *testing.T) {
	ctx := context.Background()
	const kidID = "firespawn-kid"
	m := state.ForgeFor[outcomeCtx]("firespawn").
		Reducer("recordDone", func(in state.AssignCtx[outcomeCtx]) outcomeCtx {
			c := in.Entity
			c.Notes = append(c.Notes, strings.TrimSpace(in.Event.(string)+" done"))
			return c
		}).
		State("idle").
		State("spawning").
		Initial("idle").
		Transition("idle").On("go").GoTo("spawning").
		Spawn("selfish", kidID,
			state.WithSpawnInput(map[string]any{"self": kidID}),
			state.WithSpawnOnDone("childDone")).
		Transition("spawning").On("childDone").GoTo("spawning").Assign("recordDone").
		Quench()
	var runs int64
	palette := durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{
		"selfish": func(input map[string]any) (state.ActorInstance, error) {
			runs++
			self, _ := input["self"].(string)
			return &selfFinisher{self: self, output: make(chan int)}, nil
		},
	})
	store, id := durable.NewMemStore(), durable.InstanceID("firespawn-1")
	h, err := durable.NewRunner(m, store, palette).Start(ctx, id, outcomeCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// The Fire spawns the self-finishing actor, whose chan output can't record: the
	// settle aborts, the Handle fails, and nothing is journaled for the step.
	_, ferr := h.Fire(ctx, "go")
	if !errors.Is(ferr, durable.ErrHandleFailed) {
		t.Fatalf("Fire go = %v, want ErrHandleFailed: the settle aborted", ferr)
	}
	if !strings.Contains(ferr.Error(), "actor done-data won't encode") {
		t.Fatalf("Fire go = %v, want the actor's own wording: the actor done-data won't encode", ferr)
	}
	if _, recs, lerr := store.Load(ctx, id); lerr != nil || len(recs) != 0 {
		t.Fatalf("the journal holds %d Records (err %v), want none: the aborted step recorded nothing", len(recs), lerr)
	}

	// A Recover restores the pre-event state, which declares no spawn: the actor
	// isn't respawned, the Handle doesn't run it, and re-firing the event is what
	// runs it again.
	rh, rerr := durable.Recover(ctx, m, store, id, palette)
	if rerr != nil {
		t.Fatalf("Recover: %v", rerr)
	}
	if got := rh.Instance().Current(); got != "idle" {
		t.Fatalf("after Recover the parent is in %q, want idle: the Fire recorded nothing", got)
	}
	if _, ok := rh.ActorRef(kidID); ok {
		t.Fatal("the recovered Handle runs the Fire's own spawn, want it not respawned: the pre-event state declares it")
	}
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("the actor ran %d times, want the live Fire's once: no replay re-ran it", got)
	}
}
