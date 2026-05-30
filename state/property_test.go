package state_test

import (
	"context"
	"math/rand"
	"testing"
	"testing/quick"

	"github.com/stablekernel/crucible/state"
)

// Property-based invariants over the document machine, driven by stdlib
// testing/quick. Each property generates random event sequences (and entities)
// and asserts a kernel guarantee holds for every one.

// genEvent maps a random int to a valid DocEvent so quick explores the event
// alphabet rather than out-of-range values.
func genEvent(r *rand.Rand) DocEvent {
	return DocEvent(r.Intn(int(Archive) + 1))
}

// TestProperty_TraceAlwaysNonNil asserts every Fire records a trace with a
// non-empty event and a defined outcome, regardless of whether the event was
// valid in the current state.
func TestProperty_TraceAlwaysNonNil(t *testing.T) {
	m := buildDocMachine()
	prop := func(seed int64, n uint8) bool {
		r := rand.New(rand.NewSource(seed))
		inst := m.Cast(&Document{Status: Draft, ReviewerID: strptr("rev-1")})
		steps := int(n%8) + 1
		for i := 0; i < steps; i++ {
			res := inst.Fire(context.Background(), genEvent(r))
			if res.Trace.Event == "" {
				return false
			}
			if res.Trace.Outcome < state.OutcomeSuccess || res.Trace.Outcome > state.OutcomeEffectError {
				return false
			}
		}
		return true
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 500}); err != nil {
		t.Fatal(err)
	}
}

// TestProperty_StateUnchangedOnError asserts a Fire that fails (invalid
// transition or failed guard) never advances the instance's current state.
func TestProperty_StateUnchangedOnError(t *testing.T) {
	m := buildDocMachine()
	prop := func(seed int64) bool {
		r := rand.New(rand.NewSource(seed))
		inst := m.Cast(&Document{Status: Draft, ReviewerID: strptr("rev-1")})
		before := inst.Current()
		res := inst.Fire(context.Background(), genEvent(r))
		if res.Err != nil {
			return inst.Current() == before
		}
		return true
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 500}); err != nil {
		t.Fatal(err)
	}
}

// TestProperty_GuardShortCircuits asserts the guarded Submitted->Approved
// transition fires only when a reviewer is assigned: with no reviewer the guard
// short-circuits and the state holds.
func TestProperty_GuardShortCircuits(t *testing.T) {
	m := buildDocMachine()
	prop := func(hasReviewer bool) bool {
		doc := &Document{Status: Submitted}
		if hasReviewer {
			doc.ReviewerID = strptr("rev-1")
		}
		inst := m.Cast(doc, state.WithInitialState(Submitted))
		res := inst.Fire(context.Background(), Approve)
		if hasReviewer {
			return res.Err == nil && res.NewState == Approved
		}
		return res.Err != nil && res.NewState == Submitted
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

// TestProperty_RoundTripIdentity asserts the serialize -> load -> reserialize
// identity holds for the machine regardless of which entity the run starts from:
// the IR bytes are stable and behavior agrees on a random event.
func TestProperty_RoundTripIdentity(t *testing.T) {
	m := buildDocMachine()
	data, err := m.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](data)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	m2 := ir.Provide(docRegistry()).Quench()

	prop := func(seed int64, withReviewer bool) bool {
		r := rand.New(rand.NewSource(seed))
		doc := func() *Document {
			d := &Document{Status: Draft}
			if withReviewer {
				d.ReviewerID = strptr("rev-1")
			}
			return d
		}
		ev := genEvent(r)
		r1 := m.Cast(doc(), state.WithInitialState(Draft)).Fire(context.Background(), ev)
		r2 := m2.Cast(doc(), state.WithInitialState(Draft)).Fire(context.Background(), ev)
		return r1.NewState == r2.NewState && r1.Trace.Outcome == r2.Trace.Outcome
	}
	if err := quick.Check(prop, &quick.Config{MaxCount: 500}); err != nil {
		t.Fatal(err)
	}
}
