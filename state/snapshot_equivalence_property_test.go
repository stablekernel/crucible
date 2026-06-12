package state_test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file property-checks the snapshot equivalence contract: an instance, after
// being snapshotted, marshaled, unmarshaled and restored, must be observationally
// indistinguishable from the original. "Indistinguishable" means the restored
// instance carries the same context, current leaf and active configuration, and —
// critically — drives forward identically: firing the same event sequence into both
// the original and the restored instance yields step-for-step identical projections
// (new state, ordered effects, the order-bearing trace cascade, and the committed
// context). A divergence here means a snapshot lost or reordered hidden runtime
// state, which would make a journal/replay restore unsafe.

// eqState is the state alphabet of the equivalence machine.
type eqState int

const (
	eqIdle eqState = iota
	eqBoot
	eqWork
	eqAlpha
	eqBeta
	eqDone
	eqFail
)

func (s eqState) String() string {
	switch s {
	case eqIdle:
		return "Idle"
	case eqBoot:
		return "Boot"
	case eqWork:
		return "Work"
	case eqAlpha:
		return "Alpha"
	case eqBeta:
		return "Beta"
	case eqDone:
		return "Done"
	case eqFail:
		return "Fail"
	default:
		return "eqState?"
	}
}

// eqEvent is the event alphabet of the equivalence machine.
type eqEvent int

const (
	eqStart eqEvent = iota
	eqA
	eqB
	eqFinish
	eqReset
)

func (e eqEvent) String() string {
	switch e {
	case eqStart:
		return "Start"
	case eqA:
		return "A"
	case eqB:
		return "B"
	case eqFinish:
		return "Finish"
	case eqReset:
		return "Reset"
	default:
		return "eqEvent?"
	}
}

// eqCtx is a value context whose fields are all JSON-serializable, so it survives
// the default snapshot codec (json.Marshal/json.Unmarshal) without a custom option.
type eqCtx struct {
	Count  int      `json:"count"`
	Log    []string `json:"log"`
	Active bool     `json:"active"`
}

// forgeEquivalence builds a compact machine that exercises the dimensions a
// snapshot must preserve: a compound (super)state with hierarchy, an entry/exit
// assign on the superstate, an eventless ("always") step, a guarded transition, a
// transition-scoped action+assign, and a Raise of an internal event. None of these
// use parallel regions, so the snapshot/codec round-trip stays clean and the
// forward-drive comparison stays deterministic.
func forgeEquivalence() *state.Machine[eqState, eqEvent, eqCtx] {
	return state.Forge[eqState, eqEvent, eqCtx]("eq-machine").
		Action("tag", func(state.ActionCtx[eqCtx]) (state.Effect, error) { return "tagged", nil }).
		Reducer("activate", func(in state.AssignCtx[eqCtx]) eqCtx {
			c := in.Entity
			c.Active = true
			return c
		}).
		Reducer("deactivate", func(in state.AssignCtx[eqCtx]) eqCtx {
			c := in.Entity
			c.Active = false
			return c
		}).
		Reducer("inc", func(in state.AssignCtx[eqCtx]) eqCtx {
			c := in.Entity
			c.Count++
			c.Log = append(append([]string(nil), c.Log...), fmt.Sprintf("inc%d", c.Count))
			return c
		}).
		Guard("isActive", func(g state.GuardCtx[eqCtx]) bool { return g.Entity.Active }).

		// Idle: Start activates the context, raises an internal A, and moves to Boot.
		State(eqIdle).
		Transition(eqIdle).On(eqStart).GoTo(eqBoot).Assign("activate").Raise(eqA).

		// Boot: entry effect, then an eventless step into the Work superstate.
		State(eqBoot).
		OnEntry("tag", state.P{}).
		Always().GoTo(eqWork).

		// Work: compound; entry folds inc, exit folds deactivate. Finish/Reset are
		// cross-cutting transitions on Work.
		SuperState(eqWork).
		Initial(eqAlpha).
		OnEntryAssign("inc").
		OnExitAssign("deactivate").
		Transition(eqWork).On(eqFinish).GoTo(eqDone).
		Transition(eqWork).On(eqReset).GoTo(eqIdle).

		// Alpha: A self-transitions emitting tag and folding inc; B moves to Beta only
		// while the context is active (guarded).
		SubState(eqAlpha).
		On(eqA).GoTo(eqAlpha).Do("tag", state.P{}).Assign("inc").
		On(eqB).GoTo(eqBeta).WhenExpr(state.Guard[eqState]("isActive")).

		// Beta: A returns to Alpha; Finish lands in Done (also covered by Work).
		SubState(eqBeta).
		On(eqA).GoTo(eqAlpha).
		On(eqFinish).GoTo(eqDone).
		EndSuperState().
		State(eqDone).
		OnEntry("tag", state.P{}).
		Initial(eqIdle).
		CurrentStateFn(func(eqCtx) eqState { return eqIdle }).
		Quench()
}

// equivTrace is the deterministic, order-bearing projection of a macrostep plus the
// committed context. Two instances that are observationally equivalent produce
// equal equivTrace values for the same event.
type equivTrace struct {
	NewState       string
	Effects        []string
	Err            string
	ExitedStates   []string
	EnteredStates  []string
	EffectsEmitted []string
	AssignsApplied []string
	Microsteps     []string
	ContextCount   int
	ContextActive  bool
	ContextLog     []string
}

func projectEquiv(res state.FireResult[eqState], ctx eqCtx) equivTrace {
	effects := make([]string, 0, len(res.Effects))
	for _, e := range res.Effects {
		if s, ok := e.(string); ok {
			effects = append(effects, s)
		}
	}
	errStr := ""
	if res.Err != nil {
		errStr = res.Err.Error()
	}
	return equivTrace{
		NewState:       res.NewState.String(),
		Effects:        effects,
		Err:            errStr,
		ExitedStates:   res.Trace.ExitedStates,
		EnteredStates:  res.Trace.EnteredStates,
		EffectsEmitted: res.Trace.EffectsEmitted,
		AssignsApplied: res.Trace.AssignsApplied,
		Microsteps:     res.Trace.Microsteps,
		ContextCount:   ctx.Count,
		ContextActive:  ctx.Active,
		ContextLog:     ctx.Log,
	}
}

// TestSnapshotEquivalenceProperty drives many random pre-histories, snapshots and
// round-trips each through the codec, restores, and then asserts the restored
// instance is indistinguishable from the original under the same forward drive.
func TestSnapshotEquivalenceProperty(t *testing.T) {
	m := forgeEquivalence()

	// Event alphabet for random sequences.
	alphabet := []eqEvent{eqStart, eqA, eqB, eqFinish, eqReset}

	for seed := int64(1); seed <= 50; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))

			// Instance A: build a random pre-history of preN events.
			instA := m.Cast(eqCtx{}, state.WithInitialState[eqState](eqIdle), state.WithFullTrace[eqState]())

			const preN = 15
			for i := 0; i < preN; i++ {
				ev := alphabet[rng.Intn(len(alphabet))]
				// Invalid transitions are simply no-ops/errors; the contract is about
				// equivalence, not about every event being accepted.
				_ = instA.Fire(context.Background(), ev)
			}

			// Snapshot A, marshal, unmarshal, restore as B.
			snapA := instA.Snapshot()
			data, err := state.MarshalSnapshot[eqState, eqEvent, eqCtx](snapA)
			if err != nil {
				t.Fatalf("MarshalSnapshot seed %d: %v", seed, err)
			}

			snapB, err := state.UnmarshalSnapshot[eqState, eqEvent, eqCtx](data)
			if err != nil {
				t.Fatalf("UnmarshalSnapshot seed %d: %v", seed, err)
			}

			instB, err := m.Restore(snapB, state.WithRestoreFullTrace[eqState]())
			if err != nil {
				t.Fatalf("Restore seed %d: %v", seed, err)
			}

			// Post-restore identity: context, current leaf and active configuration.
			if !reflect.DeepEqual(instA.Entity(), instB.Entity()) {
				t.Fatalf("context mismatch after restore seed %d:\n  A=%#v\n  B=%#v",
					seed, instA.Entity(), instB.Entity())
			}
			if instA.Current() != instB.Current() {
				t.Fatalf("current state mismatch after restore seed %d: A=%v B=%v",
					seed, instA.Current(), instB.Current())
			}
			if !reflect.DeepEqual(instA.Configuration(), instB.Configuration()) {
				t.Fatalf("configuration mismatch after restore seed %d:\n  A=%v\n  B=%v",
					seed, instA.Configuration(), instB.Configuration())
			}

			// Drain any effects the snapshot captured as pending on the restored side.
			_ = instB.ResumeEffects()

			// Forward drive: fire the SAME postN events into both instances (the rng is
			// shared, so both observe identical events) and assert step-for-step parity.
			const postN = 15
			for i := 0; i < postN; i++ {
				ev := alphabet[rng.Intn(len(alphabet))]

				resA := instA.Fire(context.Background(), ev)
				resB := instB.Fire(context.Background(), ev)

				projA := projectEquiv(resA, instA.Entity())
				projB := projectEquiv(resB, instB.Entity())

				if !reflect.DeepEqual(projA, projB) {
					t.Fatalf("step %d (event %v) diverged, seed %d:\n  A=%#v\n  B=%#v",
						i, ev, seed, projA, projB)
				}
			}
		})
	}
}
