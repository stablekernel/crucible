package state_test

// Property 2: Parallel random-determinism.
//
// Fire is contracted as a deterministic pure function: the same machine driven
// from the same initial state by the same event sequence must produce the same
// macrostep results every time, with no map-iteration nondeterminism leaking
// into parallel region ordering. This property pins that contract directly by
// running TWO independent instances of the ordering machine (which has a
// parallel state with two regions, "alpha" and "beta") side by side, feeding
// BOTH the SAME seeded random event sequence, and asserting per-step that the
// two instances stay byte-identical across every order-bearing dimension:
// config, ordered effects+payloads, committed state/context, and the trace.
//
// A region-order regression — a map leaking into region iteration so that
// "beta" sometimes broadcasts before "alpha" — would surface here as a
// divergence between the two instances on some seed/step, not as a flake. The
// 50-seed sweep makes residual nondeterminism a reproducible failure.

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestParallelDeterminismProperty drives two independent instances of the
// ordering machine through the same seeded random event sequence and asserts
// they never diverge. Reusing the machine, types, and projection from
// determinism_test.go (same package) keeps the parallel-region ordering under
// test identical to the golden-locked one.
func TestParallelDeterminismProperty(t *testing.T) {
	// All events in the alphabet of the ordering machine. ordKick is omitted
	// deliberately: it is an internal (raised) event, not part of the external
	// driving alphabet.
	alphabet := []ordEvent{ordGo, ordAlphaE, ordBetaE, ordCross, ordTick}

	for seed := int64(1); seed <= 50; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewSource(seed))

			m := forgeOrdering()

			// Two independent instances, same machine, same initial state and
			// the same full-trace projection options.
			inst1 := m.Cast(ordCtx{}, state.WithInitialState[ordState](ordStart), state.WithFullTrace[ordState]())
			inst2 := m.Cast(ordCtx{}, state.WithInitialState[ordState](ordStart), state.WithFullTrace[ordState]())

			const seqLen = 20
			for i := 0; i < seqLen; i++ {
				ev := alphabet[rng.Intn(len(alphabet))]

				res1 := inst1.Fire(context.Background(), ev)
				res2 := inst2.Fire(context.Background(), ev)

				proj1 := projectOrder(res1, inst1.Entity())
				proj2 := projectOrder(res2, inst2.Entity())

				if !reflect.DeepEqual(proj1, proj2) {
					t.Fatalf("seed %d step %d (event %v): instances diverged\n  inst1: %+v\n  inst2: %+v",
						seed, i, ev, proj1, proj2)
				}

				// The committed live state must also stay in lockstep; the
				// projection captures the trace's From/To but the instance's
				// Current() is the authoritative committed config.
				if inst1.Current() != inst2.Current() {
					t.Fatalf("seed %d step %d (event %v): committed state diverged: inst1=%v inst2=%v",
						seed, i, ev, inst1.Current(), inst2.Current())
				}
			}
		})
	}
}
