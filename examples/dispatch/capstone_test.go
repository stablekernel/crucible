package dispatch

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/telemetry"
)

// TestCapstone_OrderSagaProvenDurableDistributedPolyglotObserved is the flagship
// narrative of the Crucible showcase: it runs the WHOLE story over the single
// food-delivery order machine, in sequence, asserting each capability's headline
// result. The same proven order saga is shown to run:
//
//	proven       — Prove establishes the machine is well-formed (every key stage
//	               reachable, the Watchdog leaves mutually exclusive, no dead guard);
//	durable      — RunCrashRecovery drives it under the durable runtime, crashes the
//	               process, and reconstructs it from the store alone, then on to
//	               Delivered;
//	distributed  — RunDistributedFulfillment hosts the kitchen and courier as remote
//	               cluster actors over real gRPC, restarts a crashed worker actor, and
//	               drives both to completion across the wire;
//	polyglot     — RunPolyglotEquivalence proves the generous-order guard decides
//	               identically in CEL and in a WebAssembly guest;
//	observed     — RunObservedSaga drives it to Delivered while emitting a span and a
//	               metric per transition through the vendor-neutral telemetry seam.
//
// It is a Test rather than an Example because the distributed (gRPC) and polyglot
// (WASM compile) stages make raw output ordering nondeterministic; the assertions
// below pin each stage's headline result deterministically and keep CI fast.
func TestCapstone_OrderSagaProvenDurableDistributedPolyglotObserved(t *testing.T) {
	ctx := context.Background()

	// (1) Proven — the order machine is well-formed before any order is dispatched.
	model, err := fooddelivery.NewModel()
	if err != nil {
		t.Fatalf("capstone: build model: %v", err)
	}
	proof, err := Prove(model)
	if err != nil {
		t.Fatalf("capstone: prove: %v", err)
	}
	if !proof.Sound() {
		t.Fatalf("capstone: order saga is not sound: %+v", proof)
	}

	// (2) Durable — the proven saga survives a process crash, reconstructs from the
	// store alone, and drives on to Delivered.
	recovery, err := RunCrashRecovery(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("capstone: crash recovery: %v", err)
	}
	if got := recovery.RecoveredConfig; len(got) != 2 ||
		got[0] != fooddelivery.Cooking || got[1] != fooddelivery.OnTime {
		t.Fatalf("capstone: recovered config = %v, want [Cooking OnTime]", got)
	}
	if got := recovery.FinalConfig; len(got) != 1 || got[0] != fooddelivery.Delivered {
		t.Fatalf("capstone: final config = %v, want [Delivered]", got)
	}

	// (3) Distributed — the same fulfillment actors run as remote cluster actors over
	// gRPC, a crashed worker actor is supervised back to life, and both are driven to
	// completion across the wire.
	dist, err := RunDistributedFulfillment(ctx)
	if err != nil {
		t.Fatalf("capstone: distributed fulfillment: %v", err)
	}
	if dist.SupervisorDecision != cluster.Restart {
		t.Fatalf("capstone: supervisor decision = %v, want Restart", dist.SupervisorDecision)
	}
	if dist.Restarts != 1 {
		t.Fatalf("capstone: restarts = %d, want 1", dist.Restarts)
	}
	if dist.Delivered != 2 {
		t.Fatalf("capstone: signals delivered over the wire = %d, want 2", dist.Delivered)
	}
	if len(dist.Spawned) != 2 {
		t.Fatalf("capstone: remote actors spawned = %d, want 2", len(dist.Spawned))
	}

	// (4) Polyglot — the generous-order guard decides identically in CEL and WASM.
	poly, err := RunPolyglotEquivalence(ctx, buildGenerousGuest(t))
	if err != nil {
		t.Fatalf("capstone: polyglot equivalence: %v", err)
	}
	if !poly.Equivalent {
		t.Fatalf("capstone: CEL and WASM guards not equivalent: %+v", poly)
	}

	// (5) Observed — the saga drives to Delivered while emitting a span and a metric
	// per transition through the telemetry seam.
	observed, err := RunObservedSaga(ctx, telemetry.Nop())
	if err != nil {
		t.Fatalf("capstone: observed saga: %v", err)
	}
	if observed.FinalStage != fooddelivery.Delivered {
		t.Fatalf("capstone: observed final stage = %v, want Delivered", observed.FinalStage)
	}
	if observed.Transitions != 3 {
		t.Fatalf("capstone: observed transitions = %d, want 3", observed.Transitions)
	}

	// The single coherent story: one order machine, proven sound, run durably across a
	// crash, distributed over gRPC, decided polyglot, and observed to Delivered.
	t.Logf("capstone: order saga proven (sound=%t), durable (recovered %v → %v), "+
		"distributed (%d actors, %d wire signals, decision=%v), polyglot (equivalent=%t), "+
		"observed (%d transitions → %v)",
		proof.Sound(), recovery.RecoveredConfig, recovery.FinalConfig,
		len(dist.Spawned), dist.Delivered, dist.SupervisorDecision,
		poly.Equivalent, observed.Transitions, observed.FinalStage)
}
