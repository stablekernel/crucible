package dispatch

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/transport"
)

// TestRunDistributedFulfillment_SpawnDeliverAndSupervisedRestart drives the full
// distributed flow end to end over real gRPC (carried in-memory by bufconn): the
// coordinator spawns the kitchen on worker-a and the courier on worker-b, drives each
// across the wire, the worker-a supervisor restarts the crashed kitchen actor, and the
// coordinator drives the restarted actor again — then asserts the report reflects every
// observed fact.
func TestRunDistributedFulfillment_SpawnDeliverAndSupervisedRestart(t *testing.T) {
	report, err := RunDistributedFulfillment(context.Background())
	if err != nil {
		t.Fatalf("RunDistributedFulfillment: %v", err)
	}

	if report.Coordinator != "coordinator" {
		t.Errorf("Coordinator = %q, want coordinator", report.Coordinator)
	}
	if got := report.Workers; len(got) != 2 || got[0] != "worker-a" || got[1] != "worker-b" {
		t.Errorf("Workers = %v, want [worker-a worker-b]", got)
	}

	// The kitchen landed on worker-a, the courier on worker-b — location-transparent
	// placement the coordinator addressed only by ref.
	if len(report.Spawned) != 2 {
		t.Fatalf("Spawned = %v, want 2 actors", report.Spawned)
	}
	wantSpawn := map[string]SpawnedActor{
		"kitchen": {Src: "kitchen", ID: crashKitchen, Node: "worker-a"},
		"courier": {Src: "courier", ID: "courier-1", Node: "worker-b"},
	}
	for _, got := range report.Spawned {
		if want := wantSpawn[got.Src]; got != want {
			t.Errorf("spawned %s = %+v, want %+v", got.Src, got, want)
		}
	}

	// Two signals crossed the wire after the supervised restart: the kitchen drive to the
	// restarted actor and the courier drive.
	if report.Delivered != 2 {
		t.Errorf("Delivered = %d, want 2 (restarted kitchen + courier)", report.Delivered)
	}

	// The worker supervisor met the injected crash with a single Restart of the kitchen.
	if report.SupervisorDecision != cluster.Restart {
		t.Errorf("SupervisorDecision = %v, want Restart", report.SupervisorDecision)
	}
	if report.Restarts != 1 {
		t.Errorf("Restarts = %d, want 1", report.Restarts)
	}
	if report.RestartedActor != crashKitchen {
		t.Errorf("RestartedActor = %q, want %q", report.RestartedActor, crashKitchen)
	}
}

// TestRunDistributed_NoCrashEscalation exercises the recordSupervision error branch:
// when the injector reports the actor was crashed but the supervisor (in this contrived
// wiring) handled no escalation, the run fails loudly. Here the injector is a no-op that
// claims success, so the supervisor sees nothing.
func TestRunDistributed_NoCrashEscalation(t *testing.T) {
	_, err := runDistributed(context.Background(),
		func(context.Context, func(context.Context, error) bool) error { return nil })
	if err == nil {
		t.Fatal("runDistributed with no crash = nil error, want a no-escalation error")
	}
}

// TestRunDistributed_CrashInjectorError surfaces a crash-injector failure back to the
// caller, covering the crash error branch.
func TestRunDistributed_CrashInjectorError(t *testing.T) {
	wantErr := errors.New("injector boom")
	_, err := runDistributed(context.Background(),
		func(context.Context, func(context.Context, error) bool) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("runDistributed crash error = %v, want %v", err, wantErr)
	}
}

// TestSpawnRemote_UnreachableNode exercises the spawnRemote error branch: a coordinator
// whose transport knows no worker cannot place an actor, and the failure is reported with
// context rather than swallowed.
func TestSpawnRemote_UnreachableNode(t *testing.T) {
	coord := coordinatorNode("coordinator", transport.New())
	var report DistributedReport
	if _, err := spawnRemote(context.Background(), coord, "ghost", "kitchen", crashKitchen, &report); err == nil {
		t.Fatal("spawnRemote to unreachable node = nil error, want a wire error")
	}
}

// TestDeliverRemote_UnreachableNode exercises the deliverRemote error branch: a delivery
// to a node the transport cannot reach fails with context.
func TestDeliverRemote_UnreachableNode(t *testing.T) {
	coord := coordinatorNode("coordinator", transport.New())
	var report DistributedReport
	ref := state.ActorRef{ID: "x", Node: "ghost"}
	if err := deliverRemote(context.Background(), coord, ref, "sig", &report); err == nil {
		t.Fatal("deliverRemote to unreachable node = nil error, want a wire error")
	}
}

// TestRecordSupervision_WrongDecision covers the non-Restart decision branch of
// recordSupervision directly: an Escalate decision must be rejected.
func TestRecordSupervision_WrongDecision(t *testing.T) {
	sup := cluster.NewSupervisor(cluster.WithDecision("kitchen", cluster.Escalate))
	sup.Handle(context.Background(), &state.ActorEscalation{Src: "kitchen", ActorID: crashKitchen, Err: errors.New("x")})
	var report DistributedReport
	if err := recordSupervision(sup, &report); err == nil {
		t.Fatal("recordSupervision with Escalate decision = nil error, want rejection")
	}
}

// TestOvenFireCrash_NoActor covers ovenFireCrash's not-running branch: when the inject
// callback reports no actor was running, the canonical injector returns a clear error.
func TestOvenFireCrash_NoActor(t *testing.T) {
	err := ovenFireCrash(context.Background(),
		func(context.Context, error) bool { return false })
	if err == nil {
		t.Fatal("ovenFireCrash with no running actor = nil error, want an error")
	}
}

// TestOvenFireCrash_Crashes covers ovenFireCrash's success branch and confirms it
// forwards a non-nil failure to the inject callback.
func TestOvenFireCrash_Crashes(t *testing.T) {
	var gotErr error
	err := ovenFireCrash(context.Background(),
		func(_ context.Context, e error) bool { gotErr = e; return true })
	if err != nil {
		t.Fatalf("ovenFireCrash crashing a running actor = %v, want nil", err)
	}
	if gotErr == nil {
		t.Fatal("ovenFireCrash forwarded a nil failure, want a non-nil oven-fire error")
	}
}

// TestDeliverRemote_ActorNotRunning covers deliverRemote's not-running branch over a real
// served worker: once the kitchen actor has run to its plated final state, a further
// delivery finds no actor to receive the signal and reports it as an error.
func TestDeliverRemote_ActorNotRunning(t *testing.T) {
	ctx := context.Background()
	worker, _ := workerNode("worker-a", "kitchen", fooddelivery.KitchenBehavior(), fooddelivery.KitchenCook)
	closeW, conn, err := serve(worker)
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	defer closeW()

	tr := transport.New()
	tr.AddNode("worker-a", conn)
	coord := coordinatorNode("coordinator", tr)

	var report DistributedReport
	ref, err := spawnRemote(ctx, coord, "worker-a", "kitchen", crashKitchen, &report)
	if err != nil {
		t.Fatalf("spawnRemote: %v", err)
	}
	// First delivery drives the kitchen to its plated final state, completing the actor.
	if err = deliverRemote(ctx, coord, ref, fooddelivery.KitchenCook, &report); err != nil {
		t.Fatalf("first deliverRemote: %v", err)
	}
	// Second delivery finds no running actor: deliverRemote reports it as an error.
	if err = deliverRemote(ctx, coord, ref, fooddelivery.KitchenCook, &report); err == nil {
		t.Fatal("deliverRemote to a finished actor = nil error, want a not-running error")
	}
}

// TestServe_Closer confirms serve returns a usable connection and a closer that tears
// down the server and connection without panicking — the cleanup path the harness
// relies on.
func TestServe_Closer(t *testing.T) {
	sys, _ := workerNode("worker-x", "kitchen", fooddelivery.KitchenBehavior(), fooddelivery.KitchenCook)
	closer, conn, err := serve(sys)
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	if conn == nil {
		t.Fatal("serve returned a nil connection")
	}
	closer() // must not panic
}
