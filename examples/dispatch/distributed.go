package dispatch

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// This file is the showcase's distributed-execution capability: it takes the same
// proven kitchen and courier fulfillment behaviors the durable runtime hosts as
// in-process actors of one order instance — [fooddelivery.KitchenBehavior] and
// [fooddelivery.CourierBehavior] — and hosts them instead as REMOTE cluster actors on
// separate worker nodes, driven by a coordinator node over real gRPC. It proves the
// fulfillment actors are location-transparent: the coordinator spawns them on a worker,
// delivers their [fooddelivery.KitchenCook] / [fooddelivery.CourierDrive] signals across
// the wire by opaque ref, and a worker-local supervisor restarts a crashed actor so
// dispatch continues — all without the coordinator knowing or caring where the actors
// run.
//
// The transport is real gRPC; the demonstration carries it over an in-memory
// [bufconn.Listener] so the whole cluster stands up inside one process (and inside an
// Example) without binding a TCP port. Every Spawn and Deliver the coordinator issues is
// encoded, sent over the gRPC connection, decoded on the worker into the worker's
// concrete event type, and applied to the worker-hosted actor — the same path a
// cross-machine deployment would take.
//
// One detail makes the wire path honest. A worker decodes each delivered signal into its
// own host machine's event type before handing it to the hosted actor, and the kitchen
// and courier actors advance on distinct, unexported signal types. So each worker node is
// typed to the signal of the actor it hosts — its event type is inferred from the exported
// signal constant ([workerNode] is generic over it) — and the coordinator, which only
// marshals the raw signal it is handed, can drive both workers regardless of its own type.

// crashKitchen names the worker-hosted kitchen actor the demonstration fails to exercise
// supervised restart. It is the actor id the coordinator spawns the kitchen under, so the
// demonstration (and a test) can address it on the worker to inject the crash.
const crashKitchen = "kitchen-1"

// courierActor names the worker-hosted courier actor the coordinator spawns and drives.
const courierActor = "courier-1"

// host is the trivial parent entity a node's ActorSystem requires. The kitchen and
// courier behaviors are parent-agnostic, so they register into any node's ActorSystem
// regardless of this host type; it exists only to give each node a well-formed parent
// machine to hang its actors off.
type host struct{}

// workerNode forges a worker node that hosts a single fulfillment behavior under src,
// driven by signals of type E. E is inferred from the trailing drive-signal argument — the
// exported signal constant the actor advances on (for example [fooddelivery.KitchenCook]) —
// so the node's host machine decodes wire-delivered signals into the exact type the hosted
// actor expects. The kitchen and courier use distinct, unexported signal types, so each
// worker is typed to its own through this inference without the type ever being named. It
// returns the cluster System and the underlying ActorSystem (the latter is where a
// supervisor wires its escalation handler and where a crash is injected).
func workerNode[E comparable](
	name, src string,
	behavior state.ActorBehavior,
	_ E,
) (*cluster.System[string, E, *host], *state.ActorSystem[string, E, *host]) {
	parent := state.Forge[string, E, *host]("worker").
		State("idle").Initial("idle").Quench().
		Cast(&host{}, state.WithInitialState("idle"))
	as := state.NewActorSystem(parent).Register(src, behavior)
	return cluster.NewSystem(name, as), as
}

// coordinatorNode forges the coordinator node: a node that spawns and drives actors on the
// workers but hosts none itself. Its host event type is immaterial to remote delivery — it
// marshals the raw signal it is handed and the worker decodes it — so it is typed with a
// plain string event for simplicity. tr routes its operations to the workers over gRPC.
func coordinatorNode(name string, tr *transport.Transport) *cluster.System[string, string, *host] {
	parent := state.ForgeFor[*host]("coordinator").
		State("idle").Initial("idle").Quench().
		Cast(&host{}, state.WithInitialState("idle"))
	return cluster.NewSystem(name, state.NewActorSystem(parent), cluster.WithTransport(tr))
}

// dialBuf wires a gRPC client connection to a bufconn-served server. It is the same
// in-memory dial the cluster and transport test suites use, so the demonstration's wire
// path is identical to the verified one — only the listener is in-memory.
func dialBuf(lis *bufconn.Listener) (grpc.ClientConnInterface, error) {
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dispatch: dial worker over bufconn: %w", err)
	}
	return conn, nil
}

// SpawnedActor is one fulfillment actor the coordinator placed on a worker node: the src
// behavior it was spawned from, the actor id, and the worker node it landed on — the
// opaque ref's coordinates, made observable.
type SpawnedActor struct {
	// Src is the registered behavior the actor was spawned from ("kitchen"/"courier").
	Src string
	// ID is the actor id the coordinator addressed the spawn under.
	ID string
	// Node is the worker node the coordinator placed the actor on.
	Node string
}

// DistributedReport is the observable outcome of [RunDistributedFulfillment]: the nodes
// that made up the cluster, the fulfillment actors the coordinator spawned remotely and
// where, the count of signals delivered across the gRPC wire, and the evidence that a
// crashed worker actor was supervised back to life.
type DistributedReport struct {
	// Coordinator is the node that dispatched fulfillment to the workers.
	Coordinator string
	// Workers lists the worker nodes that hosted fulfillment actors, in placement order.
	Workers []string
	// Spawned lists the fulfillment actors the coordinator placed on workers over gRPC.
	Spawned []SpawnedActor
	// Delivered is the number of actor-driving signals the coordinator delivered across the
	// wire after the supervised restart: the kitchen drive to the restarted actor and the
	// courier drive, each completing its remote actor.
	Delivered int
	// SupervisorDecision is the decision the worker's supervisor applied to the injected
	// crash — [cluster.Restart] when the crashed kitchen actor was respawned within budget.
	SupervisorDecision cluster.Decision
	// Restarts is the number of escalations the worker's supervisor handled (one, for the
	// single injected kitchen crash).
	Restarts int
	// RestartedActor is the id of the actor the supervisor restarted.
	RestartedActor string
}

// RunDistributedFulfillment stands up a three-node cluster wired over real gRPC (carried
// in-memory by bufconn), hosts the proven kitchen and courier fulfillment behaviors as
// remote actors on two worker nodes, and drives them entirely from a coordinator node:
//
//   - the coordinator spawns the kitchen on worker-a and the courier on worker-b, each over
//     the gRPC wire, addressing them only by an opaque ref;
//   - worker-a runs a supervisor that, when its freshly-spawned kitchen actor crashes,
//     restarts it within budget;
//   - the coordinator then delivers the KitchenCook and CourierDrive signals across the
//     wire — driving the restarted kitchen and the courier each to completion — proving
//     dispatch survives a worker-side failure and the actors are location-transparent.
//
// It returns the observable evidence of that flow. The bufconn listeners and gRPC
// connections are torn down before it returns, so the call leaves nothing running.
func RunDistributedFulfillment(ctx context.Context) (DistributedReport, error) {
	return runDistributed(ctx, ovenFireCrash)
}

// ovenFireCrash is the canonical [CrashInjector]: it crashes the worker-hosted kitchen
// actor with an oven-fire failure so worker-a's supervisor catches the escalation and
// respawns it within budget. It reports a clear error when no kitchen actor was running
// to crash, so a mis-sequenced run fails loudly rather than silently skipping the
// supervision demonstration.
func ovenFireCrash(ctx context.Context, crashKitchenActor func(context.Context, error) bool) error {
	if crashed := crashKitchenActor(ctx, errors.New("oven fire")); !crashed {
		return fmt.Errorf("dispatch: no %s actor was running to crash", crashKitchen)
	}
	return nil
}

// CrashInjector fails the worker-hosted kitchen actor so a test (or the production entry
// point) can exercise supervised restart. It settles the kitchen actor with err and
// reports whether an actor was running to receive the failure; the worker's supervisor
// observes the resulting escalation and restarts the actor within budget.
type CrashInjector func(ctx context.Context, inject func(context.Context, error) bool) error

// runDistributed is the testable core of [RunDistributedFulfillment]. crash is the failure
// injected against worker-a's local ActorSystem after the kitchen actor is driven once;
// taking it as a parameter lets a test substitute its own crash (or a no-op) while the
// production entry point injects the canonical oven-fire failure. All gRPC plumbing —
// listeners, servers, client connections — is created and torn down here, so neither the
// caller nor an Example manages any cluster lifecycle.
func runDistributed(ctx context.Context, crash CrashInjector) (DistributedReport, error) {
	// worker-a hosts and supervises the kitchen; worker-b hosts the courier. Each is typed
	// to its actor's signal and served over its own bufconn-backed gRPC server. worker-a's
	// supervisor restarts a crashed kitchen actor up to twice, re-spawning through worker-a's
	// own System.
	workerASys, workerAActors := workerNode("worker-a", "kitchen", fooddelivery.KitchenBehavior(), fooddelivery.KitchenCook)
	sup := cluster.NewSupervisor(cluster.WithRestart("kitchen", 2))
	sup.SetRespawner(workerASys)
	workerAActors.WithEscalationHandler(sup.Handle)

	workerBSys, _ := workerNode("worker-b", "courier", fooddelivery.CourierBehavior(), fooddelivery.CourierDrive)

	closeA, connA, err := serve(workerASys)
	if err != nil {
		return DistributedReport{}, err
	}
	defer closeA()
	closeB, connB, err := serve(workerBSys)
	if err != nil {
		return DistributedReport{}, err
	}
	defer closeB()

	// The coordinator routes to both workers over the gRPC transport. It spawns nothing
	// locally — every actor lives on a worker.
	tr := transport.New()
	tr.AddNode("worker-a", connA)
	tr.AddNode("worker-b", connB)
	coordSys := coordinatorNode("coordinator", tr)

	report := DistributedReport{
		Coordinator: "coordinator",
		Workers:     []string{"worker-a", "worker-b"},
	}

	// Spawn the kitchen on worker-a and the courier on worker-b, each over gRPC.
	kitchenRef, err := spawnRemote(ctx, coordSys, "worker-a", "kitchen", crashKitchen, &report)
	if err != nil {
		return DistributedReport{}, err
	}
	courierRef, err := spawnRemote(ctx, coordSys, "worker-b", "courier", courierActor, &report)
	if err != nil {
		return DistributedReport{}, err
	}

	// Crash the freshly-spawned kitchen actor while it is still prepping — before any
	// signal drives it to completion — so worker-a's supervisor has a live actor to
	// restart. The injector crashes the actor through worker-a's local ActorSystem;
	// binding it here keeps worker-a's concrete (unnameable) signal type out of the
	// injector's signature while still letting a test substitute or suppress the crash.
	inject := func(c context.Context, e error) bool {
		if !workerAActors.IsRunning(crashKitchen) {
			return false
		}
		// The kitchen actor has no parent onError transition, so settling it with an
		// error escalates the failure to worker-a's supervisor (the SettleError bool
		// reports parent routing, not existence — existence is checked above).
		_, _ = workerAActors.SettleError(c, crashKitchen, e)
		return true
	}
	if err = crash(ctx, inject); err != nil {
		return DistributedReport{}, err
	}
	if err = recordSupervision(sup, &report); err != nil {
		return DistributedReport{}, err
	}

	// The restarted kitchen actor is live again under the same opaque ref: drive it over
	// gRPC to prove dispatch continued past the crash. KitchenCook advances the restarted
	// actor to its plated final state.
	if err = deliverRemote(ctx, coordSys, kitchenRef, fooddelivery.KitchenCook, &report); err != nil {
		return DistributedReport{}, err
	}

	// Drive the courier across the wire to its delivered final state.
	if err = deliverRemote(ctx, coordSys, courierRef, fooddelivery.CourierDrive, &report); err != nil {
		return DistributedReport{}, err
	}
	return report, nil
}

// serve stands up a gRPC server exposing sys's wire endpoint on an in-memory bufconn
// listener and dials a client connection back to it. It returns a closer that stops the
// server and closes the connection, and the client connection the coordinator's transport
// routes through. Keeping the listener in-memory lets the whole cluster run inside one
// process without binding a TCP port.
func serve(sys cluster.WireEndpoint) (func(), grpc.ClientConnInterface, error) {
	lis := bufconn.Listen(1 << 20)
	gs := transport.NewServer(sys)
	go func() { _ = gs.Serve(lis) }()

	conn, err := dialBuf(lis)
	if err != nil {
		gs.Stop()
		return nil, nil, err
	}
	closer := func() {
		if c, ok := conn.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		gs.Stop()
	}
	return closer, conn, nil
}

// spawnRemote spawns the src behavior under id on the worker node from coord over gRPC,
// recording the placement in report, and returns the opaque ref the coordinator drives it
// through. It reports a clear error when the remote spawn fails.
func spawnRemote(
	ctx context.Context,
	coord *cluster.System[string, string, *host],
	worker, src, id string,
	report *DistributedReport,
) (state.ActorRef, error) {
	ref, err := coord.Spawn(ctx, worker, src, id, nil)
	if err != nil {
		return state.ActorRef{}, fmt.Errorf("dispatch: spawn %s on %s over grpc: %w", src, worker, err)
	}
	report.Spawned = append(report.Spawned, SpawnedActor{Src: src, ID: id, Node: worker})
	return ref, nil
}

// deliverRemote delivers signal to the actor addressed by ref from coord over gRPC,
// counting the delivery in report. It reports a clear error when the wire delivery fails or
// the actor was not running to receive it.
func deliverRemote(
	ctx context.Context,
	coord *cluster.System[string, string, *host],
	ref state.ActorRef,
	signal any,
	report *DistributedReport,
) error {
	delivered, err := coord.Deliver(ctx, ref, signal)
	if err != nil {
		return fmt.Errorf("dispatch: deliver to %s on %s over grpc: %w", ref.ID, ref.Node, err)
	}
	if !delivered {
		return fmt.Errorf("dispatch: actor %s on %s was not running to receive the signal", ref.ID, ref.Node)
	}
	report.Delivered++
	return nil
}

// recordSupervision reads the worker supervisor's handled escalations into report and
// confirms the injected crash was met with a Restart. It reports a clear error when the
// supervisor handled no escalation or applied a decision other than Restart, so a mis-wired
// supervision fails loudly rather than reporting a hollow success.
func recordSupervision(sup *cluster.Supervisor, report *DistributedReport) error {
	handled := sup.Handled()
	if len(handled) == 0 {
		return errors.New("dispatch: supervisor handled no escalation for the crashed kitchen actor")
	}
	last := handled[len(handled)-1]
	if last.Decision != cluster.Restart {
		return fmt.Errorf("dispatch: supervisor applied %s to the kitchen crash, want Restart", last.Decision)
	}
	report.Restarts = len(handled)
	report.SupervisorDecision = last.Decision
	report.RestartedActor = last.ActorID
	return nil
}
