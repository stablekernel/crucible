package dispatch_test

import (
	"context"
	"fmt"

	"github.com/stablekernel/crucible/examples/dispatch"
)

// ExampleRunDistributedFulfillment hosts the proven kitchen and courier fulfillment
// behaviors as remote cluster actors on two worker nodes and drives them entirely from
// a coordinator node over real gRPC (carried in-memory by bufconn): the coordinator
// spawns the kitchen on worker-a and the courier on worker-b, worker-a's supervisor
// restarts the kitchen actor after it crashes, and the coordinator then delivers the
// actor-driving signals across the wire to drive each remote actor to completion —
// proving the fulfillment actors are location-transparent and survive a worker-side
// failure.
func ExampleRunDistributedFulfillment() {
	report, err := dispatch.RunDistributedFulfillment(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Printf("coordinator: %s\n", report.Coordinator)
	fmt.Printf("workers:     %v\n", report.Workers)
	fmt.Println("spawned remotely:")
	for _, a := range report.Spawned {
		fmt.Printf("  %s actor %q on %s\n", a.Src, a.ID, a.Node)
	}
	fmt.Printf("signals delivered over grpc: %d\n", report.Delivered)
	fmt.Printf("supervision: %s of %q (restarts: %d)\n",
		report.SupervisorDecision, report.RestartedActor, report.Restarts)

	// Output:
	// coordinator: coordinator
	// workers:     [worker-a worker-b]
	// spawned remotely:
	//   kitchen actor "kitchen-1" on worker-a
	//   courier actor "courier-1" on worker-b
	// signals delivered over grpc: 2
	// supervision: restart of "kitchen-1" (restarts: 1)
}
