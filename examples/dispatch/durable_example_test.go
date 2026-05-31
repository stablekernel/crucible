package dispatch_test

import (
	"context"
	"fmt"
	"os"

	"github.com/stablekernel/crucible/examples/dispatch"
)

// ExampleRunCrashRecovery runs the proven order saga under the durable runtime
// backed by an on-disk FileStore: it drives an order to its live Active fulfillment
// configuration, simulates a process crash, reconstructs the order from the store
// alone — its state, payment hold, and folded log intact — and drives the recovered
// order on to Delivered. It then time-travels over a history-retaining MemStore to
// reconstruct the order's state at an earlier point in its lifecycle, read-only.
func ExampleRunCrashRecovery() {
	ctx := context.Background()

	// (i) Crash and recovery against a real on-disk store. A temp dir stands in for
	// the durable storage a host would point at; the order survives the crash because
	// every step was recorded write-ahead before the runner was dropped.
	dir, err := os.MkdirTemp("", "dispatch-durable-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	recovery, err := dispatch.RunCrashRecovery(ctx, dir)
	if err != nil {
		panic(err)
	}

	fmt.Println("crash + recovery")
	fmt.Printf("  recovered state:     %v\n", recovery.RecoveredConfig)
	fmt.Printf("  recovered auth hold: %s\n", recovery.RecoveredAuthHold)
	fmt.Printf("  recovered log:       %v\n", recovery.RecoveredLog)
	fmt.Printf("  drove on to:         %v\n", recovery.FinalConfig)
	fmt.Printf("  final log:           %v\n", recovery.FinalLog)

	// (ii) Read-only time travel against a history-retaining store. Every step is
	// reachable, so the order's state can be reconstructed at any earlier point in its
	// lifecycle without re-running any service or actor.
	travel, err := dispatch.RunTimeTravel(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println("time travel")
	for _, step := range travel.Timeline {
		fmt.Printf("  step %d: state=%v log=%d\n", step.Step, step.Config, step.LogLen)
	}
	fmt.Printf("  state at step %d: %v (final: %v)\n",
		travel.EarlierStep, travel.EarlierConfig, travel.FinalConfig)

	// Output:
	// crash + recovery
	//   recovered state:     [Cooking OnTime]
	//   recovered auth hold: hold-5500
	//   recovered log:       [authorized:hold-5500]
	//   drove on to:         [Delivered]
	//   final log:           [authorized:hold-5500 kitchen:prepared-meal courier:drop-confirmed captured]
	// time travel
	//   step 0: state=[Authorizing] log=0
	//   step 1: state=[Cooking OnTime] log=1
	//   step 2: state=[AwaitingCourier OnTime] log=2
	//   step 3: state=[EnRoute OnTime] log=2
	//   step 4: state=[Delivered] log=4
	//   state at step 0: [Authorizing] (final: [Delivered])
}
