package state_test

import (
	"context"
	"fmt"
)

// Example_connectionLifecycle drives the connection lifecycle exemplar end-to-end
// through the real host runtime — an ActorSystem, a Scheduler on a FakeClock, and
// a ServiceRunner wired around one instance. It shows a transient dial failure
// that backs off and retries on a timer, a guarded admission into a parallel
// Connected configuration, a worker actor that runs a task to completion, and an
// eventless run-to-completion shutdown. The connHarness (in exemplar_test.go)
// wires the three drivers and routes every Fire's effects through them.
func Example_connectionLifecycle() {
	ctx := context.Background()
	h := newConnHarness()
	fmt.Println("start:", fmtConfig(h.inst.Configuration()))

	// Connect arms the dial service; the first attempt fails, falling back to
	// Backoff, where a connect-timeout timer is armed.
	h.fire(ctx, Connect)
	h.settleDial(ctx, false)
	fmt.Println("dial failed:", fmtConfig(h.inst.Configuration()))

	// Advancing the fake clock past the timeout fires the delayed Retry edge, which
	// re-enters Connecting; the second dial succeeds and the guarded Dialed edge
	// admits the instance into the parallel Connected configuration.
	h.advancePastTimeout(ctx)
	h.settleDial(ctx, true)
	fmt.Println("connected:", fmtConfig(h.inst.Configuration()))

	// Assigning work spawns a worker actor; stepping it to completion routes the
	// result back through the parent, draining the Work region.
	h.fire(ctx, Assign)
	h.runWorkers(ctx)
	fmt.Println("work done:", fmtConfig(h.inst.Configuration()))

	// Close runs to completion through the eventless edge into the final state.
	h.fire(ctx, Close)
	fmt.Println("closed:", fmtConfig(h.inst.Configuration()), "final:", h.inst.InFinal())

	// Output:
	// start: Disconnected
	// dial failed: Backoff
	// connected: Beating,WorkIdle
	// work done: Beating,Drained
	// closed: Closed final: true
}
