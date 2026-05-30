package state_test

import (
	"context"
	"fmt"
	"time"

	"github.com/stablekernel/crucible/state"
)

// ExampleScheduler drives a delayed (`after`) transition deterministically with
// a FakeClock and the reusable Scheduler host-driver. The kernel stays pure:
// entering "pending" emits a ScheduleAfter effect, the Scheduler arms a timer,
// and advancing the fake clock past the delay fires the delayed event back
// through Fire — driving a delayed (after) transition with no real waiting.
func ExampleScheduler() {
	type cart struct{}
	m := state.Forge[string, string, cart]("checkout").
		State("active").
		State("pending").
		State("expired").
		Initial("active").
		Transition("active").On("submit").GoTo("pending").
		// After 15 minutes in "pending" with no action, the cart expires.
		Transition("pending").After(15 * time.Minute).On("timeout").GoTo("expired").
		State("expired").Final().
		Quench()

	clk := state.NewFakeClock(time.Unix(0, 0))
	inst := m.Cast(cart{}, state.WithInitialState("active"), state.WithClock[string](clk))
	sch := state.NewScheduler(inst)
	ctx := context.Background()

	// Entering "pending" emits the ScheduleAfter effect; the host absorbs every
	// Fire's effects into the Scheduler, which arms the timer.
	res := inst.Fire(ctx, "submit")
	sch.Absorb(ctx, res.Effects)
	fmt.Println("before:", inst.Current(), "pending timers:", sch.Pending())

	// Nothing happens until the delay elapses; advancing the fake clock and
	// ticking the Scheduler fires the delayed "timeout" event.
	clk.Advance(15 * time.Minute)
	sch.Tick(ctx)
	fmt.Println("after: ", inst.Current(), "pending timers:", sch.Pending())
	// Output:
	// before: pending pending timers: 1
	// after:  expired pending timers: 0
}
