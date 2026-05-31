package verify_test

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/verify"
)

// ExampleVerify checks that an order machine's terminal state is reachable and
// prints the event sequence that proves it, without ever firing an event.
func ExampleVerify() {
	m := state.Forge[string, string, any]("order").
		State("open").
		Transition("open").On("pay").GoTo("paid").
		State("paid").
		Transition("paid").On("ship").GoTo("shipped").
		State("shipped").Final().
		Initial("open").
		Quench()

	result := verify.Verify(m, verify.Reachable("shipped"))
	f, _ := result.For("shipped")
	fmt.Printf("shipped reachable: %t via %v\n", f.Reachable, f.Witness.Events())

	// Output:
	// shipped reachable: true via [pay ship]
}

// ExampleVerify_unreachable lists the states a machine declares but can never
// enter — the marquee reachability defect, found statically.
func ExampleVerify_unreachable() {
	m := state.Forge[string, string, any]("order").
		State("open").
		Transition("open").On("close").GoTo("closed").
		State("closed").Final().
		State("orphan"). // nothing transitions here
		Initial("open").
		Quench()

	fmt.Println(verify.Verify(m).Unreachable())

	// Output:
	// [orphan]
}

// ExampleReachAvoiding checks a safety property: can the order reach "shipped"
// along a run that never passes through "canceled"? The clean route exists, and
// its witness is the event sequence that proves it without ever canceling.
func ExampleReachAvoiding() {
	m := state.Forge[string, string, any]("order").
		State("open").
		Transition("open").On("pay").GoTo("paid").
		Transition("open").On("abandon").GoTo("canceled").
		State("paid").
		Transition("paid").On("ship").GoTo("shipped").
		State("canceled").Final().
		State("shipped").Final().
		Initial("open").
		Quench()

	result := verify.Verify(m, verify.ReachAvoiding("shipped", "canceled"))
	f, _ := result.ConditionalReach("shipped")
	fmt.Printf("shipped without canceling: %t via %v\n", f.Reachable, f.Witness.Events())

	// Output:
	// shipped without canceling: true via [pay ship]
}

// ExampleReachable restricts the pass to named target states.
func ExampleReachable() {
	m := state.Forge[string, string, any]("toggle").
		State("off").
		Transition("off").On("flip").GoTo("on").
		State("on").
		Transition("on").On("flip").GoTo("off").
		Initial("off").
		Quench()

	result := verify.Verify(m, verify.Reachable("on"))
	fmt.Printf("%d finding(s): on reachable=%t\n", len(result.Findings), result.CanReach("on"))

	// Output:
	// 1 finding(s): on reachable=true
}
