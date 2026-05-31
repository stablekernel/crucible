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

// ExampleAlwaysEventually checks a liveness property: from every reachable
// configuration, can the order still eventually reach "delivered"? Here a parcel
// can be marked "lost", a terminal that delivered can never follow — so the
// property fails and the counterexample names the stuck configuration.
func ExampleAlwaysEventually() {
	m := state.Forge[string, string, any]("parcel").
		State("shipped").
		Transition("shipped").On("arrive").GoTo("delivered").
		Transition("shipped").On("misroute").GoTo("lost").
		State("lost").Final(). // a terminal from which delivered is unreachable
		State("delivered").Final().
		Initial("shipped").
		Quench()

	result := verify.Verify(m, verify.AlwaysEventually("delivered"))
	f, _ := result.Liveness("delivered")
	fmt.Printf("always delivered: %t; stuck at %q via %v\n",
		f.Reachable, f.Witness.Target, f.Witness.Events())

	// Output:
	// always delivered: false; stuck at "lost" via [misroute]
}

// ExampleCheckInvariant checks a configuration invariant: that a parcel is never
// both "held" and "delivered" at once. The two regions of a parallel state can
// advance independently, so the configuration where both are active is reachable,
// the invariant is violated, and the counterexample names that configuration.
func ExampleCheckInvariant() {
	m := state.Forge[string, string, any]("parcel").
		State("created").
		Transition("created").On("dispatch").GoTo("transit").
		SuperState("transit").
		Region("Custody").Initial("held").
		SubState("held").On("release").GoTo("delivered").
		SubState("delivered").Final().
		EndRegion().
		Region("Billing").Initial("unpaid").
		SubState("unpaid").On("pay").GoTo("paid").
		SubState("paid").Final().
		EndRegion().
		EndSuperState().
		Initial("created").
		Quench()

	inv := verify.MutualExclusion("held", "paid")
	result := verify.Verify(m, verify.CheckInvariant(inv))
	f, _ := result.Invariant(inv.Label())
	fmt.Printf("%s holds: %t; violated at %q via %v\n",
		inv.Label(), f.Reachable, f.Witness.Target, f.Witness.Events())

	// Output:
	// mutex(held,paid) holds: false; violated at "held|paid" via [dispatch pay]
}

// ExampleSimulateBounded explores every trace up to a depth bound and reports the
// shortest one whose reached configuration a caller-supplied oracle rejects. Here
// the oracle flags any configuration where the order is "shipped", so the bounded
// search returns the shortest trace that drives the machine there.
func ExampleSimulateBounded() {
	m := state.Forge[string, string, any]("order").
		State("open").
		Transition("open").On("pay").GoTo("paid").
		State("paid").
		Transition("paid").On("ship").GoTo("shipped").
		State("shipped").Final().
		Initial("open").
		Quench()

	// The oracle holds (true) until the order is shipped.
	oracle := func(active map[string]bool) bool { return !active["shipped"] }
	result := verify.Verify(m, verify.SimulateBounded("never-shipped", 5, oracle))
	f, _ := result.BoundedSim("never-shipped")
	fmt.Printf("held within bound: %t; violation at %q via %v\n",
		f.Reachable, f.Witness.Target, f.Witness.Events())

	// Output:
	// held within bound: false; violation at "shipped" via [pay ship]
}

// ExampleCoverage measures which states and transitions a scenario set exercises
// against the reachable universe, so a CI gate can flag the structural gap a test
// suite leaves. Here a single happy-path scenario covers the shipping line but
// leaves the cancellation branch unexercised.
func ExampleCoverage() {
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

	result := verify.Verify(m, verify.Coverage([]string{"pay", "ship"}))
	rep, _ := result.Coverage()
	fmt.Printf("state coverage: %.0f%%\n", rep.StateCoverage()*100)
	fmt.Printf("uncovered states: %v\n", rep.UncoveredStates)
	fmt.Printf("uncovered transitions: %v\n", rep.UncoveredTransitions)

	// Output:
	// state coverage: 75%
	// uncovered states: [canceled]
	// uncovered transitions: [open -abandon-> canceled]
}

// ExampleCoveringSuite generates a covering suite — a set of event sequences that
// together exercise every reachable state and transition — and confirms it by
// feeding it straight back into [Coverage], which reports full coverage. The suite
// is the seed for a conformance test set built from the machine's structure alone.
func ExampleCoveringSuite() {
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

	suite := verify.CoveringSuite(m)
	rep, _ := verify.Verify(m, verify.Coverage(suite...)).Coverage()
	fmt.Printf("suite: %v\n", suite)
	fmt.Printf("covers states: %.0f%%, transitions: %.0f%%\n",
		rep.StateCoverage()*100, rep.TransitionCoverage()*100)

	// Output:
	// suite: [[pay ship] [abandon]]
	// covers states: 100%, transitions: 100%
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
