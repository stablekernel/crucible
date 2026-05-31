package dispatch_test

import (
	"fmt"
	"sort"

	"github.com/stablekernel/crucible/examples/dispatch"
	"github.com/stablekernel/crucible/examples/fooddelivery"
)

// ExampleProve proves the food-delivery order saga before any order is dispatched
// and prints a human-readable summary of the evidence: which key stages are
// reachable, whether the Watchdog leaves are mutually exclusive, whether the
// analyzer found any nondeterministic overlap, and whether every guard can fire.
func ExampleProve() {
	model, err := fooddelivery.NewModel()
	if err != nil {
		fmt.Println("build model:", err)
		return
	}

	report, err := dispatch.Prove(model)
	if err != nil {
		fmt.Println("prove model:", err)
		return
	}

	stages := make([]string, 0, len(report.Reachable))
	for stage := range report.Reachable {
		stages = append(stages, stage)
	}
	sort.Strings(stages)

	fmt.Println("order saga proof")
	for _, stage := range stages {
		fmt.Printf("  reachable %-10s %t\n", stage, report.Reachable[stage])
	}
	fmt.Printf("  watchdog mutually exclusive: %t\n", report.WatchdogExclusive)
	fmt.Printf("  nondeterministic overlaps:   %d\n", len(report.Overlaps))
	fmt.Printf("  guards (all satisfiable):    %d\n", len(report.Guards))
	fmt.Printf("  sound: %t\n", report.Sound())

	// Output:
	// order saga proof
	//   reachable Active     true
	//   reachable Canceled   true
	//   reachable Delivered  true
	//   reachable Overdue    true
	//   reachable Rejected   true
	//   watchdog mutually exclusive: true
	//   nondeterministic overlaps:   0
	//   guards (all satisfiable):    1
	//   sound: true
}
