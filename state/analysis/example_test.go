package analysis_test

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
)

// ExampleAnalyze statically checks an order machine that carries two deliberate
// defects: a "lost" state nothing can reach, and a non-final "stuck" state with
// no way out. The report names both without ever firing an event.
func ExampleAnalyze() {
	m := state.Forge[string, string, any]("order").
		State("open").
		Transition("open").On("pay").GoTo("paid").
		Transition("open").On("hold").GoTo("stuck").
		State("paid").
		Transition("paid").On("ship").GoTo("closed").
		State("stuck"). // non-final, no way out -> dead end
		State("closed").Final().
		State("lost"). // declared but nothing transitions to it -> unreachable
		Transition("lost").On("found").GoTo("open").
		Initial("open").
		Quench()

	report := analysis.Analyze(m)
	for _, f := range report.Findings {
		fmt.Printf("%s [%s] %s\n", f.Severity, f.Kind, f.State)
	}

	// Output:
	// error [unreachable_state] lost
	// error [dead_transition] lost
	// warning [dead_end] stuck
	// warning [cannot_reach_final] stuck
}
