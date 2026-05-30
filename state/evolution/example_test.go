package evolution_test

import (
	"fmt"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/evolution"
)

// ExampleDiff classifies an additive change (a new state plus the transition
// that reaches it) and a breaking change (a retargeted transition), and reports
// the recommended version bump for each.
func ExampleDiff() {
	old := &state.IR[string, string, any]{
		Name: "order", Initial: "open", HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "open", Transitions: []state.Transition[string, string, any]{
				{From: "open", On: "pay", To: "paid"},
			}},
			{Name: "paid"},
		},
	}

	// Additive: add a "shipped" state reachable from "paid".
	additive := &state.IR[string, string, any]{
		Name: "order", Initial: "open", HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "open", Transitions: []state.Transition[string, string, any]{
				{From: "open", On: "pay", To: "paid"},
			}},
			{Name: "paid", Transitions: []state.Transition[string, string, any]{
				{From: "paid", On: "ship", To: "shipped"},
			}},
			{Name: "shipped"},
		},
	}

	// Breaking: "pay" now lands in "shipped" instead of "paid".
	breaking := &state.IR[string, string, any]{
		Name: "order", Initial: "open", HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "open", Transitions: []state.Transition[string, string, any]{
				{From: "open", On: "pay", To: "shipped"},
			}},
			{Name: "paid"},
			{Name: "shipped"},
		},
	}

	a := evolution.Diff(old, additive)
	fmt.Printf("additive: breaking=%v bump=%s\n", a.Breaking(), a.SemverBump())

	b := evolution.Diff(old, breaking)
	fmt.Printf("breaking: breaking=%v bump=%s\n", b.Breaking(), b.SemverBump())

	// Output:
	// additive: breaking=false bump=minor
	// breaking: breaking=true bump=major
}
