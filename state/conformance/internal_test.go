package conformance

import (
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestOutcomeName_AllKernelOutcomes asserts outcomeName renders every kernel
// Outcome by a distinct stable name, so a conformance trace never collapses two
// different failure classes onto the same label (which would make a regression
// compare equal). An out-of-range value falls back to "Unknown".
func TestOutcomeName_AllKernelOutcomes(t *testing.T) {
	cases := []struct {
		outcome state.Outcome
		want    string
	}{
		{state.OutcomeSuccess, "Success"},
		{state.OutcomeInvalidTransition, "InvalidTransition"},
		{state.OutcomeGuardFailed, "GuardFailed"},
		{state.OutcomeGuardPanic, "GuardPanic"},
		{state.OutcomePolicyDenied, "PolicyDenied"},
		{state.OutcomeEffectError, "EffectError"},
		{state.OutcomeAssignFailed, "AssignFailed"},
		{state.Outcome(-1), "Unknown"},
		{state.Outcome(9999), "Unknown"},
	}

	seen := map[string]state.Outcome{}
	for _, tc := range cases {
		got := outcomeName(tc.outcome)
		if got != tc.want {
			t.Fatalf("outcomeName(%d) = %q, want %q", tc.outcome, got, tc.want)
		}
		// Every named (non-Unknown) outcome must be distinct from the others, so a
		// trace diff distinguishes the failure classes.
		if tc.want != "Unknown" {
			if prev, dup := seen[got]; dup {
				t.Fatalf("outcomeName collision: %d and %d both render %q", prev, tc.outcome, got)
			}
			seen[got] = tc.outcome
		}
	}
}
